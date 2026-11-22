package engine

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/GMWalletApp/ChainScanner/internal/chains"
	"github.com/GMWalletApp/ChainScanner/internal/feed"
)

const otherWallet = "0x2222222222222222222222222222222222222222"

// filterProbe is a linked EVM source. Each block read records the address
// filter it received and may wait for the test before answering.
type filterProbe struct {
	head    int64
	mu      sync.Mutex
	seen    map[int64]chains.Watched
	entered chan int64
	release map[int64]chan struct{}
}

func newFilterProbe(head int64) *filterProbe {
	return &filterProbe{head: head, seen: map[int64]chains.Watched{}, entered: make(chan int64, 16), release: map[int64]chan struct{}{}}
}
func (p *filterProbe) Head(context.Context) (int64, error) { return p.head, nil }
func (*filterProbe) Close()                                {}
func (*filterProbe) Read(context.Context, int64, int64) (int64, []chains.Activity, error) {
	panic("EVM spans are read block by block")
}
func (p *filterProbe) ReadLinkedBlock(ctx context.Context, h int64, link *chains.BlockLink) (int64, []chains.Activity, error) {
	match := chains.WatchedAddresses(ctx)
	p.mu.Lock()
	p.seen[h] = match
	wait := p.release[h]
	p.mu.Unlock()
	p.entered <- h
	if wait != nil {
		select {
		case <-wait:
		case <-ctx.Done():
			return 0, nil, ctx.Err()
		}
	}
	*link = chains.BlockLink{Hash: fmt.Sprint("h", h), Parent: fmt.Sprint("h", h-1)}
	var out []chains.Activity
	for _, a := range []string{wallet, otherWallet} {
		// A filtering reader drops what the filter rejects.
		if match == nil || match(a) {
			out = append(out, chains.Activity{Height: h, TxID: fmt.Sprint(a, "@", h), TimeMS: 1, Addresses: []string{a}, Data: []byte("{}")})
		}
	}
	return h, out, nil
}
func (p *filterProbe) filter(h int64) chains.Watched {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.seen[h]
}

func TestScanReadsCarryTheCachedAddressIndex(t *testing.T) {
	ctx := context.Background()
	s, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.SeedCursor(ctx, "base", 99); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Subscribe(ctx, "a", []feed.Watch{{Network: "base", Address: wallet}}); err != nil {
		t.Fatal(err)
	}
	period, _ := s.period("base")
	if chains.WatchedAddresses(period) != nil {
		t.Fatal("filtered before the address index was loaded")
	}
	src := newFilterProbe(100)
	if _, err = s.Step(ctx, chains.Network{Name: "base"}, src); err != nil {
		t.Fatal(err)
	}
	match := src.filter(100)
	if match == nil || !match(wallet) || match(otherWallet) {
		t.Fatal("read did not receive the watch index")
	}
	// The live queue derives its reads from the period context itself.
	if match = chains.WatchedAddresses(mustPeriod(t, s)); match == nil || !match(wallet) {
		t.Fatal("watch period does not carry the filter")
	}
	d, err := s.PendingNetwork(ctx, "a", "base", 0)
	if err != nil || d == nil || d.Activity.TxID != wallet+"@100" {
		t.Fatal(d, err)
	}
}

// The filter is a snapshot of the cached index taken when a block read
// starts. Subscription changes made during the read must not hide activity
// from a watcher that can match that read's heights.
func TestAddressFilterCoversEveryWatcherOfAnInFlightSpan(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.resolve = func(context.Context, string) (int64, error) { return 101, nil }
	if err = s.SeedCursor(ctx, "base", 99); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Subscribe(ctx, "a", []feed.Watch{{Network: "base", Address: wallet}}); err != nil {
		t.Fatal(err)
	}
	src := newFilterProbe(101)
	src.release[100] = make(chan struct{})
	done := make(chan error, 1)
	go func() { _, err := s.Step(ctx, chains.Network{Name: "base"}, src); done <- err }()
	if h := <-src.entered; h != 100 {
		t.Fatal("first read", h)
	}
	inFlight := src.filter(100)
	if inFlight == nil || !inFlight(wallet) {
		t.Fatal("in-flight read was not filtered by the watch index")
	}
	// Spans 100 and 101 are reserved. A new watch starts after both, so the
	// in-flight snapshot may omit it.
	receipt, err := s.Subscribe(ctx, "b", []feed.Watch{{Network: "base", Address: otherWallet}})
	if err != nil || len(receipt.Registrations) != 1 || receipt.Registrations[0].FromHeight <= 101 {
		t.Fatal("new watch overlaps reserved spans", receipt, err)
	}
	if inFlight(otherWallet) {
		t.Fatal("snapshot changed during the read")
	}
	if now := chains.WatchedAddresses(mustPeriod(t, s)); now != nil && !now(otherWallet) {
		t.Fatal("a later snapshot omits the new watch")
	}
	// A removed watch keeps matching every height that was already reserved.
	receipt, err = s.Subscribe(ctx, "a", nil)
	if err != nil || len(receipt.Terminations) != 1 || receipt.Terminations[0].ToHeight < 101 {
		t.Fatal("termination before reserved spans", receipt, err)
	}
	close(src.release[100])
	if h := <-src.entered; h != 101 {
		t.Fatal("second read", h)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if later := src.filter(101); later == nil || !later(wallet) {
		t.Fatal("closing watch was filtered out of a span it still covers")
	}
	for _, h := range []int64{100, 101} {
		d, err := s.PendingNetwork(ctx, "a", "base", 0)
		if err != nil || d == nil || d.Activity.Height != h || d.Activity.TxID != fmt.Sprint(wallet, "@", h) {
			t.Fatal("closing watch delivery", h, d, err)
		}
		if err = s.Ack(ctx, "a", d.ID); err != nil {
			t.Fatal(err)
		}
	}
	if d, err := s.PendingNetwork(ctx, "b", "base", 0); err != nil || d != nil {
		t.Fatal("delivered before the watch started", d, err)
	}
}

func mustPeriod(t *testing.T, s *Store) context.Context {
	t.Helper()
	p, _ := s.period("base")
	if p == nil {
		t.Fatal("no watch period")
	}
	return p
}

// RunWithSource reads under WhileWatched's context, which derives from the
// watch period that carries the filter.
func TestRunReadsUnderTheWatchPeriodFilter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.SeedCursor(ctx, "base", 99); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Subscribe(ctx, "a", []feed.Watch{{Network: "base", Address: wallet}}); err != nil {
		t.Fatal(err)
	}
	src := newFilterProbe(100)
	open := func(context.Context, chains.Network) (chains.Source, error) { return src, nil }
	go s.WhileWatched(ctx, "base", func(live context.Context) {
		RunWithSource(live, s, chains.Network{Name: "base", IntervalSeconds: 1}, open, func(chains.Source) <-chan struct{} { return nil })
	})
	select {
	case <-src.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if match := src.filter(100); match == nil || !match(wallet) || match(otherWallet) {
		t.Fatal("RunWithSource read without the period filter")
	}
}

func TestEVMCatchUpReservesLinkedSpans(t *testing.T) {
	ctx := context.Background()
	s, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	widths := func(network string, high, head int64, limit int) []int64 {
		t.Helper()
		if err := s.SeedCursor(ctx, network, high); err != nil {
			t.Fatal(err)
		}
		work, _, err := s.planAvailable(ctx, network, high, head, limit, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		var out []int64
		for _, r := range work {
			out = append(out, r.Last-r.First+1)
		}
		return out
	}
	if got := widths("base", 100, 103, 4); fmt.Sprint(got) != "[1 1 1]" {
		t.Fatal("near the tip", got)
	}
	if got := widths("ethereum", 100, 200, 4); fmt.Sprint(got) != "[4 4 4 4]" {
		t.Fatal("behind", got)
	}
	if got := widths("polygon", 100, 110, 16); fmt.Sprint(got) != "[1 1 1 1 1 1 1 1 1 1]" {
		t.Fatal("less than one window behind", got)
	}
	if got := widths("solana", 100, 200, 4); fmt.Sprint(got) != "[2 2 2 2]" {
		t.Fatal("solana", got)
	}
	// A failed linked span is bisected.
	if err = s.deferGap(ctx, "ethereum", chains.Span{First: 101, Last: 104}); err != nil {
		t.Fatal(err)
	}
	work, _, err := s.planAvailable(ctx, "ethereum", 116, 116, 8, nil, 0)
	if err != nil || fmt.Sprint(work) != "[{105 108 false} {109 112 false} {113 116 false} {101 102 true} {103 104 true}]" {
		t.Fatal(work, err)
	}
}
