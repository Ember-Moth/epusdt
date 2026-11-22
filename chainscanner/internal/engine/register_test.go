package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/GMWalletApp/ChainScanner/internal/chains"
	"github.com/GMWalletApp/ChainScanner/internal/feed"
)

func drainHeights(t *testing.T, s *Store, consumer, network string) []int64 {
	t.Helper()
	var got []int64
	for {
		d, err := s.PendingNetwork(context.Background(), consumer, network, 0)
		if err != nil {
			t.Fatal(err)
		}
		if d == nil {
			return got
		}
		got = append(got, d.Activity.Height)
		if err = s.Ack(context.Background(), consumer, d.ID); err != nil {
			t.Fatal(err)
		}
	}
}

func countWatches(t *testing.T, s *Store, consumer string) int {
	t.Helper()
	var n int
	if err := s.db.QueryRow("SELECT count(*) FROM watches WHERE consumer=?", consumer).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestRegisterAddsWithoutReplacing(t *testing.T) {
	ctx := context.Background()
	s, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	const second = "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
	a := feed.Watch{Network: "base", Address: wallet}
	b := feed.Watch{Network: "base", Address: second}
	c := feed.Watch{Network: "base", Address: "0x3333333333333333333333333333333333333333"}

	// A cold network needs a live head, exactly as a replacement does.
	if rows, err := s.Register(ctx, "pay", []feed.Watch{a}); !errors.Is(err, feed.ErrCursorNotReady) {
		t.Fatal(rows, err)
	}
	if s.Watching("base") || countWatches(t, s, "pay") != 0 {
		t.Fatal("failed registration changed state")
	}
	src := &chain{height: 100}
	ConfigureNetworks(s, map[string]chains.Network{"base": {Name: "base"}}, func(ctx context.Context, _ string) (int64, error) { return src.Head(ctx) })
	rows, err := s.Register(ctx, "pay", []feed.Watch{a})
	if err != nil || len(rows) != 1 || rows[0].FromHeight != 101 {
		t.Fatal(rows, err)
	}
	if !s.Watching("base") {
		t.Fatal("registration did not start the worker")
	}
	// Committing a block caches the address index for this network.
	if err = completeNext(s, ctx, "base", 100, 101, []chains.Activity{{Height: 101, TxID: "101", Addresses: []string{wallet}}}); err != nil {
		t.Fatal(err)
	}
	heads := src.heads
	rows, err = s.Register(ctx, "pay", []feed.Watch{a, {Network: "base", Address: strings.ToUpper(second[2:])}, b})
	want := []feed.Registration{{Watch: a, FromHeight: 101}, {Watch: b, FromHeight: 102}}
	if err != nil || !slices.Equal(rows, want) {
		t.Fatal(rows, err)
	}
	if src.heads != heads {
		t.Fatal("a running network must not read a head")
	}
	// The new address must reach the cached index.
	if err = completeNext(s, ctx, "base", 101, 102, []chains.Activity{{Height: 102, TxID: "102", Addresses: []string{wallet, second}}}); err != nil {
		t.Fatal(err)
	}
	d, err := s.PendingNetwork(ctx, "pay", "base", 0)
	if err != nil || d == nil || d.Activity.Height != 101 {
		t.Fatal(d, err)
	}
	if err = s.Ack(ctx, "pay", d.ID); err != nil {
		t.Fatal(err)
	}
	d, err = s.PendingNetwork(ctx, "pay", "base", 0)
	if err != nil || d == nil || d.Activity.Height != 102 || !slices.Equal(d.Activity.Addresses, []string{wallet, second}) {
		t.Fatal(d, err)
	}
	if err = s.Ack(ctx, "pay", d.ID); err != nil {
		t.Fatal(err)
	}

	if rows, err = s.Register(ctx, "pay", []feed.Watch{c}); err != nil || len(rows) != 1 || rows[0].FromHeight != 103 {
		t.Fatal(rows, err)
	}
	// Existing watches keep their start and other watches stay in place.
	rows, err = s.Register(ctx, "pay", []feed.Watch{c, a})
	if err != nil || !slices.Equal(rows, []feed.Registration{{Watch: c, FromHeight: 103}, {Watch: a, FromHeight: 101}}) {
		t.Fatal(rows, err)
	}
	if n := countWatches(t, s, "pay"); n != 3 {
		t.Fatal("watches", n)
	}
	receipt, err := s.Subscribe(ctx, "pay", []feed.Watch{a, b, c})
	if err != nil || len(receipt.Terminations) != 0 || len(receipt.Registrations) != 3 || receipt.Registrations[1].FromHeight != 102 {
		t.Fatal(receipt, err)
	}
	// Consumers are independent.
	if rows, err = s.Register(ctx, "other", []feed.Watch{a}); err != nil || rows[0].FromHeight != 103 {
		t.Fatal(rows, err)
	}
	if countWatches(t, s, "pay") != 3 || countWatches(t, s, "other") != 1 {
		t.Fatal("registration crossed consumers")
	}
	if rows, err = s.Register(ctx, "pay", []feed.Watch{}); err != nil || len(rows) != 0 {
		t.Fatal(rows, err)
	}
	if _, err = s.Register(ctx, "pay", []feed.Watch{{Network: "base", Address: "not-an-address"}}); err == nil {
		t.Fatal("invalid address accepted")
	}
	if _, err = s.Register(ctx, "pay", []feed.Watch{{Network: "optimism", Address: wallet}}); !errors.Is(err, ErrNetworkNotConfigured) {
		t.Fatal(err)
	}
	if _, err = s.Register(ctx, " ", []feed.Watch{a}); err == nil {
		t.Fatal("empty consumer accepted")
	}
}

func TestRegisterDuringDrainStartsAfterClosedInterval(t *testing.T) {
	ctx := context.Background()
	s, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	cfg := chains.Network{Name: "base"}
	src := &chain{height: 100}
	ConfigureNetworks(s, map[string]chains.Network{"base": cfg}, func(ctx context.Context, _ string) (int64, error) { return src.Head(ctx) })
	watches := []feed.Watch{{Network: "base", Address: wallet}}
	if _, err = s.Subscribe(ctx, "pay", watches); err != nil {
		t.Fatal(err)
	}
	src.height = 102
	r, err := s.Subscribe(ctx, "pay", nil)
	if err != nil || len(r.Terminations) != 1 || r.Terminations[0].ToHeight != 102 || r.Terminations[0].Completed {
		t.Fatal(r, err)
	}
	if !s.Draining("base") {
		t.Fatal("expected drain")
	}
	rows, err := s.Register(ctx, "pay", watches)
	if err != nil || len(rows) != 1 || rows[0].FromHeight != 103 {
		t.Fatal(rows, err)
	}
	if s.Draining("base") || !s.Watching("base") {
		t.Fatal("registration did not reopen the network")
	}
	if rows, err = s.Register(ctx, "pay", watches); err != nil || rows[0].FromHeight != 103 {
		t.Fatal(rows, err)
	}
	// Registration leaves the replayed termination receipt as it was.
	r, err = s.Subscribe(ctx, "pay", watches)
	if err != nil || len(r.Registrations) != 1 || r.Registrations[0].FromHeight != 103 || len(r.Terminations) != 1 || r.Terminations[0].ToHeight != 102 {
		t.Fatal(r, err)
	}
	src.height = 104
	if _, err = s.Step(ctx, cfg, src); err != nil {
		t.Fatal(err)
	}
	if got := drainHeights(t, s, "pay", "base"); !slices.Equal(got, []int64{101, 102, 103, 104}) {
		t.Fatal(got)
	}
	if r, err = s.Subscribe(ctx, "pay", watches); err != nil || len(r.Terminations) != 1 || !r.Terminations[0].Completed {
		t.Fatal(r, err)
	}
}

func TestRegisterInterleavesWithReplacement(t *testing.T) {
	ctx := context.Background()
	s, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	src := &chain{height: 100}
	var headMu sync.Mutex
	ConfigureNetworks(s, map[string]chains.Network{"base": {Name: "base"}}, func(ctx context.Context, _ string) (int64, error) {
		headMu.Lock()
		defer headMu.Unlock()
		return src.Head(ctx)
	})
	base := []feed.Watch{{Network: "base", Address: wallet}, {Network: "base", Address: "0x2222222222222222222222222222222222222222"}}
	if _, err = s.Subscribe(ctx, "pay", base); err != nil {
		t.Fatal(err)
	}
	const extra = 32
	added := make([]feed.Watch, extra)
	for i := range added {
		added[i] = feed.Watch{Network: "base", Address: fmt.Sprintf("0x%040x", 0x1000+i)}
	}
	var wg sync.WaitGroup
	wg.Go(func() {
		for range 20 {
			if _, err := s.Subscribe(ctx, "pay", base); err != nil {
				t.Error(err)
			}
		}
	})
	wg.Go(func() {
		for h := int64(101); h <= 130; h++ {
			if err := completeNext(s, ctx, "base", h-1, h, []chains.Activity{{Height: h, TxID: fmt.Sprint(h), Addresses: []string{wallet}}}); err != nil {
				t.Error(err)
				return
			}
		}
	})
	for _, w := range added {
		wg.Go(func() {
			rows, err := s.Register(ctx, "pay", []feed.Watch{w})
			if err != nil || len(rows) != 1 || rows[0].Watch != w || rows[0].FromHeight < 101 {
				t.Error(w, rows, err)
			}
		})
	}
	wg.Wait()
	if t.Failed() {
		return
	}
	// Registration is additive, a replacement is authoritative.
	rows, err := s.Register(ctx, "pay", append(slices.Clone(base), added...))
	if err != nil || len(rows) != extra+2 || countWatches(t, s, "pay") != extra+2 {
		t.Fatal(len(rows), err)
	}
	r, err := s.Subscribe(ctx, "pay", base)
	if err != nil || len(r.Registrations) != 2 || len(r.Terminations) != extra || countWatches(t, s, "pay") != 2 {
		t.Fatal(len(r.Registrations), len(r.Terminations), err)
	}
	// Every block was delivered exactly once for the persistent watch.
	got := drainHeights(t, s, "pay", "base")
	want := make([]int64, 0, 30)
	for h := int64(101); h <= 130; h++ {
		want = append(want, h)
	}
	if !slices.Equal(got, want) {
		t.Fatal(got)
	}
}

func TestRegisterHTTP(t *testing.T) {
	ctx := context.Background()
	s, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	const token = "012345678901234567890123"
	h, err := ConsumerHandler(s, map[string]string{"pay": token})
	if err != nil {
		t.Fatal(err)
	}
	post := func(body string, want int) string {
		t.Helper()
		w := httptest.NewRecorder()
		r := httptest.NewRequest("POST", "/v1/subscriptions", strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+token)
		h.ServeHTTP(w, r)
		if w.Code != want {
			t.Fatalf("got %d %s, want %d", w.Code, w.Body, want)
		}
		return w.Body.String()
	}
	watch := `[{"network":"base","address":"` + wallet + `"}]`
	post(watch, 409)
	post(`{"network":"base"}`, 400)
	post(`null`, 400)
	post(`[{"network":"base","address":"bad"}]`, 400)
	if err = s.SeedCursor(ctx, "base", 100); err != nil {
		t.Fatal(err)
	}
	var receipt map[string]json.RawMessage
	if err = json.Unmarshal([]byte(post(watch, 200)), &receipt); err != nil || len(receipt) != 1 || string(receipt["registrations"]) != `[{"network":"base","address":"`+wallet+`","from_height":101}]` {
		t.Fatal(receipt, err)
	}
	if body := post(`[]`, 200); strings.TrimSpace(body) != `{"registrations":[]}` {
		t.Fatal(body)
	}

	srv := httptest.NewServer(h)
	defer srv.Close()
	c := feed.Client{URL: srv.URL, Token: token}
	second := feed.Watch{Network: "base", Address: "0x2222222222222222222222222222222222222222"}
	rows, err := c.Register(ctx, "", []feed.Watch{second, second})
	if err != nil || !slices.Equal(rows, []feed.Registration{{Watch: second, FromHeight: 101}}) {
		t.Fatal(rows, err)
	}
	if rows, err = c.Register(ctx, "", nil); err != nil || len(rows) != 0 {
		t.Fatal(rows, err)
	}
	if countWatches(t, s, "pay") != 2 {
		t.Fatal("client registration replaced watches")
	}
	if _, err = c.Register(ctx, "", []feed.Watch{{Network: "sui", Address: "0x1"}}); !errors.Is(err, feed.ErrCursorNotReady) {
		t.Fatal(err)
	}
}
