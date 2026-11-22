package task

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	scanner "github.com/GMWalletApp/ChainScanner"
	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
)

type recordingFeed struct {
	scanner.Feed
	mu         sync.Mutex
	registered [][]scanner.Watch
	replaced   [][]scanner.Watch
	rows       func([]scanner.Watch) []scanner.Registration
	err        error
}

func (f *recordingFeed) Register(_ context.Context, consumer string, w []scanner.Watch) ([]scanner.Registration, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if consumer != paymentConsumer {
		return nil, errors.New("wrong consumer")
	}
	f.registered = append(f.registered, slices.Clone(w))
	if f.err != nil {
		return nil, f.err
	}
	if f.rows != nil {
		return f.rows(w), nil
	}
	rows := make([]scanner.Registration, len(w))
	for i := range w {
		rows[i] = scanner.Registration{Watch: w[i], FromHeight: 1}
	}
	return rows, nil
}

func (f *recordingFeed) Subscribe(_ context.Context, _ string, w []scanner.Watch) (scanner.SubscriptionReceipt, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.replaced = append(f.replaced, slices.Clone(w))
	return scanner.SubscriptionReceipt{Registrations: []scanner.Registration{}, Terminations: []scanner.Termination{}}, nil
}

func TestEnsureRegistersOnlyTheOrderAddress(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	for i := range 50 {
		if _, err := data.AddWalletAddressWithNetwork(mdb.NetworkEthereum, fmt.Sprintf("0x%040x", i+1)); err != nil {
			t.Fatal(err)
		}
	}
	feed := &recordingFeed{}
	p := &paymentSubscriptions{feed: feed}
	mixed := "0xABCDEFabcdefABCDEFabcdefABCDEFabcdefABCD"
	if err := p.ensure(mdb.NetworkEthereum, mixed); err != nil {
		t.Fatal(err)
	}
	want := []scanner.Watch{{Network: mdb.NetworkEthereum, Address: strings.ToLower(mixed)}}
	if len(feed.registered) != 1 || !slices.Equal(feed.registered[0], want) || len(feed.replaced) != 0 {
		t.Fatalf("register %v, replace %d", feed.registered, len(feed.replaced))
	}
	// Each order asks again: a remote scanner may have lost its state.
	if err := p.ensure(mdb.NetworkEthereum, mixed); err != nil || len(feed.registered) != 2 {
		t.Fatal(err, len(feed.registered))
	}
	if err := p.refresh(context.Background()); err != nil || len(feed.replaced) != 1 || len(feed.replaced[0]) != 50 {
		t.Fatal(err, feed.replaced)
	}

	feed.err = scanner.ErrCursorNotReady
	if err := p.ensure(mdb.NetworkEthereum, mixed); !errors.Is(err, scanner.ErrCursorNotReady) {
		t.Fatal(err)
	}
	feed.err = nil
	feed.rows = func([]scanner.Watch) []scanner.Registration { return []scanner.Registration{} }
	if err := p.ensure(mdb.NetworkEthereum, mixed); err == nil {
		t.Fatal("missing registration accepted")
	}
	if err := p.ensure(mdb.NetworkEthereum, "not-an-address"); err == nil {
		t.Fatal("invalid address accepted")
	}
}

// gatedFeed parks a replacement after refresh has read the wallet list.
type gatedFeed struct {
	scanner.Feed
	entered chan struct{}
	release chan struct{}
}

func (g gatedFeed) Subscribe(ctx context.Context, consumer string, w []scanner.Watch) (scanner.SubscriptionReceipt, error) {
	g.entered <- struct{}{}
	<-g.release
	return g.Feed.Subscribe(ctx, consumer, w)
}

// An address ensured after a refresh read the wallet list must survive that
// refresh's replacement.
func TestEnsuredAddressSurvivesConcurrentRefresh(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	ctx := context.Background()
	const network = mdb.NetworkEthereum
	store, err := scanner.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	scanner.ConfigureNetworks(store, map[string]scanner.Network{network: {Name: network}}, func(context.Context, string) (int64, error) { return 16, nil })
	token := strings.Repeat("e", 32)
	handler, err := scanner.Handler(store, map[string]string{paymentConsumer: token})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(handler)
	defer srv.Close()
	client := scanner.Client{URL: srv.URL, Token: token}
	gate := gatedFeed{Feed: client, entered: make(chan struct{}), release: make(chan struct{})}
	p := &paymentSubscriptions{feed: gate}
	for i := range 3 {
		if _, err := data.AddWalletAddressWithNetwork(network, fmt.Sprintf("0x%040x", 0x100+i)); err != nil {
			t.Fatal(err)
		}
	}
	refreshed := make(chan error, 1)
	go func() { refreshed <- p.refresh(ctx) }()
	<-gate.entered // the wallet list is read and the replacement is pending

	added := fmt.Sprintf("0x%040x", 0x200)
	if _, err := data.AddWalletAddressWithNetwork(network, added); err != nil {
		t.Fatal(err)
	}
	ensured := make(chan error, 1)
	go func() { ensured <- p.ensure(network, added) }()
	select {
	case err := <-ensured:
		ensured <- err // finished during the refresh; the final check decides
	case <-time.After(200 * time.Millisecond):
	}
	close(gate.release)
	if err := <-refreshed; err != nil {
		t.Fatal(err)
	}
	if err := <-ensured; err != nil {
		t.Fatal(err)
	}

	// Removing everything lists exactly the watches that were in place.
	receipt, err := client.Subscribe(ctx, paymentConsumer, []scanner.Watch{})
	if err != nil {
		t.Fatal(err)
	}
	present := map[string]bool{}
	for _, end := range receipt.Terminations {
		present[end.Address] = true
	}
	if !present[added] {
		t.Fatalf("refresh removed the ensured address; remaining %v", present)
	}
	if len(present) != 4 {
		t.Fatalf("%d watches remain", len(present))
	}
}
