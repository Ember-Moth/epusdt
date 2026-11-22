package nodes

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GMWalletApp/ChainScanner/internal/chains"
	"github.com/GMWalletApp/ChainScanner/internal/engine"
	"github.com/GMWalletApp/ChainScanner/internal/feed"
	"github.com/GMWalletApp/ChainScanner/internal/testutil"
)

const (
	watchedWallet   = "0x00000000000000000000000000000000000000a1"
	unwatchedWallet = "0x00000000000000000000000000000000000000b2"
)

// linkedProbe records the address filter of every EVM block read.
type linkedProbe struct {
	tip  int64
	mu   sync.Mutex
	seen map[int64]chains.Watched
}

func (p *linkedProbe) Head(context.Context) (int64, error) { return p.tip, nil }
func (*linkedProbe) Read(context.Context, int64, int64) (int64, []chains.Activity, error) {
	return 0, nil, errors.New("EVM spans are read block by block")
}
func (*linkedProbe) Close() {}
func (p *linkedProbe) ReadLinkedBlock(ctx context.Context, h int64, link *chains.BlockLink) (int64, []chains.Activity, error) {
	match := chains.WatchedAddresses(ctx)
	p.mu.Lock()
	p.seen[h] = match
	p.mu.Unlock()
	*link = chains.BlockLink{Hash: fmt.Sprint("h", h), Parent: fmt.Sprint("h", h-1)}
	var out []chains.Activity
	for _, a := range []string{watchedWallet, unwatchedWallet} {
		if match == nil || match(a) {
			out = append(out, chains.Activity{Height: h, TxID: fmt.Sprint(a, h), TimeMS: 1, Addresses: []string{a}, Data: []byte("{}")})
		}
	}
	return h, out, nil
}

// Both the live queue (Pump) and ScanPending (Step) read under the watch
// filter; the live queue derives its context from the watch period.
func TestPoolReadsCarryTheWatchFilter(t *testing.T) {
	for _, live := range []bool{true, false} {
		t.Run(fmt.Sprint("live=", live), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			db, err := engine.Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if err = db.SeedCursor(ctx, "base", 99); err != nil {
				t.Fatal(err)
			}
			if _, err = db.Subscribe(ctx, "pay", []feed.Watch{{Network: "base", Address: watchedWallet}}); err != nil {
				t.Fatal(err)
			}
			cfg := chains.Network{Name: "base", URL: "http://probe.test", IntervalSeconds: 1}
			p := NewSourcePool(ctx, "base")
			defer p.Close()
			if err = p.SetNodes([]chains.Network{cfg}); err != nil {
				t.Fatal(err)
			}
			probe := &linkedProbe{tip: 103, seen: map[int64]chains.Watched{}}
			p.open = func(context.Context, chains.Network) (chains.Source, error) { return probe, nil }
			for {
				h, err := db.Height(ctx, "base")
				if err != nil {
					t.Fatal(err)
				}
				if h == 103 {
					break
				}
				if live {
					_, err = db.Pump(ctx, cfg, p)
				} else {
					_, err = db.Step(ctx, cfg, p)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			for h := int64(100); h <= 103; h++ {
				match := probe.seen[h]
				if match == nil || !match(watchedWallet) || match(unwatchedWallet) {
					t.Fatal("unfiltered read", h)
				}
				d, err := db.PendingNetwork(ctx, "pay", "base", 0)
				if err != nil || d == nil || d.Activity.TxID != fmt.Sprint(watchedWallet, h) {
					t.Fatal(h, d, err)
				}
				if err = db.Ack(ctx, "pay", d.ID); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

// End to end through NodeRegistry.Run: a watched base block is read with its
// logs by height and fetches only the watched transfer's receipt.
func TestRegistryScanSkipsUnwatchedReceipts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	zeroBloom := "0x" + strings.Repeat("0", 512)
	var head atomic.Int64
	head.Store(99)
	var mu sync.Mutex
	calls := map[string]int{}
	block := func(h int64) map[string]any {
		label := fmt.Sprintf("0x%x", h)
		var txs []any
		for i, to := range []string{watchedWallet, unwatchedWallet} {
			txs = append(txs, map[string]any{"hash": fmt.Sprintf("0x%064x", h*10+int64(i)), "from": "0x00000000000000000000000000000000000000c3", "to": to, "value": "0x5", "nonce": "0x1"})
		}
		return map[string]any{"number": label, "hash": fmt.Sprintf("0x%064x", h), "parentHash": fmt.Sprintf("0x%064x", h-1), "timestamp": "0x10", "logsBloom": zeroBloom, "transactions": txs}
	}
	rpc := testutil.RPCServer(t, func(r testutil.RPCRequest) (any, error) {
		mu.Lock()
		calls[r.Method]++
		mu.Unlock()
		param := func() string { var s string; json.Unmarshal(r.Params[0], &s); return s }
		switch r.Method {
		case "eth_chainId":
			return "0x2105", nil
		case "eth_blockNumber":
			return fmt.Sprintf("0x%x", head.Load()), nil
		case "eth_getBlockByNumber":
			var h int64
			fmt.Sscanf(param(), "0x%x", &h)
			return block(h), nil
		case "eth_getLogs":
			return []any{}, nil
		case "eth_getTransactionReceipt":
			var h, i int64
			id := param()
			fmt.Sscanf(id[len(id)-8:], "%x", &h)
			h, i = h/10, h%10
			return map[string]any{"transactionHash": id, "blockHash": fmt.Sprintf("0x%064x", h), "status": "0x1", "index": i}, nil
		}
		return nil, fmt.Errorf("unexpected %s", r.Method)
	})
	defer rpc.Close()
	store, err := engine.Open(filepath.Join(t.TempDir(), "scanner.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	cfg := chains.Network{Name: "base", URL: rpc.URL, IntervalSeconds: 1}
	registry, err := NewNodeRegistry(ctx, store, []chains.Network{cfg})
	if err != nil {
		t.Fatal(err)
	}
	existing, err := registry.List(ctx, "base")
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range existing {
		if n.Origin == "builtin" {
			if err := registry.Delete(ctx, n.ID); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err = store.SeedCursor(ctx, "base", 99); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Subscribe(ctx, "pay", []feed.Watch{{Network: "base", Address: watchedWallet}}); err != nil {
		t.Fatal(err)
	}
	go registry.Run(ctx, cfg)
	head.Store(103)
	for h := int64(100); h <= 103; h++ {
		d, err := store.PendingNetwork(ctx, "pay", "base", 5*time.Second)
		if err != nil || d == nil || d.Activity.Height != h || d.Activity.TxID != fmt.Sprintf("0x%064x", h*10) {
			t.Fatal(h, d, err)
		}
		if err = store.Ack(ctx, "pay", d.ID); err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if calls["eth_getBlockReceipts"] != 0 || calls["eth_getTransactionReceipt"] != 4 || calls["eth_getLogs"] != 4 {
		t.Fatal("filtered scan requests", calls)
	}
}
