package chains

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GMWalletApp/ChainScanner/internal/testutil"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/tidwall/gjson"
)

// rpcError answers with a specific JSON-RPC error code.
type rpcError struct {
	code int
	msg  string
}

func (e *rpcError) Error() string { return e.msg }

// httpStatus answers with an HTTP error instead of a JSON-RPC response.
type httpStatus int

func (h httpStatus) Error() string { return http.StatusText(int(h)) }

func rpcServer(t *testing.T, reply func(testutil.RPCRequest) (any, error)) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req testutil.RPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			return
		}
		value, err := reply(req)
		var status httpStatus
		if errors.As(err, &status) {
			http.Error(w, status.Error(), int(status))
			return
		}
		response := map[string]any{"jsonrpc": "2.0", "id": req.ID}
		if err != nil {
			code := -32000
			var coded *rpcError
			if errors.As(err, &coded) {
				code = coded.code
			}
			response["error"] = map[string]any{"code": code, "message": err.Error()}
		} else {
			response["result"] = value
		}
		json.NewEncoder(w).Encode(response)
	}))
	t.Cleanup(srv.Close)
	return srv
}

const transferTopic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

func hexAddr(n int) string { return fmt.Sprintf("0x%040x", n) }
func topicAddr(a string) string {
	return "0x000000000000000000000000" + strings.TrimPrefix(a, "0x")
}

type fixtureTx struct {
	from, to, value string
	nonce           uint64
	typ             string
	logs            []map[string]any
}

// evmFixture is one block with consistent logs, receipts and logsBloom.
type evmFixture struct {
	number   int64
	hash     string
	block    map[string]any
	logs     []map[string]any
	receipts []map[string]any
	byTx     map[string]map[string]any
}

func newEVMFixture(number int64, parent string, txs []fixtureTx) *evmFixture {
	f := &evmFixture{number: number, hash: fmt.Sprintf("0x%064x", number*7+1), byTx: map[string]map[string]any{}, logs: []map[string]any{}, receipts: []map[string]any{}}
	label := fmt.Sprintf("0x%x", number)
	var bloom [bloomLength]byte
	hasher := crypto.NewKeccakState()
	var list []any
	for i, t := range txs {
		id := fmt.Sprintf("0x%064x", number*1000+int64(i))
		tx := map[string]any{"hash": id, "from": t.from, "value": t.value, "nonce": fmt.Sprintf("0x%x", t.nonce), "blockHash": f.hash, "blockNumber": label, "transactionIndex": fmt.Sprintf("0x%x", i), "input": "0x"}
		if t.to != "" {
			tx["to"] = t.to
		} else {
			tx["to"] = nil
		}
		if t.typ != "" {
			tx["type"] = t.typ
		}
		list = append(list, tx)
		var logs []any
		for _, l := range t.logs {
			row := map[string]any{"blockHash": f.hash, "blockNumber": label, "transactionHash": id, "transactionIndex": fmt.Sprintf("0x%x", i), "logIndex": fmt.Sprintf("0x%x", len(f.logs)), "removed": false, "data": "0x" + strings.Repeat("0", 63) + "1"}
			for k, v := range l {
				row[k] = v
			}
			raw, _ := json.Marshal(row)
			var parsed map[string]any
			json.Unmarshal(raw, &parsed)
			addBloom(&bloom, hasher, common.HexToAddress(row["address"].(string)).Bytes())
			for _, topic := range row["topics"].([]string) {
				addBloom(&bloom, hasher, common.FromHex(topic))
			}
			f.logs = append(f.logs, row)
			logs = append(logs, row)
		}
		receipt := map[string]any{"blockHash": f.hash, "blockNumber": label, "transactionHash": id, "status": "0x1", "logs": logs, "contractAddress": nil}
		if logs == nil {
			receipt["logs"] = []any{}
		}
		if t.to == "" {
			receipt["contractAddress"] = strings.ToLower(crypto.CreateAddress(common.HexToAddress(t.from), t.nonce).Hex())
		}
		f.receipts = append(f.receipts, receipt)
		f.byTx[id] = receipt
	}
	if list == nil {
		list = []any{}
	}
	f.block = map[string]any{"number": label, "hash": f.hash, "parentHash": parent, "timestamp": "0x10", "logsBloom": fmt.Sprintf("0x%x", bloom[:]), "transactions": list}
	return f
}

// evmNode serves fixtures by height and records each method it receives.
type evmNode struct {
	t        *testing.T
	chainID  string
	mu       sync.Mutex
	calls    map[string]int
	blocks   map[string]*evmFixture
	byHash   map[string]*evmFixture
	override func(testutil.RPCRequest) (any, error, bool)
}

func (n *evmNode) count(method string) int { n.mu.Lock(); defer n.mu.Unlock(); return n.calls[method] }

func (n *evmNode) serve(r testutil.RPCRequest) (any, error) {
	if r.Method != "eth_chainId" {
		n.mu.Lock()
		n.calls[r.Method]++
		n.mu.Unlock()
	}
	if n.override != nil {
		if v, err, ok := n.override(r); ok {
			return v, err
		}
	}
	param := func(i int) string {
		var s string
		json.Unmarshal(r.Params[i], &s)
		return s
	}
	switch r.Method {
	case "eth_chainId":
		return n.chainID, nil
	case "eth_getBlockByNumber":
		if f := n.blocks[param(0)]; f != nil {
			return f.block, nil
		}
		return nil, nil
	case "eth_getBlockReceipts":
		if f := n.blocks[param(0)]; f != nil {
			return f.receipts, nil
		}
		return nil, nil
	case "eth_getLogs":
		var q map[string]string
		json.Unmarshal(r.Params[0], &q)
		f := n.blocks[q["fromBlock"]]
		if q["blockHash"] != "" {
			f = n.byHash[q["blockHash"]]
		} else if q["fromBlock"] != q["toBlock"] {
			n.t.Error("range spans several blocks", q)
		}
		if f == nil {
			return []any{}, nil
		}
		return f.logs, nil
	case "eth_getTransactionReceipt":
		for _, f := range n.blocks {
			if r := f.byTx[param(0)]; r != nil {
				return r, nil
			}
		}
		return nil, nil
	}
	n.t.Error("unexpected method", r.Method)
	return nil, errors.New("unexpected method")
}

func startEVM(t *testing.T, cfg Network, fixtures ...*evmFixture) (*evmNode, *jsonSource) {
	t.Helper()
	n := &evmNode{t: t, chainID: fmt.Sprintf("0x%x", EVMChainID(cfg.Name)), calls: map[string]int{}, blocks: map[string]*evmFixture{}, byHash: map[string]*evmFixture{}}
	for _, f := range fixtures {
		n.blocks[fmt.Sprintf("0x%x", f.number)] = f
		n.byHash[f.hash] = f
	}
	cfg.URL = rpcServer(t, n.serve).URL
	src, err := NewSource(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(src.Close)
	return n, src.(*jsonSource)
}

func watching(addresses ...string) context.Context {
	set := map[string]bool{}
	for _, a := range addresses {
		set[a] = true
	}
	return WithWatched(context.Background(), func() Watched { return func(a string) bool { return set[a] } })
}

var (
	payer    = hexAddr(0xa1)
	merchant = hexAddr(0xb2)
	stranger = hexAddr(0xc3)
	token    = hexAddr(0xd4)
)

func busyBlock(number int64) *evmFixture {
	return newEVMFixture(number, fmt.Sprintf("0x%064x", (number-1)*7+1), []fixtureTx{
		{from: payer, to: merchant, value: "0x10"}, // watched native transfer
		{from: payer, to: stranger, value: "0x10"}, // unwatched native transfer
		{from: payer, to: token, value: "0x0", logs: []map[string]any{{"address": token, "topics": []string{transferTopic, topicAddr(payer), topicAddr(merchant)}}}}, // watched token transfer
		{from: payer, to: token, value: "0x0", logs: []map[string]any{{"address": token, "topics": []string{transferTopic, topicAddr(payer), topicAddr(stranger)}}}}, // unwatched token transfer
		{from: stranger, nonce: 7}, // creation of an unwatched contract
		{from: payer, nonce: 9},    // creation of a watched contract
	})
}

func txIDs(events []Activity) []string {
	var out []string
	for _, e := range events {
		out = append(out, e.TxID)
	}
	return out
}

func TestEVMReadsLogsByHeightWithBlock(t *testing.T) {
	f := busyBlock(5)
	node, s := startEVM(t, Network{Name: "ethereum"}, f)
	// Hold the block until the log request has arrived: a serial reader
	// never sends it and times out.
	logsArrived := make(chan struct{})
	var once sync.Once
	node.override = func(r testutil.RPCRequest) (any, error, bool) {
		switch r.Method {
		case "eth_getLogs":
			once.Do(func() { close(logsArrived) })
		case "eth_getBlockByNumber":
			select {
			case <-logsArrived:
			case <-time.After(5 * time.Second):
				return nil, errors.New("block requested alone"), true
			}
		}
		return nil, nil, false
	}
	var link BlockLink
	end, events, err := s.ReadLinkedBlock(context.Background(), 5, &link)
	if err != nil || end != 5 || len(events) != 6 || link.Hash != f.hash {
		t.Fatal(end, len(events), link, err)
	}
	if node.count("eth_getLogs") != 1 || node.count("eth_getBlockByNumber") != 1 || node.count("eth_getBlockReceipts") != 0 {
		t.Fatal("expected one block and one height-keyed log request", node.calls)
	}
	// Creations are the only transactions with receipts on a chain without native evidence.
	if node.count("eth_getTransactionReceipt") != 2 {
		t.Fatal("creation receipts", node.calls)
	}
	if !slices.Contains(events[2].Addresses, merchant) || len(gjsonLogs(events[2].Data)) != 1 {
		t.Fatal("token transfer evidence", events[2])
	}
}

func gjsonLogs(data json.RawMessage) []json.RawMessage {
	var e Evidence
	json.Unmarshal(data, &e)
	return e.Logs
}

func TestEVMHeightLogsFallBackToBlockHashUnlessProven(t *testing.T) {
	cases := map[string]func(f *evmFixture) any{
		"missing log": func(f *evmFixture) any { return f.logs[:len(f.logs)-1] },
		"other block": func(f *evmFixture) any {
			rows := slices.Clone(f.logs)
			moved := map[string]any{}
			for k, v := range rows[0] {
				moved[k] = v
			}
			moved["blockHash"] = "0xother"
			rows[0] = moved
			return rows
		},
		"removed": func(f *evmFixture) any {
			rows := slices.Clone(f.logs)
			moved := map[string]any{}
			for k, v := range rows[0] {
				moved[k] = v
			}
			moved["removed"] = true
			rows[0] = moved
			return rows
		},
		"empty":        func(f *evmFixture) any { return []any{} },
		"unsupported":  func(f *evmFixture) any { return errors.New("method not supported") },
		"no bloom":     nil,
		"zero log set": nil,
	}
	for name, rangeAnswer := range cases {
		t.Run(name, func(t *testing.T) {
			f := busyBlock(5)
			if name == "no bloom" {
				delete(f.block, "logsBloom")
			}
			node, s := startEVM(t, Network{Name: "polygon"}, f)
			if name == "zero log set" {
				// A block without logs is proven by its zero bloom.
				f = newEVMFixture(5, "0xparent", []fixtureTx{{from: payer, to: merchant, value: "0x1"}})
				node.blocks["0x5"], node.byHash[f.hash] = f, f
			}
			node.override = func(r testutil.RPCRequest) (any, error, bool) {
				var q map[string]string
				if r.Method != "eth_getLogs" || rangeAnswer == nil || json.Unmarshal(r.Params[0], &q) != nil || q["blockHash"] != "" {
					return nil, nil, false
				}
				if err, ok := rangeAnswer(f).(error); ok {
					return nil, err, true
				}
				return rangeAnswer(f), nil, true
			}
			_, events, err := s.ReadLinkedBlock(context.Background(), 5, nil)
			if err != nil || len(events) != len(f.block["transactions"].([]any)) {
				t.Fatal(len(events), err)
			}
			want := 2
			if name == "zero log set" {
				want = 1
			}
			if node.count("eth_getLogs") != want {
				t.Fatal("log requests", node.calls)
			}
			got := 0
			for _, e := range events {
				got += len(gjsonLogs(e.Data))
			}
			if got != len(f.logs) {
				t.Fatal("logs", got, len(f.logs))
			}
		})
	}
}

func TestEVMRateLimitedLogsAreNotRetriedByHash(t *testing.T) {
	node, s := startEVM(t, Network{Name: "ethereum"}, busyBlock(5))
	node.override = func(r testutil.RPCRequest) (any, error, bool) {
		if r.Method == "eth_getLogs" {
			return nil, httpStatus(http.StatusTooManyRequests), true
		}
		return nil, nil, false
	}
	if _, _, err := s.ReadLinkedBlock(context.Background(), 5, nil); !rateLimited(err) {
		t.Fatal("expected the provider's rate limit", err)
	}
	if node.count("eth_getLogs") != 1 || node.count("eth_getBlockReceipts") != 0 {
		t.Fatal("retried a rate-limited provider", node.calls)
	}
}

func TestEVMNativeReceiptsRequestedWithBlock(t *testing.T) {
	f := busyBlock(5)
	node, s := startEVM(t, Network{Name: "base"}, f)
	receiptsArrived := make(chan struct{})
	var once sync.Once
	node.override = func(r testutil.RPCRequest) (any, error, bool) {
		switch r.Method {
		case "eth_getBlockReceipts":
			once.Do(func() { close(receiptsArrived) })
		case "eth_getBlockByNumber":
			select {
			case <-receiptsArrived:
			case <-time.After(5 * time.Second):
				return nil, errors.New("block requested alone"), true
			}
		}
		return nil, nil, false
	}
	_, events, err := s.ReadLinkedBlock(context.Background(), 5, nil)
	if err != nil || len(events) != 6 {
		t.Fatal(len(events), err)
	}
	if node.count("eth_getBlockReceipts") != 1 || node.count("eth_getLogs") != 0 || node.count("eth_getTransactionReceipt") != 0 {
		t.Fatal("unfiltered native read", node.calls)
	}
}

func TestEVMFilterKeepsOnlyWatchedTransactionsAndTheirReceipts(t *testing.T) {
	f := busyBlock(5)
	created := strings.ToLower(crypto.CreateAddress(common.HexToAddress(payer), 9).Hex())
	_, plain := startEVM(t, Network{Name: "base"}, f)
	_, all, err := plain.ReadLinkedBlock(context.Background(), 5, nil)
	if err != nil {
		t.Fatal(err)
	}
	node, s := startEVM(t, Network{Name: "base"}, f)
	_, got, err := s.ReadLinkedBlock(watching(merchant, created), 5, nil)
	if err != nil {
		t.Fatal(err)
	}
	ids := txIDs(all)
	if want := []string{ids[0], ids[2], ids[5]}; !slices.Equal(txIDs(got), want) {
		t.Fatal("kept", txIDs(got), "want", want)
	}
	// Kept activities are identical to the unfiltered read.
	for _, e := range got {
		i := slices.Index(ids, e.TxID)
		if string(e.Data) != string(all[i].Data) || !slices.Equal(e.Addresses, all[i].Addresses) {
			t.Fatalf("activity %s differs:\n%s\n%s", e.TxID, e.Data, all[i].Data)
		}
	}
	// Logs come by height; only the watched native transfer and the watched
	// creation need receipts.
	if node.count("eth_getBlockReceipts") != 0 || node.count("eth_getLogs") != 1 || node.count("eth_getTransactionReceipt") != 2 {
		t.Fatal("filtered native read", node.calls)
	}
}

func TestEVMFilterDecidesUnusualCreationsByReceipt(t *testing.T) {
	f := newEVMFixture(5, "0xparent", []fixtureTx{{from: payer, nonce: 3, typ: "0x7e"}, {from: stranger, nonce: 4, typ: "0x7e"}})
	created := strings.ToLower(crypto.CreateAddress(common.HexToAddress(payer), 3).Hex())
	node, s := startEVM(t, Network{Name: "optimism"}, f)
	_, got, err := s.ReadLinkedBlock(watching(created), 5, nil)
	if err != nil || len(got) != 1 || !slices.Contains(got[0].Addresses, created) {
		t.Fatal(got, err)
	}
	if node.count("eth_getTransactionReceipt") != 2 {
		t.Fatal("deposit creations need receipts", node.calls)
	}
}

func TestEVMFilterWithoutBlockReceiptsFetchesOnlyKeptReceipts(t *testing.T) {
	var txs []fixtureTx
	for i := range 250 {
		txs = append(txs, fixtureTx{from: hexAddr(0x1000 + i), to: hexAddr(0x2000 + i%50), value: "0x1"})
	}
	txs[17].to = merchant
	f := newEVMFixture(5, "0xparent", txs)
	node, s := startEVM(t, Network{Name: "base"}, f)
	node.override = func(r testutil.RPCRequest) (any, error, bool) {
		if r.Method == "eth_getBlockReceipts" {
			return nil, errors.New("the method eth_getBlockReceipts does not exist"), true
		}
		return nil, nil, false
	}
	_, got, err := s.ReadLinkedBlock(watching(merchant), 5, nil)
	if err != nil || len(got) != 1 {
		t.Fatal(len(got), err)
	}
	if n := node.count("eth_getTransactionReceipt"); n != 1 {
		t.Fatal("receipt requests", n)
	}
	// Without a filter every value transfer still needs its own receipt.
	_, all, err := s.ReadLinkedBlock(context.Background(), 5, nil)
	if err != nil || len(all) != 250 || node.count("eth_getTransactionReceipt") != 251 {
		t.Fatal(len(all), err, node.calls)
	}
}

func TestEVMAvalancheKeepsHashBoundReceiptsWithFilter(t *testing.T) {
	f := busyBlock(5)
	f.block["logsBloom"] = "0x" + strings.Repeat("f", 512) // headers settle an earlier block
	node, s := startEVM(t, Network{Name: "avalanche"}, f)
	_, got, err := s.ReadLinkedBlock(watching(merchant), 5, nil)
	if err != nil || len(got) != 2 {
		t.Fatal(len(got), err)
	}
	if node.count("eth_getLogs") != 0 || node.count("eth_getBlockReceipts") != 1 || node.count("eth_getTransactionReceipt") != 0 {
		t.Fatal(node.calls)
	}
}

func TestEVMTraceFramesFollowTransactionOrder(t *testing.T) {
	f := newEVMFixture(5, "0xparent", []fixtureTx{{from: payer, to: token, value: "0x0"}, {from: stranger, to: token, value: "0x0"}})
	node, src := startEVM(t, Network{Name: "ethereum", TraceCalls: true}, f)
	inner := []string{merchant, hexAddr(0xe5)}
	node.override = func(r testutil.RPCRequest) (any, error, bool) {
		if r.Method != "debug_traceBlockByHash" {
			return nil, nil, false
		}
		var frames []any
		for i := range 2 {
			frames = append(frames, map[string]any{"result": map[string]any{"from": payer, "to": token, "calls": []any{map[string]any{"from": token, "to": inner[i]}}}})
		}
		return frames, nil, true
	}
	_, events, err := src.ReadLinkedBlock(context.Background(), 5, nil)
	if err != nil || len(events) != 2 {
		t.Fatal(events, err)
	}
	for i, e := range events {
		if !slices.Contains(e.Addresses, inner[i]) || slices.Contains(e.Addresses, inner[1-i]) {
			t.Fatal("trace attached to the wrong transaction", i, e.Addresses)
		}
	}
	_, kept, err := src.ReadLinkedBlock(watching(inner[1]), 5, nil)
	if err != nil || len(kept) != 1 || kept[0].TxID != events[1].TxID {
		t.Fatal("internal call watch", kept, err)
	}
}

type linkedBlocks struct {
	mu     sync.Mutex
	parent map[int64]string
	fail   map[int64]bool
	reads  []int64
}

func (l *linkedBlocks) Head(context.Context) (int64, error) { return 100, nil }
func (l *linkedBlocks) Read(context.Context, int64, int64) (int64, []Activity, error) {
	return 0, nil, errors.New("unused")
}
func (*linkedBlocks) Close() {}
func (l *linkedBlocks) ReadLinkedBlock(_ context.Context, h int64, link *BlockLink) (int64, []Activity, error) {
	l.mu.Lock()
	l.reads = append(l.reads, h)
	l.mu.Unlock()
	if l.fail[h] {
		return 0, nil, errors.New("unavailable")
	}
	parent := fmt.Sprintf("h%d", h-1)
	if p, ok := l.parent[h]; ok {
		parent = p
	}
	*link = BlockLink{Hash: fmt.Sprintf("h%d", h), Parent: parent}
	return h, []Activity{{Height: h, TxID: fmt.Sprint(h)}}, nil
}

func TestReadSpanLinksEveryEVMBlock(t *testing.T) {
	src := &linkedBlocks{}
	span, link, rows, err := ReadSpan(context.Background(), "base", src, Span{First: 10, Last: 13})
	if err != nil || span.Last != 13 || link != (BlockLink{Hash: "h13", Parent: "h9"}) || !slices.Equal(txIDs(rows), []string{"10", "11", "12", "13"}) {
		t.Fatal(span, link, txIDs(rows), err)
	}
	src = &linkedBlocks{parent: map[int64]string{12: "reorged"}}
	if _, _, _, err = ReadSpan(context.Background(), "base", src, Span{First: 10, Last: 13}); err == nil {
		t.Fatal("accepted a broken chain inside the span")
	}
	src = &linkedBlocks{fail: map[int64]bool{12: true}}
	if _, _, rows, err = ReadSpan(context.Background(), "base", src, Span{First: 10, Last: 13}); err == nil || rows != nil {
		t.Fatal("partial span accepted", rows, err)
	}
	// A request beyond the read bound is answered in part.
	src = &linkedBlocks{}
	span, link, rows, err = ReadSpan(context.Background(), "base", src, Span{First: 10, Last: 30})
	if err != nil || span.Last != 10+MaxLinkedSpan-1 || link.Hash != fmt.Sprintf("h%d", span.Last) || len(rows) != MaxLinkedSpan {
		t.Fatal(span, link, len(rows), err)
	}
}

func TestEVMSpanReadsBlocksConcurrently(t *testing.T) {
	var fixtures []*evmFixture
	parent := fmt.Sprintf("0x%064x", 9*7+1)
	for h := int64(10); h <= 13; h++ {
		f := newEVMFixture(h, parent, []fixtureTx{{from: payer, to: merchant, value: "0x1"}})
		fixtures = append(fixtures, f)
		parent = f.hash
	}
	node, s := startEVM(t, Network{Name: "ethereum"}, fixtures...)
	var mu sync.Mutex
	waiting := 0
	all := make(chan struct{})
	node.override = func(r testutil.RPCRequest) (any, error, bool) {
		if r.Method != "eth_getBlockByNumber" {
			return nil, nil, false
		}
		mu.Lock()
		waiting++
		if waiting == 4 {
			close(all)
		}
		mu.Unlock()
		select {
		case <-all:
		case <-time.After(5 * time.Second):
			return nil, errors.New("blocks requested one at a time"), true
		}
		return nil, nil, false
	}
	span, link, rows, err := ReadSpan(context.Background(), "ethereum", s, Span{First: 10, Last: 13})
	if err != nil || span.Last != 13 || len(rows) != 4 || link.Hash != fixtures[3].hash || link.Parent != fixtures[0].block["parentHash"] {
		t.Fatal(span, link, len(rows), err)
	}
}

func TestCanonicalEVMIsLowercaseHex(t *testing.T) {
	for _, a := range []string{"0x52908400098527886E0F7030069857D2E4169EE7", "52908400098527886e0f7030069857d2e4169ee7", "0X52908400098527886E0F7030069857D2E4169EE7", " 0xde709f2102306220921060314715629080e2fb77 "} {
		got, err := Canonical("base", a)
		want := strings.ToLower(common.HexToAddress(strings.TrimSpace(a)).Hex())
		if err != nil || got != want {
			t.Fatal(a, got, want, err)
		}
	}
	if _, err := Canonical("base", "0x123"); err == nil {
		t.Fatal("short address accepted")
	}
}

// solanaNode serves one slot in full or accounts detail, plus getTransaction.
type solanaNode struct {
	mu      sync.Mutex
	calls   map[string]int
	slot    int64
	txs     []map[string]any
	details []string
	reject  bool
}

func (n *solanaNode) serve(r testutil.RPCRequest) (any, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.calls[r.Method]++
	switch r.Method {
	case "getBlocks":
		return []int64{n.slot}, nil
	case "getBlock":
		var opts map[string]any
		json.Unmarshal(r.Params[1], &opts)
		detail := opts["transactionDetails"].(string)
		n.details = append(n.details, detail)
		if detail == "accounts" && n.reject {
			return nil, &rpcError{-32602, "Invalid params"}
		}
		var txs []any
		for _, tx := range n.txs {
			if detail == "full" {
				txs = append(txs, tx)
				continue
			}
			msg := tx["transaction"].(map[string]any)["message"].(map[string]any)
			meta := tx["meta"].(map[string]any)
			txs = append(txs, map[string]any{
				"transaction": map[string]any{"signatures": tx["transaction"].(map[string]any)["signatures"], "accountKeys": msg["accountKeys"]},
				"meta":        map[string]any{"err": nil, "preTokenBalances": meta["preTokenBalances"], "postTokenBalances": meta["postTokenBalances"]},
				"version":     tx["version"],
			})
		}
		return map[string]any{"blockhash": "slot-hash", "blockTime": 10, "transactions": txs}, nil
	case "getTransaction":
		var sig string
		json.Unmarshal(r.Params[0], &sig)
		for _, tx := range n.txs {
			if tx["transaction"].(map[string]any)["signatures"].([]string)[0] == sig {
				return map[string]any{"slot": n.slot, "blockTime": 10, "meta": tx["meta"], "transaction": tx["transaction"], "version": tx["version"]}, nil
			}
		}
		return nil, nil
	}
	return nil, fmt.Errorf("unexpected %s", r.Method)
}

func solanaTx(sig string, keys []string, owner string) map[string]any {
	var accounts []any
	for _, k := range keys {
		accounts = append(accounts, map[string]any{"pubkey": k, "signer": false, "source": "transaction", "writable": true})
	}
	balances := []any{}
	if owner != "" {
		balances = append(balances, map[string]any{"owner": owner, "mint": "Mint1111111111111111111111111111111111111111"})
	}
	return map[string]any{
		"transaction": map[string]any{"signatures": []string{sig}, "message": map[string]any{"accountKeys": accounts, "instructions": []any{map[string]any{"programId": "11111111111111111111111111111111", "parsed": map[string]any{"type": "transfer"}}}}},
		"meta":        map[string]any{"err": nil, "fee": 5000, "innerInstructions": []any{}, "preTokenBalances": balances, "postTokenBalances": balances, "logMessages": []string{"x"}},
		"version":     0,
	}
}

func TestSolanaFilterFetchesOnlyWatchedTransactions(t *testing.T) {
	wallet := "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
	owner := "So11111111111111111111111111111111111111112"
	other := "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin"
	node := &solanaNode{calls: map[string]int{}, slot: 101, txs: []map[string]any{
		solanaTx("a", []string{other}, ""),
		solanaTx("b", []string{other, wallet}, ""),
		solanaTx("c", []string{other}, owner),
	}}
	srv := rpcServer(t, node.serve)
	src, err := NewSource(context.Background(), Network{Name: "solana", URL: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	s := src.(*jsonSource)
	full, err := s.solanaBlock(context.Background(), 101)
	if err != nil || len(full) != 3 {
		t.Fatal(full, err)
	}
	got, err := s.solanaBlock(watching(wallet, owner), 101)
	if err != nil || !slices.Equal(txIDs(got), []string{"b", "c"}) {
		t.Fatal(txIDs(got), err)
	}
	for i, e := range got {
		if string(e.Data) != string(full[i+1].Data) || !slices.Equal(e.Addresses, full[i+1].Addresses) || e.TimeMS != full[i+1].TimeMS {
			t.Fatalf("watched transaction differs from the full block:\n%s\n%s", e.Data, full[i+1].Data)
		}
	}
	if node.calls["getTransaction"] != 2 || !slices.Equal(node.details, []string{"full", "accounts"}) {
		t.Fatal(node.calls, node.details)
	}
	// A slot with many watched transactions reads the full block once more.
	node.details = nil
	for i := range solanaTransactionReads {
		node.txs = append(node.txs, solanaTx(fmt.Sprint("w", i), []string{wallet}, ""))
	}
	got, err = s.solanaBlock(watching(wallet), 101)
	if err != nil || len(got) != solanaTransactionReads+1 || !slices.Equal(node.details, []string{"accounts", "full"}) || node.calls["getTransaction"] != 2 {
		t.Fatal(len(got), err, node.details, node.calls)
	}
	// A provider without the accounts detail level serves the full block.
	node.details, node.reject = nil, true
	got, err = s.solanaBlock(watching(owner), 101)
	if err != nil || !slices.Equal(txIDs(got), []string{"c"}) || !slices.Equal(node.details, []string{"accounts", "full"}) {
		t.Fatal(txIDs(got), err, node.details)
	}
}

func TestSolanaWatchedTransactionMustBelongToSlot(t *testing.T) {
	wallet := "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
	node := &solanaNode{calls: map[string]int{}, slot: 101, txs: []map[string]any{solanaTx("b", []string{wallet}, "")}}
	srv := rpcServer(t, func(r testutil.RPCRequest) (any, error) {
		v, err := node.serve(r)
		if r.Method == "getTransaction" {
			v.(map[string]any)["slot"] = 102
		}
		return v, err
	})
	src, err := NewSource(context.Background(), Network{Name: "solana", URL: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	if _, err := src.(*jsonSource).solanaBlock(watching(wallet), 101); err == nil {
		t.Fatal("accepted a transaction from another slot")
	}
}

// Robinhood Chain block 120540528: three logs and the header's logsBloom.
func TestLogBloomMatchesARealHeader(t *testing.T) {
	const header = "0x00000040000000000000000000000000010000000000000000000000000000100000000000000000000000000000000000020000040000000000000000000000000000000008400000000008000000000000000000000020000000000000000000000000000000000000000000000000000000000080400000000010000000000000000000000000000000000000000000080000010000000000000000000000000000000000000000000022000000000000000000000000000000000000000000000402000000000000000000000000000000000000000100400000000000000000000000000000004000000100000000000000000000000000400002000000"
	const logs = `[{"address":"0x62b5c53fed197341fe69a0943083b876b6bb3477","topics":["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef","0x00000000000000000000000057395509edc5e4dbe589c7b934312ba99d848c9d","0x0000000000000000000000008876789976decbfcbbbe364623c63652db8c0904"]},{"address":"0x8366a39cc670b4001a1121b8f6a443a643e40951","topics":["0x40e9cecb9f5f1f1c5b9c97dec2917b7ee92e57ba5563708daca94dd84ad7112f","0x399db262dc77724b9fd3e39d34c027602119ba044fac5eaaa939e12f0c5e7ee8","0x0000000000000000000000008876789976decbfcbbbe364623c63652db8c0904"]},{"address":"0x62b5c53fed197341fe69a0943083b876b6bb3477","topics":["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef","0x0000000000000000000000008876789976decbfcbbbe364623c63652db8c0904","0x0000000000000000000000008366a39cc670b4001a1121b8f6a443a643e40951"]}]`
	var rows []map[string]any
	if err := json.Unmarshal([]byte(logs), &rows); err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		r["blockHash"], r["blockNumber"], r["removed"] = "0xb", "0x72f4c70", false
	}
	raw, _ := json.Marshal(rows)
	block, _ := json.Marshal(map[string]any{"hash": "0xb", "number": "0x72f4c70", "logsBloom": header})
	byHash := map[string][]gjson.Result{}
	if err := bindRangeLogs(gjson.ParseBytes(raw), gjson.ParseBytes(block), byHash); err != nil {
		t.Fatal(err)
	}
	// Dropping any one log changes the bloom.
	for i := range rows {
		fewer, _ := json.Marshal(slices.Delete(slices.Clone(rows), i, i+1))
		if bindRangeLogs(gjson.ParseBytes(fewer), gjson.ParseBytes(block), map[string][]gjson.Result{}) == nil {
			t.Fatal("accepted logs without", i)
		}
	}
	var bloom [bloomLength]byte
	for _, bad := range []string{`{"address":"0x12","topics":[]}`, `{"address":"` + payer + `","topics":["0x12"]}`, `{"address":"` + payer + `","topics":["0x` + strings.Repeat("g", 64) + `"]}`} {
		if addLogBloom(&bloom, crypto.NewKeccakState(), gjson.Parse(bad)) {
			t.Fatal("accepted", bad)
		}
	}
}
