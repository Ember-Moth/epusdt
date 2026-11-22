package chains

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/tidwall/gjson"
	"golang.org/x/sync/errgroup"
)

// Head notifications only wake the scheduler. The read endpoint supplies
// the chain ID, authoritative height and block contents.
func evmHeads(ctx context.Context, c *rpc.Client, observe func(int64)) error {
	headers := make(chan struct{ Number hexutil.Uint64 }, 8)
	setup, cancel := context.WithTimeout(ctx, 8*time.Second)
	sub, err := c.EthSubscribe(setup, headers, "newHeads")
	cancel()
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-sub.Err():
			return err
		case h := <-headers:
			if height := int64(h.Number); height > 0 {
				observe(height)
			}
		}
	}
}

// BlockLink identifies the exact EVM block whose transactions were decoded.
type BlockLink struct{ Hash, Parent string }

// receiptReads bounds the per-transaction receipt requests one block may have
// outstanding against its provider.
const receiptReads = 4

// unboundLogs lists networks whose header logsBloom does not commit to the
// block's own logs, so logs requested by height cannot be proven to belong to
// the block. Avalanche C-Chain headers settle an earlier block's results
// (settledHeight); sampled headers matched neither their logs nor the OR of
// their receipts' blooms.
var unboundLogs = map[string]bool{"avalanche": true}

func rateLimited(err error) bool {
	var status rpc.HTTPError
	return errors.Is(err, ErrRateLimited) || (errors.As(err, &status) && status.StatusCode == 429)
}

// ReadLinkedBlock decodes one EVM block. Its log or receipt evidence is
// requested by height together with the block, and every row is then bound
// to the block hash. When the read carries a Watched filter, transactions that
// touch no watched address are skipped before their evidence is assembled or
// their receipts fetched.
func (s *jsonSource) ReadLinkedBlock(ctx context.Context, h int64, link *BlockLink) (int64, []Activity, error) {
	label := fmt.Sprintf("0x%x", h)
	match := WatchedAddresses(ctx)
	native := NativeAsset(s.n.Name) != ""
	// Whole-block receipts carry the status of every native transfer. With a
	// filter, logs locate the candidates and only those fetch a receipt,
	// unless the network's logs cannot be bound to the block by height.
	wholeReceipts := native && (match == nil || unboundLogs[s.n.Name])

	call, cancel := context.WithCancel(ctx)
	defer cancel()
	var evidence gjson.Result
	var evidenceErr error
	evidenceDone := make(chan struct{})
	go func() {
		defer close(evidenceDone)
		switch {
		case wholeReceipts:
			evidence, evidenceErr = s.call(call, "eth_getBlockReceipts", label)
		case unboundLogs[s.n.Name]:
			evidenceErr = errors.New("EVM logs need the block hash")
		default:
			evidence, evidenceErr = s.call(call, "eth_getLogs", map[string]string{"fromBlock": label, "toBlock": label})
		}
	}()
	block, err := s.call(call, "eth_getBlockByNumber", label, true)
	if err == nil && (block.Get("number").String() != label || !block.Get("transactions").IsArray() || block.Get("hash").String() == "") {
		err = errors.New("incomplete EVM block")
	}
	var stamp int64
	if err == nil {
		stamp, err = strconv.ParseInt(block.Get("timestamp").String(), 0, 64)
	}
	if err != nil {
		cancel()
		<-evidenceDone
		return 0, nil, err
	}
	blockHash := block.Get("hash").String()
	txs := block.Get("transactions").Array()
	if link != nil {
		*link = BlockLink{Hash: blockHash, Parent: block.Get("parentHash").String()}
	}

	// Call traces are keyed by the block hash, so they start once it is known.
	var frames gjson.Result
	var traceErr error
	traceDone := make(chan struct{})
	if s.n.TraceCalls {
		go func() {
			defer close(traceDone)
			frames, traceErr = s.call(call, "debug_traceBlockByHash", blockHash, map[string]string{"tracer": "callTracer"})
		}()
		defer func() { cancel(); <-traceDone }()
	}

	byHash := map[string][]gjson.Result{}
	receipts := map[string]gjson.Result{}
	<-evidenceDone
	if evidenceErr == nil {
		if wholeReceipts {
			evidenceErr = bindReceipts(evidence, txs, blockHash, receipts, byHash)
		} else {
			evidenceErr = bindRangeLogs(evidence, block, byHash)
		}
	}
	if evidenceErr != nil {
		if rateLimited(evidenceErr) {
			return 0, nil, evidenceErr
		}
		// The height-keyed response was unavailable or could not be proven to
		// belong to this block. Ask for the logs of this exact block hash.
		clear(byHash)
		clear(receipts)
		logs, err := s.call(ctx, "eth_getLogs", map[string]string{"blockHash": blockHash})
		switch {
		case err == nil:
			if err = bindHashLogs(logs, blockHash, byHash); err != nil {
				return 0, nil, err
			}
		case wholeReceipts || rateLimited(err):
			return 0, nil, err
		default:
			rows, err := s.call(ctx, "eth_getBlockReceipts", label)
			if err == nil {
				err = bindReceipts(rows, txs, blockHash, receipts, byHash)
			}
			if err != nil {
				return 0, nil, err
			}
		}
	}

	traces := map[string]gjson.Result{}
	if s.n.TraceCalls {
		<-traceDone
		if traceErr != nil {
			return 0, nil, traceErr
		}
		if !frames.IsArray() || len(frames.Array()) != len(txs) {
			return 0, nil, errors.New("incomplete EVM call traces")
		}
		for i, f := range frames.Array() {
			if !f.Get("result").IsObject() {
				return 0, nil, errors.New("missing EVM call frame")
			}
			traces[txs[i].Get("hash").String()] = f.Get("result")
		}
	}

	type candidate struct {
		tx       gjson.Result
		event    Activity
		receipt  bool // needs a receipt in its evidence
		creation bool
		decide   bool // only the receipt's contract address can decide the filter
	}
	var picked []candidate
	for _, tx := range txs {
		id := tx.Get("hash").String()
		if id == "" {
			return 0, nil, errors.New("missing EVM transaction hash")
		}
		e := Activity{Height: h, TxID: id, TimeMS: stamp * 1000}
		addAddress(&e.Addresses, s.n.Name, tx.Get("from").String())
		addAddress(&e.Addresses, s.n.Name, tx.Get("to").String())
		for _, l := range byHash[id] {
			addAddress(&e.Addresses, s.n.Name, l.Get("address").String())
			if topics := l.Get("topics").Array(); len(topics) > 1 {
				indexTopics(&e.Addresses, s.n.Name, topics[1:])
			}
		}
		if trace, ok := traces[id]; ok {
			traceAddresses(&e.Addresses, s.n.Name, trace)
		}
		creation := strings.TrimSpace(tx.Get("to").String()) == ""
		value := tx.Get("value").String()
		c := candidate{tx: tx, event: e, creation: creation, receipt: creation || (native && value != "" && value != "0x0")}
		if match != nil && !anyWatched(match, e.Addresses) {
			if !creation {
				continue
			}
			created, known := createdAddress(tx)
			if known && !match(created) {
				continue
			}
			c.decide = !known
		}
		picked = append(picked, c)
	}

	// Without whole-block receipts, fetch only the receipts the kept
	// transactions need.
	missing := make([]gjson.Result, len(picked))
	group, fetch := errgroup.WithContext(ctx)
	group.SetLimit(receiptReads)
	for i := range picked {
		id := picked[i].event.TxID
		if _, ok := receipts[id]; ok || !picked[i].receipt {
			continue
		}
		group.Go(func() error {
			r, err := s.call(fetch, "eth_getTransactionReceipt", id)
			missing[i] = r
			return err
		})
	}
	if err := group.Wait(); err != nil {
		return 0, nil, err
	}

	out := make([]Activity, 0, len(picked))
	for i := range picked {
		c := &picked[i]
		e := c.event
		data := Evidence{Transaction: json.RawMessage(c.tx.Raw)}
		for _, l := range byHash[e.TxID] {
			data.Logs = append(data.Logs, json.RawMessage(l.Raw))
		}
		if trace, ok := traces[e.TxID]; ok {
			data.Trace = json.RawMessage(trace.Raw)
		}
		if c.receipt {
			r, ok := receipts[e.TxID]
			if !ok {
				r = missing[i]
			}
			if r.Get("blockHash").String() != blockHash || r.Get("transactionHash").String() != e.TxID {
				return 0, nil, errors.New("receipt transaction or block mismatch")
			}
			if c.creation {
				addAddress(&e.Addresses, s.n.Name, r.Get("contractAddress").String())
			}
			data.Receipt = json.RawMessage(r.Raw)
		}
		if c.decide && !anyWatched(match, e.Addresses) {
			continue
		}
		e.Data = object(data)
		out = append(out, e)
	}
	return h, out, nil
}

// bindReceipts accepts whole-block receipts only when each one names this
// block hash and the transaction at the same index.
func bindReceipts(rows gjson.Result, txs []gjson.Result, blockHash string, receipts map[string]gjson.Result, byHash map[string][]gjson.Result) error {
	if !rows.IsArray() || len(rows.Array()) != len(txs) {
		return errors.New("incomplete EVM receipts")
	}
	for i, r := range rows.Array() {
		id := txs[i].Get("hash").String()
		if r.Get("blockHash").String() != blockHash || r.Get("transactionHash").String() != id || !r.Get("logs").IsArray() {
			return errors.New("EVM receipt mismatch")
		}
		receipts[id] = r
		byHash[id] = r.Get("logs").Array()
	}
	return nil
}

func bindHashLogs(logs gjson.Result, blockHash string, byHash map[string][]gjson.Result) error {
	if !logs.IsArray() {
		return errors.New("missing EVM logs")
	}
	for _, l := range logs.Array() {
		if l.Get("removed").Bool() || l.Get("blockHash").String() != blockHash {
			return errors.New("EVM log block mismatch")
		}
		id := l.Get("transactionHash").String()
		byHash[id] = append(byHash[id], l)
	}
	return nil
}

// bindRangeLogs accepts logs requested by height only if they provably belong
// to the block that was read. Every log must name the block hash, and their
// bloom must equal the header's logsBloom. That equality also rejects an
// empty answer for a block whose bloom shows logs.
func bindRangeLogs(logs gjson.Result, block gjson.Result, byHash map[string][]gjson.Result) error {
	header, err := hexutil.Decode(block.Get("logsBloom").String())
	if err != nil || len(header) != bloomLength {
		return errors.New("missing EVM logs bloom")
	}
	if !logs.IsArray() {
		return errors.New("missing EVM logs")
	}
	blockHash := block.Get("hash").String()
	number := block.Get("number").String()
	rows := logs.Array()
	var bloom [bloomLength]byte
	hasher := crypto.NewKeccakState()
	for _, l := range rows {
		if l.Get("removed").Bool() || l.Get("blockHash").String() != blockHash || l.Get("blockNumber").String() != number {
			return errors.New("EVM log block mismatch")
		}
		if !addLogBloom(&bloom, hasher, l) {
			return errors.New("malformed EVM log")
		}
	}
	if !bytes.Equal(bloom[:], header) {
		return errors.New("EVM logs do not match the block bloom")
	}
	for _, l := range rows {
		id := l.Get("transactionHash").String()
		byHash[id] = append(byHash[id], l)
	}
	return nil
}

const bloomLength = 256

func addLogBloom(bloom *[bloomLength]byte, hasher crypto.KeccakState, l gjson.Result) bool {
	var buf [32]byte
	if !decodeHex(buf[:20], l.Get("address").String()) {
		return false
	}
	addBloom(bloom, hasher, buf[:20])
	ok := true
	l.Get("topics").ForEach(func(_, t gjson.Result) bool {
		ok = decodeHex(buf[:], t.String())
		if ok {
			addBloom(bloom, hasher, buf[:])
		}
		return ok
	})
	return ok
}

// decodeHex fills dst from a 0x-prefixed hex string of exactly its length.
func decodeHex(dst []byte, s string) bool {
	if len(s) != 2+2*len(dst) || s[0] != '0' || (s[1] != 'x' && s[1] != 'X') {
		return false
	}
	for i := range dst {
		hi, ok1 := fromHexChar(s[2+2*i])
		lo, ok2 := fromHexChar(s[3+2*i])
		if !ok1 || !ok2 {
			return false
		}
		dst[i] = hi<<4 | lo
	}
	return true
}

func fromHexChar(c byte) (byte, bool) {
	switch {
	case '0' <= c && c <= '9':
		return c - '0', true
	case 'a' <= c && c <= 'f':
		return c - 'a' + 10, true
	case 'A' <= c && c <= 'F':
		return c - 'A' + 10, true
	}
	return 0, false
}

// addBloom sets the three bits of the Ethereum log bloom (yellow paper M3:2048).
func addBloom(bloom *[bloomLength]byte, hasher crypto.KeccakState, data []byte) {
	var h [32]byte
	hasher.Reset()
	hasher.Write(data)
	hasher.Read(h[:])
	for i := 0; i < 6; i += 2 {
		bit := (uint(h[i])<<8 | uint(h[i+1])) & 2047
		bloom[bloomLength-1-bit/8] |= 1 << (bit % 8)
	}
}

// createdAddress derives the CREATE address of a top-level contract creation.
// Only ordinary signed transactions are derived; system and deposit types fall
// back to their receipt.
func createdAddress(tx gjson.Result) (string, bool) {
	switch tx.Get("type").String() {
	case "", "0x0", "0x1", "0x2":
	default:
		return "", false
	}
	from := tx.Get("from").String()
	nonce, err := hexutil.DecodeUint64(tx.Get("nonce").String())
	if !common.IsHexAddress(from) || err != nil {
		return "", false
	}
	return strings.ToLower(crypto.CreateAddress(common.HexToAddress(from), nonce).Hex()), true
}

func traceAddresses(a *[]string, n string, t gjson.Result) {
	addAddress(a, n, t.Get("from").String())
	addAddress(a, n, t.Get("to").String())
	for _, child := range t.Get("calls").Array() {
		traceAddresses(a, n, child)
	}
}

// Fetch a bounded window concurrently, then hand it to SQLite in ledger order.
// A partial window is retried from the durable cursor.
func (s *jsonSource) evmRange(ctx context.Context, start, head int64) (int64, []Activity, error) {
	end := min(head, start+3)
	if end == start {
		return s.ReadLinkedBlock(ctx, start, nil)
	}
	group, ctx := errgroup.WithContext(ctx)
	blocks := make([][]Activity, end-start+1)
	for h := start; h <= end; h++ {
		group.Go(func() error { _, events, err := s.ReadLinkedBlock(ctx, h, nil); blocks[h-start] = events; return err })
	}
	if err := group.Wait(); err != nil {
		return 0, nil, err
	}
	var out []Activity
	for _, events := range blocks {
		out = append(out, events...)
	}
	return end, out, nil
}
