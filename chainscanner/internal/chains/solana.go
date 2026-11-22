package chains

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"

	"github.com/ethereum/go-ethereum/rpc"
	"github.com/tidwall/gjson"
	"golang.org/x/sync/errgroup"
)

// Only finalized slot membership is cached. Block payloads and delivery boundaries
// remain per requested span, even when a neighboring worker is delayed.
type slotWindow struct {
	start, end int64
	produced   [16]bool
}

func (s *jsonSource) finalizedSlots(ctx context.Context, start, end int64) (*slotWindow, error) {
	if w := s.slots.Load(); w != nil && w.start <= start && end <= w.end {
		return w, nil
	}
	w := &slotWindow{start: start, end: start + min(int64(15), max(end, s.finalizedSlot.Load())-start)}
	slots, err := s.call(ctx, "getBlocks", start, w.end, map[string]string{"commitment": "finalized"})
	if err != nil {
		return nil, err
	}
	if !slots.IsArray() {
		return nil, errors.New("missing finalized Solana slots")
	}
	prev := start - 1
	for _, v := range slots.Array() {
		h, err := strconv.ParseInt(v.Raw, 10, 64)
		if err != nil || h <= prev || h > w.end {
			return nil, errors.New("invalid finalized Solana slot list")
		}
		w.produced[h-start] = true
		prev = h
	}
	if !w.produced[0] {
		first, err := s.call(ctx, "getFirstAvailableBlock")
		if err != nil {
			return nil, err
		}
		earliest, err := strconv.ParseInt(first.Raw, 10, 64)
		if err != nil || earliest < 0 || earliest > start {
			return nil, errors.New("Solana history pruned or unavailable before saved cursor")
		}
	}
	s.slots.Store(w)
	return w, nil
}
func (s *jsonSource) solana(ctx context.Context, start, head int64) (int64, []Activity, error) {
	end := min(start+15, head)
	window, err := s.finalizedSlots(ctx, start, end)
	if err != nil {
		return 0, nil, err
	}
	read := func(ctx context.Context, first, last int64) (int64, []Activity, error) {
		var out []Activity
		for h := first; h <= last; h++ {
			if !window.produced[h-window.start] {
				continue
			}
			events, err := s.solanaBlock(ctx, h)
			if err != nil {
				return 0, nil, err
			}
			out = append(out, events...)
		}
		return last, out, nil
	}
	if end == start+1 && s.catchup.enabled() && window.produced[start-window.start] && window.produced[end-window.start] {
		return s.catchup.read(ctx, start, end, end, read)
	}
	return read(ctx, start, end)
}

// Above this many watched transactions in one slot, one full block costs fewer
// round trips than fetching each transaction.
const solanaTransactionReads = 8

func (s *jsonSource) getSolanaBlock(ctx context.Context, h int64, details string) (gjson.Result, error) {
	b, err := s.call(ctx, "getBlock", h, map[string]any{"commitment": "finalized", "encoding": "jsonParsed", "transactionDetails": details, "rewards": false, "maxSupportedTransactionVersion": 1})
	if err != nil {
		return b, err
	}
	if b.Get("blockhash").String() == "" || !b.Get("transactions").IsArray() {
		return b, errors.New("incomplete Solana block")
	}
	return b, nil
}

// solanaBlock reads a finalized slot. With a Watched filter it first lists each
// transaction's accounts and token owners. Only matching transactions are then
// fetched in full, so their Activity.Data is the same jsonParsed transaction
// object a full block read returns.
func (s *jsonSource) solanaBlock(ctx context.Context, h int64) ([]Activity, error) {
	match := WatchedAddresses(ctx)
	if match == nil {
		b, err := s.getSolanaBlock(ctx, h, "full")
		if err != nil {
			return nil, err
		}
		return decodeSolanaBlock(b, h, nil)
	}
	listed, err := s.getSolanaBlock(ctx, h, "accounts")
	if rejected := (rpc.Error)(nil); errors.As(err, &rejected) && rejected.ErrorCode() == -32602 {
		// A provider without the accounts detail level still serves full blocks.
		b, err := s.getSolanaBlock(ctx, h, "full")
		if err != nil {
			return nil, err
		}
		return decodeSolanaBlock(b, h, match)
	}
	if err != nil {
		return nil, err
	}
	stamp := listed.Get("blockTime").Int() * 1000
	var wanted []string
	cache := make(addressCache)
	for _, tx := range listed.Get("transactions").Array() {
		id := tx.Get("transaction.signatures.0").String()
		if id == "" {
			return nil, errors.New("missing Solana signature")
		}
		if anyWatched(match, solanaAddresses(cache, tx, "transaction.accountKeys")) {
			wanted = append(wanted, id)
		}
	}
	if len(wanted) == 0 {
		return nil, nil
	}
	if len(wanted) > solanaTransactionReads {
		b, err := s.getSolanaBlock(ctx, h, "full")
		if err != nil {
			return nil, err
		}
		if b.Get("blockhash").String() != listed.Get("blockhash").String() {
			return nil, errors.New("Solana block changed between reads")
		}
		return decodeSolanaBlock(b, h, match)
	}
	full := make([]gjson.Result, len(wanted))
	group, fetch := errgroup.WithContext(ctx)
	group.SetLimit(solanaTransactionReads)
	for i, id := range wanted {
		group.Go(func() error {
			tx, err := s.call(fetch, "getTransaction", id, map[string]any{"commitment": "finalized", "encoding": "jsonParsed", "maxSupportedTransactionVersion": 1})
			if err != nil {
				return err
			}
			if tx.Get("slot").Int() != h || tx.Get("transaction.signatures.0").String() != id || !tx.Get("meta").IsObject() {
				return errors.New("Solana transaction does not match its block")
			}
			full[i] = tx
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	out := make([]Activity, 0, len(full))
	for i, tx := range full {
		// getTransaction adds slot and blockTime around the block's object.
		data := []byte(`{"meta":` + tx.Get("meta").Raw + `,"transaction":` + tx.Get("transaction").Raw)
		if v := tx.Get("version"); v.Exists() {
			data = append(data, `,"version":`+v.Raw...)
		}
		data = append(data, '}')
		e := Activity{Height: h, TxID: wanted[i], TimeMS: stamp, Data: data}
		e.Addresses = solanaAddresses(cache, gjson.ParseBytes(data), "transaction.message.accountKeys")
		out = append(out, e)
	}
	return out, nil
}

func decodeSolanaBlock(b gjson.Result, h int64, match Watched) ([]Activity, error) {
	stamp := b.Get("blockTime").Int() * 1000
	txs := b.Get("transactions").Array()
	out := make([]Activity, 0, len(txs))
	cache := make(addressCache)
	for _, tx := range txs {
		id := tx.Get("transaction.signatures.0").String()
		if id == "" {
			return nil, errors.New("missing Solana signature")
		}
		e := Activity{Height: h, TxID: id, TimeMS: stamp, Data: json.RawMessage(tx.Raw)}
		e.Addresses = solanaAddresses(cache, tx, "transaction.message.accountKeys")
		if match != nil && !anyWatched(match, e.Addresses) {
			continue
		}
		out = append(out, e)
	}
	return out, nil
}

// solanaAddresses lists account keys, then SPL token owners. keys is the
// account list path of the transaction detail level that was read.
func solanaAddresses(cache addressCache, tx gjson.Result, keys string) []string {
	var out []string
	for _, k := range tx.Get(keys).Array() {
		cache.add(&out, "solana", k.Get("pubkey").String())
	}
	for _, p := range []string{"meta.preTokenBalances", "meta.postTokenBalances"} {
		for _, bal := range tx.Get(p).Array() {
			cache.add(&out, "solana", bal.Get("owner").String())
		}
	}
	return out
}
