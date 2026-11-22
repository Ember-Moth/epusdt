package chains

import (
	"context"
	"errors"

	"golang.org/x/sync/errgroup"
)

// Span is an inclusive range of block heights (ledger versions on Aptos).
type Span struct {
	First, Last int64
	Failed      bool // a previous scan attempt for this range failed
}

type FinishRange = func(Span, BlockLink, []Activity, error) error

// ErrFork marks a provider response incompatible with an already accepted neighbor.
var ErrFork = errors.New("block ancestry conflicts with a completed neighbor")

// MaxLinkedSpan bounds the EVM blocks one read requests at once.
const MaxLinkedSpan = 4

type linkedReader interface {
	ReadLinkedBlock(context.Context, int64, *BlockLink) (int64, []Activity, error)
}

// ReadSpan returns one complete adapter response, including the exact boundary
// of a partial Aptos page. A linked EVM source must identify both neighbors.
func ReadSpan(ctx context.Context, network string, s Source, r Span) (Span, BlockLink, []Activity, error) {
	var link BlockLink
	var rows []Activity
	var end int64
	var err error
	if reader, ok := s.(linkedReader); ok && IsEVM(network) {
		// A longer request is answered in part, like a short Aptos page.
		end = min(r.Last, r.First+MaxLinkedSpan-1)
		link, rows, err = readLinked(ctx, reader, Span{First: r.First, Last: end})
	} else {
		end, rows, err = s.Read(ctx, r.First, r.Last)
	}
	if err != nil {
		return r, link, nil, err
	}
	if end < r.First || end > r.Last {
		return r, link, nil, errors.New("invalid node range")
	}
	r.Last = end
	for _, e := range rows {
		if e.Height < r.First || e.Height > end || (e.Network != "" && e.Network != network) {
			return r, link, nil, errors.New("event outside assigned network or range")
		}
	}
	return r, link, rows, nil
}

// readLinked reads every block of an EVM span concurrently and proves that
// consecutive blocks are parent and child. The span then carries the same
// boundary evidence as a single block: the last hash and the first parent.
// Any failure fails the whole span, which the store bisects.
func readLinked(ctx context.Context, reader linkedReader, r Span) (BlockLink, []Activity, error) {
	count := r.Last - r.First + 1
	if count < 1 {
		return BlockLink{}, nil, errors.New("invalid EVM span")
	}
	links := make([]BlockLink, count)
	parts := make([][]Activity, count)
	group, call := errgroup.WithContext(ctx)
	for i := range count {
		group.Go(func() error {
			h := r.First + i
			end, rows, err := reader.ReadLinkedBlock(call, h, &links[i])
			if err == nil && (links[i].Hash == "" || (h > 0 && links[i].Parent == "") || end != h) {
				err = errors.New("invalid EVM block link")
			}
			parts[i] = rows
			return err
		})
	}
	if err := group.Wait(); err != nil {
		return BlockLink{}, nil, err
	}
	var rows []Activity
	for i := range count {
		if i > 0 && links[i].Parent != links[i-1].Hash {
			return BlockLink{}, nil, errors.New("inconsistent block ancestry")
		}
		rows = append(rows, parts[i]...)
	}
	return BlockLink{Hash: links[count-1].Hash, Parent: links[0].Parent}, rows, nil
}
