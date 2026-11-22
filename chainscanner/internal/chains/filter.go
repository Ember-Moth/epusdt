package chains

import "context"

// Watched reports whether a canonical address may concern a subscriber. A
// block reader given one skips unrelated transactions before it assembles
// their evidence or fetches per-transaction data. It is a superset test: the
// store still matches every delivered activity exactly by address and height.
type Watched func(address string) bool

type watchedKey struct{}

// WithWatched makes reads under ctx consult snapshot once per block. snapshot
// returns nil when it cannot vouch for every subscriber of the blocks being
// read, and the reader then keeps every transaction.
func WithWatched(ctx context.Context, snapshot func() Watched) context.Context {
	return context.WithValue(ctx, watchedKey{}, snapshot)
}

// WatchedAddresses returns the filter for one block read, or nil to read all.
func WatchedAddresses(ctx context.Context) Watched {
	if snapshot, ok := ctx.Value(watchedKey{}).(func() Watched); ok && snapshot != nil {
		return snapshot()
	}
	return nil
}

func anyWatched(match Watched, addresses []string) bool {
	for _, a := range addresses {
		if match(a) {
			return true
		}
	}
	return false
}
