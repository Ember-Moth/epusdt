package task

import (
	"context"
	"errors"
	"sync"
	"time"

	scanner "github.com/GMWalletApp/ChainScanner"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/GMWalletApp/epusdt/model/service"
	"github.com/GMWalletApp/epusdt/util/log"
)

const paymentConsumer = "epusdt"

var scannerNetworks = []string{mdb.NetworkEthereum, mdb.NetworkBsc, mdb.NetworkPolygon, mdb.NetworkPlasma, mdb.NetworkTron, mdb.NetworkTon, mdb.NetworkAptos, mdb.NetworkSolana, mdb.NetworkSui, mdb.NetworkArbitrum, mdb.NetworkHyperEVM, mdb.NetworkMonad, mdb.NetworkArc, mdb.NetworkAvalanche, mdb.NetworkOptimism, mdb.NetworkBase}

// paymentSubscriptions keeps ChainScanner's watch set for this consumer.
// refresh replaces the complete set and owns removal and reconciliation;
// ensure only adds one address. refresh holds the write lock from reading the
// wallet list until its replacement commits, so it cannot remove an address
// whose ensure finished after that read.
type paymentSubscriptions struct {
	mu   sync.RWMutex
	feed scanner.Feed
}

func (p *paymentSubscriptions) refresh(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	wallets, err := data.GetAvailableWalletAddress()
	if err != nil {
		return err
	}
	chains, err := data.ListEnabledChains()
	if err != nil {
		return err
	}
	enabled := map[string]bool{}
	for _, c := range chains {
		enabled[c.Network] = true
	}
	watches := make([]scanner.Watch, 0, len(wallets))

	for _, w := range wallets {
		if w.Network == mdb.NetworkXLayer || !enabled[w.Network] {
			continue
		}
		a, e := scanner.Canonical(w.Network, w.Address)
		if e != nil {
			log.Sugar.Warnf("[ChainScanner] invalid wallet row %d on %s", w.ID, w.Network)
			continue
		}
		watches = append(watches, scanner.Watch{Network: w.Network, Address: a})
	}
	_, err = p.feed.Subscribe(ctx, paymentConsumer, watches)
	return err
}

// ensure returns once ChainScanner confirms it watches the address. It asks
// every time because a remote scanner may have restarted without its state.
func (p *paymentSubscriptions) ensure(network, address string) error {
	a, err := scanner.Canonical(network, address)
	if err != nil {
		return err
	}
	w := scanner.Watch{Network: network, Address: a}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p.mu.RLock()
	rows, err := p.feed.Register(ctx, paymentConsumer, []scanner.Watch{w})
	p.mu.RUnlock()
	if err != nil {
		return err
	}
	for _, r := range rows {
		if r.Watch == w {
			return nil
		}
	}
	return errors.New("payment address is not registered with ChainScanner")
}

func consumeQueue(ctx context.Context, feed scanner.Feed, next func() (*scanner.Delivery, error)) {
	for ctx.Err() == nil {
		d, err := next()
		if err == nil && d != nil {
			err = processScannerActivity(ctx, d.Activity)
			if err == nil {
				err = feed.Ack(ctx, paymentConsumer, d.ID)
				if err == nil {
					continue
				}
			}
		}
		if err != nil {
			log.Sugar.Warnf("[ChainScanner] delivery pending: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func processScannerActivity(ctx context.Context, e scanner.Activity) error {
	chain, err := data.GetChainByNetwork(e.Network)
	if err != nil {
		return err
	}
	if chain.ID == 0 || !chain.Enabled {
		return nil
	}
	err = service.ApplyChainActivity(ctx, e)
	if err == nil {
		data.RecordRpcBlockHeight(e.Network, e.Height)
	}
	return err
}
