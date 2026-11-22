package service

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/GMWalletApp/epusdt/model/request"
	"github.com/GMWalletApp/epusdt/util/constant"
)

func withPaymentWatch(t *testing.T, ensure func(network, address string) error) {
	t.Helper()
	old := EnsurePaymentWatch
	EnsurePaymentWatch = ensure
	t.Cleanup(func() { EnsurePaymentWatch = old })
}

func countLocks(t *testing.T) int64 {
	t.Helper()
	var n int64
	if err := dao.RuntimeDB.Model(&mdb.TransactionLock{}).Count(&n).Error; err != nil {
		t.Fatal(err)
	}
	return n
}

func TestConcurrentCreateTransactionTargetsAndDuplicateOrderIDs(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	if err := data.ReloadSettings(); err != nil {
		t.Fatal(err)
	}
	wallets := map[string]bool{}
	for i := range 3 {
		w, err := data.AddWalletAddress(fmt.Sprintf("parallel-wallet-%d", i))
		if err != nil {
			t.Fatal(err)
		}
		wallets[w.Address] = true
	}
	var ensured atomic.Int64
	withPaymentWatch(t, func(network, address string) error {
		if network != mdb.NetworkTron || !wallets[address] {
			t.Errorf("ensured %s %s", network, address)
		}
		ensured.Add(1)
		return nil
	})

	type attempt struct {
		orderID string
		select_ bool // a status=4 placeholder without token and network
	}
	var attempts []attempt
	const unique, duplicated, copies = 60, 8, 4
	for i := range unique {
		attempts = append(attempts, attempt{orderID: fmt.Sprintf("unique-%d", i)})
	}
	for i := range duplicated {
		for range copies {
			attempts = append(attempts, attempt{orderID: fmt.Sprintf("dup-%d", i)})
			attempts = append(attempts, attempt{orderID: fmt.Sprintf("dup-select-%d", i), select_: true})
		}
	}
	type outcome struct {
		attempt
		resp *mdb.Orders
		err  error
	}
	outcomes := make([]outcome, len(attempts))
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, a := range attempts {
		wg.Go(func() {
			req := newCreateTransactionRequest(a.orderID, 10)
			if a.select_ {
				req.Token, req.Network = "", ""
			}
			<-start
			resp, err := CreateTransaction(req, nil)
			outcomes[i] = outcome{attempt: a, err: err}
			if err == nil {
				outcomes[i].resp = &mdb.Orders{TradeId: resp.TradeId, OrderId: resp.OrderId, ReceiveAddress: resp.ReceiveAddress, ActualAmount: resp.ActualAmount, Token: resp.Token}
			}
		})
	}
	close(start)
	wg.Wait()

	wins := map[string]int{}
	targets := map[string]string{}
	var payable int64
	for _, o := range outcomes {
		if o.err != nil {
			if !errors.Is(o.err, constant.OrderAlreadyExists) {
				t.Fatalf("%s: %v", o.orderID, o.err)
			}
			continue
		}
		wins[o.orderID]++
		if o.select_ {
			continue
		}
		payable++
		target := fmt.Sprintf("%s:%s:%.2f", o.resp.ReceiveAddress, o.resp.Token, o.resp.ActualAmount)
		if other, ok := targets[target]; ok {
			t.Fatalf("orders %s and %s share %s", other, o.orderID, target)
		}
		targets[target] = o.orderID
		owner, err := data.GetTradeIdByWalletAddressAndAmountAndToken(mdb.NetworkTron, o.resp.ReceiveAddress, o.resp.Token, o.resp.ActualAmount)
		if err != nil || owner != o.resp.TradeId {
			t.Fatalf("%s: lock owner %q, err %v", o.orderID, owner, err)
		}
	}
	if len(wins) != unique+2*duplicated {
		t.Fatalf("%d order ids succeeded", len(wins))
	}
	for id, n := range wins {
		if n != 1 {
			t.Fatalf("%s created %d times", id, n)
		}
	}
	if payable != unique+duplicated {
		t.Fatalf("payable orders = %d", payable)
	}
	// Losing duplicates released their reservations; ensure ran once per reservation.
	if locks := countLocks(t); locks != payable {
		t.Fatalf("locks = %d, want %d", locks, payable)
	}
	if n := ensured.Load(); n < payable || n > int64(unique+duplicated*copies) {
		t.Fatalf("ensure calls = %d", n)
	}
	var rows int64
	if err := dao.Mdb.Model(&mdb.Orders{}).Count(&rows).Error; err != nil || rows != int64(unique+2*duplicated) {
		t.Fatalf("orders = %d, err %v", rows, err)
	}
}

func TestCreateTransactionEnsuresOnlyTheSelectedAddress(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	if err := data.ReloadSettings(); err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		if _, err := data.AddWalletAddress(fmt.Sprintf("ensure-wallet-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	var calls []string
	var fail error
	withPaymentWatch(t, func(network, address string) error {
		calls = append(calls, network+"/"+address)
		return fail
	})
	var last *mdb.Orders
	for i := range 4 {
		calls = nil
		resp, err := CreateTransaction(newCreateTransactionRequest(fmt.Sprintf("ensure-%d", i), 10), nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(calls) != 1 || calls[0] != mdb.NetworkTron+"/"+resp.ReceiveAddress {
			t.Fatalf("order %d ensured %v, selected %s", i, calls, resp.ReceiveAddress)
		}
		last = &mdb.Orders{ReceiveAddress: resp.ReceiveAddress, ActualAmount: resp.ActualAmount}
	}
	if last.ActualAmount != 10.01 {
		t.Fatalf("fourth order amount %.2f, want the first increment", last.ActualAmount)
	}

	// A failed watch releases the reservation and creates no order.
	fail = errors.New("scanner unavailable")
	locks := countLocks(t)
	if _, err := CreateTransaction(newCreateTransactionRequest("ensure-failed", 10), nil); !errors.Is(err, fail) {
		t.Fatalf("err = %v", err)
	}
	if countLocks(t) != locks {
		t.Fatal("failed watch leaked its reservation")
	}
	if exist, err := data.GetOrderInfoByOrderId("ensure-failed"); err != nil || exist.ID != 0 {
		t.Fatal("failed watch created an order")
	}
	fail = nil
	resp, err := CreateTransaction(newCreateTransactionRequest("ensure-failed", 10), nil)
	if err != nil || resp.ActualAmount != 10.01 || resp.ReceiveAddress == last.ReceiveAddress {
		t.Fatalf("retry got %+v, err %v", resp, err)
	}
}

func TestCreateTransactionMapsOrderIDRace(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	if err := data.ReloadSettings(); err != nil {
		t.Fatal(err)
	}
	if _, err := data.AddWalletAddress("race-wallet"); err != nil {
		t.Fatal(err)
	}
	// Another request commits the same merchant order id after this one checked it.
	withPaymentWatch(t, func(string, string) error {
		return dao.Mdb.Create(&mdb.Orders{TradeId: "winner", OrderId: "raced", Status: mdb.StatusWaitPay}).Error
	})
	if _, err := CreateTransaction(newCreateTransactionRequest("raced", 10), nil); !errors.Is(err, constant.OrderAlreadyExists) {
		t.Fatalf("err = %v", err)
	}
	if countLocks(t) != 0 {
		t.Fatal("lost race leaked its reservation")
	}
}

func TestConcurrentSwitchNetworkOnOneParent(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	if err := data.ReloadSettings(); err != nil {
		t.Fatal(err)
	}
	if _, err := data.AddWalletAddress("switch-tron-wallet"); err != nil {
		t.Fatal(err)
	}
	if _, err := data.AddWalletAddressWithNetwork(mdb.NetworkEthereum, "0x1111111111111111111111111111111111111111"); err != nil {
		t.Fatal(err)
	}
	parent, err := CreateTransaction(newCreateTransactionRequest("switch-parent", 1), nil)
	if err != nil {
		t.Fatal(err)
	}
	placeholder := newCreateTransactionRequest("switch-placeholder", 2)
	placeholder.Token, placeholder.Network = "", ""
	waiting, err := CreateTransaction(placeholder, nil)
	if err != nil {
		t.Fatal(err)
	}

	run := func(tradeID, network string) []string {
		var wg sync.WaitGroup
		ids := make([]string, 8)
		for i := range ids {
			wg.Go(func() {
				resp, err := SwitchNetwork(&request.SwitchNetworkRequest{TradeId: tradeID, Token: "USDT", Network: network})
				if err != nil {
					t.Error(err)
					return
				}
				ids[i] = resp.TradeId
			})
		}
		wg.Wait()
		return ids
	}
	ids := run(parent.TradeId, mdb.NetworkEthereum)
	for _, id := range ids {
		if id == "" || id != ids[0] || id == parent.TradeId {
			t.Fatalf("switches returned %v", ids)
		}
	}
	if n, err := data.CountSubOrders(parent.TradeId); err != nil || n != 1 {
		t.Fatalf("sub-orders = %d, err %v", n, err)
	}
	for _, id := range run(waiting.TradeId, mdb.NetworkTron) {
		if id != waiting.TradeId {
			t.Fatalf("placeholder completion returned %s", id)
		}
	}
	var placeholderLocks int64
	if err := dao.RuntimeDB.Model(&mdb.TransactionLock{}).Where("trade_id = ?", waiting.TradeId).Count(&placeholderLocks).Error; err != nil || placeholderLocks != 1 {
		t.Fatalf("placeholder locks = %d, err %v", placeholderLocks, err)
	}
	if countLocks(t) != 3 {
		t.Fatalf("locks = %d, want parent, child and placeholder", countLocks(t))
	}
	if len(gSwitchNetworkLocks.locks) != 0 {
		t.Fatal("idle switch locks were not released")
	}
}
