package data

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/mdb"
)

func reserveSlot(t *testing.T, network, tradeID string, addresses []string, amounts ...float64) (string, float64) {
	t.Helper()
	address, amount, err := ReserveTransactionSlot(network, "usdt", tradeID, addresses, amounts, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return address, amount
}

func TestReserveTransactionSlotKeepsCandidateOrder(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	if err := ReloadSettings(); err != nil {
		t.Fatal(err)
	}
	wallets := []string{"wallet-a", "wallet-b", "wallet-a", "wallet-c"}
	amounts := []float64{10, 10.01, 10.02}
	type slot struct {
		address string
		amount  float64
	}
	var got []slot
	for i := range 9 {
		address, amount := reserveSlot(t, mdb.NetworkTron, fmt.Sprintf("order-%d", i), wallets, amounts...)
		got = append(got, slot{address, amount})
	}
	want := []slot{
		{"wallet-a", 10}, {"wallet-b", 10}, {"wallet-c", 10},
		{"wallet-a", 10.01}, {"wallet-b", 10.01}, {"wallet-c", 10.01},
		{"wallet-a", 10.02}, {"wallet-b", 10.02}, {"wallet-c", 10.02},
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("slot %d = %v, want %v (all: %v)", i, got[i], want[i], got)
		}
	}
	if address, _ := reserveSlot(t, mdb.NetworkTron, "order-full", wallets, amounts...); address != "" {
		t.Fatalf("exhausted candidates reserved %q", address)
	}
	// Another token, network or trade has its own slots.
	if address, _, err := ReserveTransactionSlot(mdb.NetworkTron, "TRX", "order-trx", wallets, amounts, time.Hour); err != nil || address != "wallet-a" {
		t.Fatal(address, err)
	}
	// Reserving again for a trade replaces its previous lock.
	if address, amount := reserveSlot(t, mdb.NetworkTron, "order-0", []string{"wallet-a"}, 10, 10.01, 10.02, 10.03); address != "wallet-a" || amount != 10 {
		t.Fatal(address, amount)
	}
	var owned int64
	if err := dao.RuntimeDB.Model(&mdb.TransactionLock{}).Where("trade_id = ?", "order-0").Count(&owned).Error; err != nil || owned != 1 {
		t.Fatal(owned, err)
	}
}

func TestReserveTransactionSlotHonorsExistingLocks(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	if err := ReloadSettings(); err != nil {
		t.Fatal(err)
	}
	// A lock stored at another precision still blocks the same value.
	if err := SetSetting(mdb.SettingGroupSystem, mdb.SettingKeyAmountPrecision, "4", mdb.SettingTypeInt); err != nil {
		t.Fatal(err)
	}
	if err := LockTransaction(mdb.NetworkTron, "precision-wallet", "USDT", "four-places", 1.2, time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := SetSetting(mdb.SettingGroupSystem, mdb.SettingKeyAmountPrecision, "2", mdb.SettingTypeInt); err != nil {
		t.Fatal(err)
	}
	if address, amount := reserveSlot(t, mdb.NetworkTron, "two-places", []string{"precision-wallet"}, 1.2, 1.21); address != "precision-wallet" || amount != 1.21 {
		t.Fatal(address, amount)
	}

	// Legacy mixed-case EVM rows match case-insensitively.
	legacy := mdb.TransactionLock{Network: mdb.NetworkEthereum, Address: "0xABCDEFabcdefABCDEFabcdefABCDEFabcdefABCD", Token: "USDT", AmountScaled: 500, AmountText: "5.00", AmountPrecision: 2, TradeId: "legacy", ExpiresAt: time.Now().Add(time.Hour)}
	if err := dao.RuntimeDB.Create(&legacy).Error; err != nil {
		t.Fatal(err)
	}
	evm := "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
	if address, amount := reserveSlot(t, mdb.NetworkEthereum, "evm", []string{evm}, 5, 5.01); address != evm || amount != 5.01 {
		t.Fatal(address, amount)
	}
	var stored mdb.TransactionLock
	if err := dao.RuntimeDB.Where("trade_id = ?", "evm").Take(&stored).Error; err != nil || stored.Address != evm {
		t.Fatal(stored, err)
	}

	// An expired lock is removed and its target reused.
	if err := LockTransaction(mdb.NetworkTron, "expired-wallet", "USDT", "expired", 3, -time.Second); err != nil {
		t.Fatal(err)
	}
	if address, amount := reserveSlot(t, mdb.NetworkTron, "reuse", []string{"expired-wallet"}, 3); address != "expired-wallet" || amount != 3 {
		t.Fatal(address, amount)
	}
	owner, err := GetTradeIdByWalletAddressAndAmountAndToken(mdb.NetworkTron, "expired-wallet", "USDT", 3)
	if err != nil || owner != "reuse" {
		t.Fatal(owner, err)
	}
	// LockTransaction sees reservations made here.
	if err := LockTransaction(mdb.NetworkTron, "expired-wallet", "USDT", "late", 3, time.Hour); err != ErrTransactionLocked {
		t.Fatal(err)
	}
}

func TestReserveTransactionSlotConcurrent(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	if err := ReloadSettings(); err != nil {
		t.Fatal(err)
	}
	wallets := []string{"w1", "w2", "w3"}
	amounts := make([]float64, 20)
	for i := range amounts {
		amounts[i] = 7 + float64(i)/100
	}
	const orders = 70 // more than the 60 candidates
	type result struct {
		address string
		amount  float64
	}
	results := make(chan result, orders)
	var wg sync.WaitGroup
	for i := range orders {
		wg.Go(func() {
			address, amount, err := ReserveTransactionSlot(mdb.NetworkTron, "USDT", fmt.Sprintf("concurrent-%d", i), wallets, amounts, time.Hour)
			if err != nil {
				t.Error(err)
				return
			}
			results <- result{address, amount}
		})
	}
	wg.Wait()
	close(results)
	seen := map[result]bool{}
	empty := 0
	for r := range results {
		if r.address == "" {
			empty++
			continue
		}
		if seen[r] {
			t.Fatalf("duplicate slot %v", r)
		}
		seen[r] = true
	}
	if len(seen) != 60 || empty != orders-60 {
		t.Fatalf("reserved %d slots, %d empty", len(seen), empty)
	}
}
