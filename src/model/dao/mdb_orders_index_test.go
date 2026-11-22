package dao

import (
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/dromara/carbon/v2"
)

func TestEnsureOrderIndexesIsIdempotent(t *testing.T) {
	db := setupSeedTableTestDB(t, &mdb.Orders{})
	for i := 0; i < 2; i++ {
		if err := EnsureOrderIndexes(db); err != nil {
			t.Fatalf("run %d: %v", i+1, err)
		}
	}
	for _, idx := range orderIndexes {
		if !db.Migrator().HasIndex(&mdb.Orders{}, idx.name) {
			t.Fatalf("index %s missing", idx.name)
		}
	}
}

func TestLowercaseEVMOrderReceiveAddresses(t *testing.T) {
	db := setupSeedTableTestDB(t, &mdb.Orders{}, &mdb.Chain{})
	Mdb = db
	seedChains()

	updated := carbon.CreateFromStdTime(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))
	orders := []mdb.Orders{
		{TradeId: "evm", OrderId: "evm", Network: mdb.NetworkEthereum, ReceiveAddress: "0xAbCdEf0000000000000000000000000000000001"},
		{TradeId: "bsc", OrderId: "bsc", Network: mdb.NetworkBsc, ReceiveAddress: "0xABCDEF0000000000000000000000000000000002"},
		{TradeId: "tron", OrderId: "tron", Network: mdb.NetworkTron, ReceiveAddress: "TMixedCaseBase58Address"},
		{TradeId: "sol", OrderId: "sol", Network: mdb.NetworkSolana, ReceiveAddress: "So1MixedCase"},
		{TradeId: "okpay", OrderId: "okpay", Network: mdb.PaymentProviderOkPay, ReceiveAddress: "OKPAY"},
	}
	for i := range orders {
		orders[i].UpdatedAt = *carbon.NewTime(updated)
		if err := db.Create(&orders[i]).Error; err != nil {
			t.Fatalf("create %s: %v", orders[i].TradeId, err)
		}
	}
	// Soft-deleted rows are rewritten too, so restoring one stays consistent.
	if err := db.Delete(&orders[1]).Error; err != nil {
		t.Fatalf("soft delete: %v", err)
	}

	changed, err := LowercaseEVMOrderReceiveAddresses(db)
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if changed != 2 {
		t.Fatalf("changed = %d, want 2", changed)
	}
	want := map[string]string{
		"evm":   "0xabcdef0000000000000000000000000000000001",
		"bsc":   "0xabcdef0000000000000000000000000000000002",
		"tron":  "TMixedCaseBase58Address",
		"sol":   "So1MixedCase",
		"okpay": "OKPAY",
	}
	var rows []mdb.Orders
	if err := db.Unscoped().Find(&rows).Error; err != nil {
		t.Fatalf("load: %v", err)
	}
	for _, row := range rows {
		if row.ReceiveAddress != want[row.TradeId] {
			t.Fatalf("%s receive_address = %q, want %q", row.TradeId, row.ReceiveAddress, want[row.TradeId])
		}
		if !row.UpdatedAt.StdTime().Equal(updated.StdTime()) {
			t.Fatalf("%s updated_at changed to %s", row.TradeId, row.UpdatedAt)
		}
	}

	changed, err = LowercaseEVMOrderReceiveAddresses(db)
	if err != nil || changed != 0 {
		t.Fatalf("second run changed=%d err=%v, want no changes", changed, err)
	}
}
