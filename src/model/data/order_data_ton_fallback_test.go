package data

import (
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/mdb"
	addressutil "github.com/GMWalletApp/epusdt/util/address"
	"github.com/xssnick/tonutils-go/address"
)

// TON orders store receive_address in the user-friendly form produced by
// addressutil.NormalizeTonAddress, while transaction locks key TON by the raw
// workchain:hash form. The lock-released fallback must query orders in the
// order column's form, whatever spelling the caller passes.
func TestOnChainOrderFallbackMatchesTonOrderInAnySpelling(t *testing.T) {
	cleanup := testutil.SetupTestDatabases(t)
	defer cleanup()

	bounceable := "EQC6KV4zs8TJtSZapOrRFmqSkxzpq-oSCoxekQRKElf4nC1I"
	addr := address.MustParseAddr(bounceable)
	stored, err := addressutil.NormalizeTonAddress(bounceable)
	if err != nil {
		t.Fatal(err)
	}

	order := mdb.Orders{
		TradeId:        "trade-ton-fallback",
		OrderId:        "order-ton-fallback",
		Network:        mdb.NetworkTon,
		Token:          "USDT",
		Amount:         1.23,
		ActualAmount:   1.23,
		ReceiveAddress: stored,
		Status:         mdb.StatusExpired,
		PayProvider:    mdb.PaymentProviderOnChain,
	}
	if err := dao.Mdb.Create(&order).Error; err != nil {
		t.Fatal(err)
	}

	for _, spelling := range []string{
		stored,           // non-bounceable user-friendly (stored form)
		bounceable,       // bounceable user-friendly
		addr.StringRaw(), // raw workchain:hash (lock form)
		" " + addr.StringRaw() + " ",
	} {
		got, err := GetOnChainOrderByWalletAddressAndAmountAndTokenBeforeTime("ton", spelling, "usdt", 1.23, time.Now().Add(time.Minute))
		if err != nil {
			t.Fatalf("fallback lookup %q: %v", spelling, err)
		}
		if got.TradeId != order.TradeId {
			t.Fatalf("fallback lookup %q returned trade %q, want %q", spelling, got.TradeId, order.TradeId)
		}
	}

	other := address.NewAddress(0, 0, make([]byte, 32)).StringRaw()
	got, err := GetOnChainOrderByWalletAddressAndAmountAndTokenBeforeTime(mdb.NetworkTon, other, "USDT", 1.23, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("fallback lookup other address: %v", err)
	}
	if got.ID != 0 {
		t.Fatalf("fallback lookup for another address matched trade %q", got.TradeId)
	}
}
