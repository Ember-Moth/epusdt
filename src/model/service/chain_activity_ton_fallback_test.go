package service

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	scanner "github.com/GMWalletApp/ChainScanner"
	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// A TON transfer that is processed after its order's transaction lock was
// released (order expired, scanner lagging or backfilling) must still reach
// the order through the receive-address fallback query.
func TestTONActivityMatchesOrderAfterLockReleased(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	const network = mdb.NetworkTon

	chain := mdb.Chain{Network: network, DisplayName: network, Enabled: true, MinConfirmations: 1}
	if err := dao.Mdb.Where("network = ?", network).Assign(map[string]any{"enabled": true}).FirstOrCreate(&chain).Error; err != nil {
		t.Fatal(err)
	}
	token := mdb.ChainToken{Network: network, Symbol: "TON", Decimals: 9, Enabled: true}
	if err := dao.Mdb.Where("network = ? AND symbol = ?", network, token.Symbol).Assign(token).FirstOrCreate(&token).Error; err != nil {
		t.Fatal(err)
	}

	owner := address.MustParseAddr("EQC6KV4zs8TJtSZapOrRFmqSkxzpq-oSCoxekQRKElf4nC1I")
	recipient := owner.StringRaw()
	timeMS := time.Now().Add(2 * time.Second).Truncate(time.Second).UnixMilli()

	tx := tonTestInboundTx(t, address.NewAddress(0, 0, bytes.Repeat([]byte{2}, 32)), owner, tlb.MustFromTON("1"), cell.BeginCell().EndCell())
	tx.Now = uint32(timeMS / 1000)
	tx.AccountAddr = owner.Data()
	tx.PrevTxHash = make([]byte, 32)
	tx.StateUpdate = tlb.HashUpdate{OldHash: make([]byte, 32), NewHash: make([]byte, 32)}
	tx.OrigStatus = tlb.AccountStatusActive
	tx.EndStatus = tlb.AccountStatusActive
	tx.Description = tlb.TransactionDescriptionOrdinary{ComputePhase: tlb.ComputePhase{Phase: tlb.ComputePhaseSkipped{Reason: tlb.ComputeSkipReason{Type: tlb.ComputeSkipReasonNoGas}}}}
	c, err := tlb.ToCell(tx)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(scanner.TONData{Account: recipient, BOC: c.ToBOC()})
	if err != nil {
		t.Fatal(err)
	}
	e := scanner.Activity{
		Network:       network,
		TxID:          hex.EncodeToString(c.Hash()),
		Height:        10,
		Confirmations: 10000,
		TimeMS:        timeMS,
		Addresses:     []string{recipient},
		Data:          payload,
	}

	// Same write path as order creation: the stored receive address is the
	// user-friendly form, and the lock is keyed by the raw form.
	order := mdb.Orders{
		TradeId:        "trade-ton-late",
		OrderId:        "order-ton-late",
		Network:        network,
		Token:          token.Symbol,
		Amount:         1,
		ActualAmount:   1,
		ReceiveAddress: normalizeOrderAddressByNetwork(network, recipient),
		Status:         mdb.StatusWaitPay,
		PayProvider:    mdb.PaymentProviderOnChain,
	}
	if order.ReceiveAddress == recipient {
		t.Fatalf("expected stored TON receive address to differ from raw %q", recipient)
	}
	if err := dao.Mdb.Create(&order).Error; err != nil {
		t.Fatal(err)
	}
	if err := data.LockTransaction(network, order.ReceiveAddress, order.Token, order.TradeId, order.ActualAmount, time.Minute); err != nil {
		t.Fatal(err)
	}

	// The order expires and its lock is released before the scanner gets to
	// the transfer.
	if err := dao.Mdb.Model(&order).Update("status", mdb.StatusExpired).Error; err != nil {
		t.Fatal(err)
	}
	if err := data.UnLockTransactionByTradeId(order.TradeId); err != nil {
		t.Fatal(err)
	}
	if tradeID, err := data.GetTradeIdByWalletAddressAndAmountAndToken(network, recipient, token.Symbol, 1); err != nil || tradeID != "" {
		t.Fatalf("lock still present: trade %q, err %v", tradeID, err)
	}

	if err := ApplyChainActivity(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	got, err := data.GetOrderInfoByTradeId(order.TradeId)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != mdb.StatusConfirming {
		t.Fatalf("order status = %d, want %d (confirming via fallback)", got.Status, mdb.StatusConfirming)
	}
	var jobs int64
	if err := dao.Mdb.Model(&mdb.PaymentConfirmation{}).Where("order_id = ?", order.ID).Count(&jobs).Error; err != nil {
		t.Fatal(err)
	}
	if jobs != 1 {
		t.Fatalf("payment confirmation jobs = %d, want 1", jobs)
	}
}
