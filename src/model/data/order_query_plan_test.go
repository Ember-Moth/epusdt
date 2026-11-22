package data

import (
	"strings"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"gorm.io/gorm"
)

type capturedStatement struct {
	sql  string
	vars []any
}

// captureStatements records every SELECT that db runs while capturing is on.
func captureStatements(t *testing.T, db *gorm.DB) (start func(), stop func() []capturedStatement) {
	t.Helper()
	var (
		on       bool
		captured []capturedStatement
	)
	record := func(tx *gorm.DB) {
		if on {
			captured = append(captured, capturedStatement{tx.Statement.SQL.String(), append([]any(nil), tx.Statement.Vars...)})
		}
	}
	if err := db.Callback().Query().After("gorm:query").Register("test:capture_query", record); err != nil {
		t.Fatal(err)
	}
	if err := db.Callback().Row().After("gorm:row").Register("test:capture_row", record); err != nil {
		t.Fatal(err)
	}
	return func() { on, captured = true, nil }, func() []capturedStatement { on = false; return captured }
}

func queryPlan(t *testing.T, db *gorm.DB, st capturedStatement) string {
	t.Helper()
	rows, err := db.Raw("EXPLAIN QUERY PLAN "+st.sql, st.vars...).Rows()
	if err != nil {
		t.Fatalf("explain %s: %v", st.sql, err)
	}
	defer rows.Close()
	var steps []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		steps = append(steps, detail)
	}
	return strings.Join(steps, " | ")
}

// planCheck lists, for one statement, substrings its plan must and must
// not contain.
type planCheck struct {
	want    []string
	notWant []string
}

const noSort = "TEMP B-TREE FOR ORDER BY"

// TestOrderQueriesUseCompositeIndexes pins the plans the orders indexes were
// designed for. Like a production database, the test database has no
// statistics besides the idx_orders_deleted_at row EnsureOrderIndexes
// writes, so the planner uses fixed estimates and the plans do not depend
// on how many rows exist.
func TestOrderQueriesUseCompositeIndexes(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	if err := dao.EnsureOrderIndexes(dao.Mdb); err != nil {
		t.Fatalf("create indexes: %v", err)
	}
	start, stop := captureStatements(t, dao.Mdb)

	now := time.Now()
	monthAgo := now.AddDate(0, 0, -30)
	uses := func(plan ...string) planCheck { return planCheck{want: plan} }
	cases := []struct {
		name string
		run  func() error
		// One entry per SELECT the call runs, in order.
		checks []planCheck
	}{
		{"expire poll (mq worker)", func() error {
			// Same query as processExpiredOrders in mq/worker.go.
			var rows []mdb.Orders
			return dao.Mdb.Model(&mdb.Orders{}).
				Select("id", "trade_id", "status", "network", "receive_address", "token", "actual_amount", "pay_provider").
				Where("status IN ?", []int{mdb.StatusWaitPay, mdb.StatusWaitSelect}).
				Where("created_at <= ?", now).
				Order("id asc").Limit(100).Find(&rows).Error
		}, []planCheck{uses("USING INDEX idx_orders_status_created (status=? AND created_at<?)")}},
		{"callback poll", func() error {
			_, err := GetPendingCallbackOrders(5, 100)
			return err
		}, []planCheck{uses("USING INDEX idx_orders_callback_due (status=? AND callback_confirm=? AND callback_num<?)")}},
		{"on-chain match fallback", func() error {
			_, err := GetOnChainOrderByWalletAddressAndAmountAndTokenBeforeTime(mdb.NetworkEthereum, "0xAbCdEf0000000000000000000000000000000001", "usdt", 1.23, now)
			return err
		}, []planCheck{uses("USING INDEX idx_orders_amount_match (actual_amount=? AND network=? AND token=? AND receive_address=? AND created_at<?)")}},
		{"paid total", func() error {
			_, err := SumPaidActualAmount()
			return err
		}, []planCheck{uses("USING COVERING INDEX idx_orders_status_created (status=?)")}},
		{"daily stats", func() error {
			_, err := DailyOrderStats(monthAgo, now)
			return err
		}, []planCheck{uses("USING COVERING INDEX idx_orders_created_stats (created_at>? AND created_at<?)")}},
		{"paid stats in range", func() error {
			_, _, _, err := PaidStatsInRange(monthAgo, now)
			return err
		}, []planCheck{uses("USING COVERING INDEX idx_orders_created_stats (created_at>? AND created_at<?)")}},
		{"active addresses", func() error {
			_, err := ActiveAddressCountInRange(monthAgo, now)
			return err
		}, []planCheck{uses("USING COVERING INDEX idx_orders_created_stats (created_at>? AND created_at<?)")}},
		{"asset by address", func() error {
			_, err := DailyAssetByAddress(monthAgo, now)
			return err
		}, []planCheck{uses("USING INDEX idx_orders_status_created (status=? AND created_at>? AND created_at<?)")}},
		{"average payment time", func() error {
			_, err := AveragePaymentDurationSeconds(monthAgo, now)
			return err
		}, []planCheck{uses("USING INDEX idx_orders_status_created (status=? AND created_at>? AND created_at<?)")}},
		{"expired count", func() error {
			_, err := CountExpiredInRange(monthAgo, now)
			return err
		}, []planCheck{uses("USING COVERING INDEX idx_orders_status_created (status=? AND created_at>? AND created_at<?)")}},
		{"count by status", func() error {
			_, err := CountOrdersByStatus()
			return err
		}, []planCheck{uses("USING COVERING INDEX")}},
		{"recent orders", func() error {
			_, err := RecentOrders(20)
			return err
		}, []planCheck{{notWant: []string{noSort}}}},
		{"admin list, parents, by status", func() error {
			_, _, err := ListOrders(OrderListFilter{Status: mdb.StatusPaySuccess, ParentOnly: true})
			return err
		}, []planCheck{
			uses("USING COVERING INDEX idx_orders_parent_status (parent_trade_id=? AND status=? AND deleted_at=?)"),
			// Every index column is fixed, so rows come back in id order.
			{want: []string{"USING INDEX idx_orders_parent_status (parent_trade_id=? AND status=? AND deleted_at=?)"}, notWant: []string{noSort}},
		}},
		{"admin list, all orders, by status", func() error {
			_, _, err := ListOrders(OrderListFilter{Status: mdb.StatusExpired})
			return err
		}, []planCheck{
			uses("USING COVERING INDEX idx_orders_status_live (status=? AND deleted_at=?)"),
			{want: []string{"USING INDEX idx_orders_status_live (status=? AND deleted_at=?)"}, notWant: []string{noSort}},
		}},
		{"admin list, parents", func() error {
			_, _, err := ListOrders(OrderListFilter{ParentOnly: true})
			return err
		}, []planCheck{
			uses("SCAN orders USING COVERING INDEX"),
			{want: []string{"SCAN orders"}, notWant: []string{noSort}},
		}},
		{"admin list, parents, by network", func() error {
			// No index narrows this: read the table in id order rather than
			// look rows up in the order of some index.
			_, _, err := ListOrders(OrderListFilter{ParentOnly: true, Network: mdb.NetworkTron})
			return err
		}, []planCheck{
			{want: []string{"SCAN orders"}, notWant: []string{"INDEX"}},
			{want: []string{"SCAN orders"}, notWant: []string{"INDEX", noSort}},
		}},
		{"sub-orders ordered by created_at", func() error {
			_, err := GetAllSubOrders("parent")
			return err
		}, []planCheck{uses("(parent_trade_id=?)")}},
		{"sub-orders of a page of parents", func() error {
			_, err := GetSubOrdersByParentTradeIds([]string{"a", "b", "c"})
			return err
		}, []planCheck{uses("(parent_trade_id=?)")}},
		{"active sub-order by token and network", func() error {
			_, err := GetSubOrderByTokenNetwork("parent", "USDT", mdb.NetworkTron)
			return err
		}, []planCheck{uses("(parent_trade_id=? AND status=? AND deleted_at=?)")}},
	}
	for _, tc := range cases {
		start()
		if err := tc.run(); err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		statements := stop()
		if len(statements) != len(tc.checks) {
			t.Fatalf("%s: ran %d statements, want %d", tc.name, len(statements), len(tc.checks))
		}
		for i, st := range statements {
			plan := queryPlan(t, dao.Mdb, st)
			check := tc.checks[i]
			check.notWant = append(check.notWant, "idx_orders_deleted_at")
			for _, want := range check.want {
				if !strings.Contains(plan, want) {
					t.Errorf("%s: plan %q does not contain %q\n  sql: %s", tc.name, plan, want, st.sql)
				}
			}
			for _, notWant := range check.notWant {
				if strings.Contains(plan, notWant) {
					t.Errorf("%s: plan %q contains %q\n  sql: %s", tc.name, plan, notWant, st.sql)
				}
			}
		}
	}
}

// TestOnChainMatchFindsMigratedEVMOrders covers orders written by older
// versions with checksummed EVM addresses: the match compares
// receive_address directly, so it relies on the startup migration.
func TestOnChainMatchFindsMigratedEVMOrders(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	const checksummed = "0xAbCdEf0000000000000000000000000000000001"
	order := mdb.Orders{
		TradeId:        "legacy-evm",
		OrderId:        "legacy-evm",
		Network:        mdb.NetworkEthereum,
		Token:          "USDT",
		ReceiveAddress: checksummed,
		ActualAmount:   1.23,
		Status:         mdb.StatusExpired,
		PayProvider:    mdb.PaymentProviderOnChain,
	}
	if err := dao.Mdb.Create(&order).Error; err != nil {
		t.Fatalf("create order: %v", err)
	}
	before := time.Now().Add(time.Minute)

	found, err := GetOnChainOrderByWalletAddressAndAmountAndTokenBeforeTime(mdb.NetworkEthereum, checksummed, "USDT", 1.23, before)
	if err != nil || found.ID != 0 {
		t.Fatalf("before migration: id=%d err=%v, want no match", found.ID, err)
	}
	if _, err := dao.LowercaseEVMOrderReceiveAddresses(dao.Mdb); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	for _, input := range []string{checksummed, strings.ToLower(checksummed), strings.ToUpper("0x" + checksummed[2:])} {
		found, err = GetOnChainOrderByWalletAddressAndAmountAndTokenBeforeTime(mdb.NetworkEthereum, input, "usdt", 1.23, before)
		if err != nil || found.TradeId != order.TradeId {
			t.Fatalf("input %s: trade_id=%q err=%v, want %q", input, found.TradeId, err, order.TradeId)
		}
	}
}
