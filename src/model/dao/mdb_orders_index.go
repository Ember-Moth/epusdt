package dao

import (
	"fmt"

	scanner "github.com/GMWalletApp/ChainScanner"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/gookit/color"
	"gorm.io/gorm"
)

// orderIndexes are composite indexes on orders that gorm struct tags cannot
// express: DeletedAt lives in the shared BaseModel, so tagging it would add
// the column to indexes on every table. Each one ends with deleted_at, which
// gorm's soft delete adds to every query as "deleted_at IS NULL", so that
// check never needs the table row. Column orders were checked with EXPLAIN
// QUERY PLAN on 300k orders, with and without ANALYZE;
// TestOrderQueriesUseCompositeIndexes in model/data pins the plans.
var orderIndexes = []struct {
	name    string
	columns string
}{
	// mq expiry poll (status IN (1,4) AND created_at <= ?), expired counts,
	// average payment time, and an index-only SUM(actual_amount) of paid
	// orders. A status index without actual_amount would make that SUM read
	// 60% of the table in random order.
	{"idx_orders_status_created", "status, created_at, actual_amount, deleted_at"},
	// Lists and counts filtered by status alone. With both columns fixed the
	// rows come back in id order, so the newest page needs no sort.
	{"idx_orders_status_live", "status, deleted_at"},
	// mq callback poll: status = 2 AND callback_confirm = 2 AND
	// callback_num <= max. Orders that exhausted their retries are skipped
	// inside the index.
	{"idx_orders_callback_due", "status, callback_confirm, callback_num, deleted_at"},
	// Fallback match for an inbound transfer without a transaction lock:
	// seek by amount and address, newest first, filtering status in the
	// index. actual_amount comes first so that only this lookup uses the
	// index: a query on network, token or address alone would read it in an
	// order unrelated to id and fetch rows at random.
	{"idx_orders_amount_match", "actual_amount, network, token, receive_address, created_at, status, deleted_at"},
	// Admin list filtered by status (COUNT from the index, page in id order)
	// and sub-order lookups by parent and status.
	{"idx_orders_parent_status", "parent_trade_id, status, deleted_at"},
	// Dashboard aggregates over a created_at range, read from the index only.
	{"idx_orders_created_stats", "created_at, status, amount, actual_amount, receive_address, deleted_at"},
}

// deletedAtIndex is the single-column index gorm creates from BaseModel's
// DeletedAt tag. Its sqlite_stat1 row is set by ensureDeletedAtIndexStat.
const deletedAtIndex = "idx_orders_deleted_at"

// EnsureOrderIndexes creates the composite orders indexes when missing and
// records the planner statistic for idx_orders_deleted_at. Creating an index
// on a large existing table takes a few hundred milliseconds; later starts
// only check that they exist.
func EnsureOrderIndexes(db *gorm.DB) error {
	table := (&mdb.Orders{}).TableName()
	for _, idx := range orderIndexes {
		if db.Migrator().HasIndex(table, idx.name) {
			continue
		}
		color.Green.Printf("[store_db] creating index %s on %s\n", idx.name, table)
		sql := fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (%s)", idx.name, table, idx.columns)
		if err := db.Exec(sql).Error; err != nil {
			return fmt.Errorf("create index %s: %w", idx.name, err)
		}
	}
	return ensureDeletedAtIndexStat(db, table)
}

// ensureDeletedAtIndexStat tells the query planner that idx_orders_deleted_at
// does not narrow anything: every live order has deleted_at NULL.
//
// gorm adds "deleted_at IS NULL" to every orders query. With no
// sqlite_stat1 data (nothing runs ANALYZE) SQLite assumes an equality on an
// indexed column matches about ten rows, so it walked the whole table
// through this index for most order queries, including the mq polls that run
// every second. A sqlite_stat1 row saying that one deleted_at value covers
// every row removes the index from consideration. Other indexes keep
// SQLite's built-in estimates, which is how the composite indexes above
// were checked. The row is written only when the index has none, so a real
// ANALYZE, whose numbers say the same thing, is kept.
func ensureDeletedAtIndexStat(db *gorm.DB, table string) error {
	// ANALYZE on the small sqlite_schema table creates sqlite_stat1 when it
	// is missing, and makes this connection reload the statistics.
	if err := db.Exec("ANALYZE sqlite_schema").Error; err != nil {
		return fmt.Errorf("create sqlite_stat1: %w", err)
	}
	result := db.Exec(`INSERT INTO sqlite_stat1 (tbl, idx, stat)
		SELECT ?, ?, '1000000 1000000'
		WHERE NOT EXISTS (SELECT 1 FROM sqlite_stat1 WHERE tbl = ? AND idx = ?)`,
		table, deletedAtIndex, table, deletedAtIndex)
	if result.Error != nil {
		return fmt.Errorf("write %s statistic: %w", deletedAtIndex, result.Error)
	}
	if result.RowsAffected == 0 {
		return nil
	}
	return db.Exec("ANALYZE sqlite_schema").Error
}

// LowercaseEVMOrderReceiveAddresses stores EVM receive addresses in
// lowercase so order lookups can compare receive_address directly and use
// an index. New orders are already written lowercase; this rewrites rows
// left by older versions. Rows that are already lowercase are not touched,
// so later runs change nothing. updated_at is left as is because the
// callback scheduler and the dashboards read it.
func LowercaseEVMOrderReceiveAddresses(db *gorm.DB) (int64, error) {
	var networks []string
	if err := db.Unscoped().Model(&mdb.Chain{}).Pluck("network", &networks).Error; err != nil {
		return 0, err
	}
	evm := make([]string, 0, len(networks))
	for _, network := range networks {
		if scanner.IsEVM(network) {
			evm = append(evm, network)
		}
	}
	if len(evm) == 0 {
		return 0, nil
	}
	result := db.Unscoped().Model(&mdb.Orders{}).
		Where("network IN ?", evm).
		Where("receive_address <> lower(receive_address)").
		UpdateColumn("receive_address", gorm.Expr("lower(receive_address)"))
	return result.RowsAffected, result.Error
}

func migrateOrders() {
	changed, err := LowercaseEVMOrderReceiveAddresses(Mdb)
	if err != nil {
		color.Red.Printf("[store_db] lowercase EVM order receive_address err=%s\n", err)
	} else if changed > 0 {
		color.Green.Printf("[store_db] lowercased receive_address on %d EVM orders\n", changed)
	}
	if err := EnsureOrderIndexes(Mdb); err != nil {
		color.Red.Printf("[store_db] orders indexes err=%s\n", err)
	}
}
