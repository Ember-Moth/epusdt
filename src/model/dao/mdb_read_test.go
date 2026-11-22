package dao

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/util/log"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// openPrimaryDBForTest opens the writer and the read pool on a temporary
// file the way DBInit does, with the driver of the active build tag.
func openPrimaryDBForTest(t *testing.T) {
	t.Helper()
	oldMdb, oldRead, oldSugar := Mdb, MdbRead, log.Sugar
	oldFile := viper.Get("sqlite_database_filename")
	viper.Set("sqlite_database_filename", filepath.Join(t.TempDir(), "primary.db"))
	if log.Sugar == nil {
		log.Sugar = zap.NewNop().Sugar()
	}
	if err := SqliteInit(); err != nil {
		t.Fatalf("SqliteInit: %v", err)
	}
	SqliteReadPoolInit()
	t.Cleanup(func() {
		CloseReadPool()
		if sqlDB, err := Mdb.DB(); err == nil {
			_ = sqlDB.Close()
		}
		Mdb, MdbRead, log.Sugar = oldMdb, oldRead, oldSugar
		viper.Set("sqlite_database_filename", oldFile)
	})
}

func TestSqliteInitOpensQueryOnlyReadPool(t *testing.T) {
	openPrimaryDBForTest(t)
	if MdbRead == nil || ReadDB() != MdbRead {
		t.Fatal("ReadDB does not return the read pool")
	}
	var mode string
	if err := Mdb.Raw("PRAGMA journal_mode").Scan(&mode).Error; err != nil || mode != "wal" {
		t.Fatalf("journal_mode = %q err=%v, want wal", mode, err)
	}

	if err := Mdb.Exec("CREATE TABLE probe (v INTEGER)").Error; err != nil {
		t.Fatalf("create on writer: %v", err)
	}
	if err := Mdb.Exec("INSERT INTO probe (v) VALUES (42)").Error; err != nil {
		t.Fatalf("insert on writer: %v", err)
	}
	var v int
	if err := MdbRead.Raw("SELECT v FROM probe").Scan(&v).Error; err != nil || v != 42 {
		t.Fatalf("read pool sees v=%d err=%v, want the committed 42", v, err)
	}

	sqlDB, err := MdbRead.DB()
	if err != nil {
		t.Fatal(err)
	}
	if got := sqlDB.Stats().MaxOpenConnections; got != readPoolSize() {
		t.Fatalf("read pool max open = %d, want %d", got, readPoolSize())
	}
	// Hold every pooled connection at once so each one is checked.
	ctx := context.Background()
	for i := 0; i < readPoolSize(); i++ {
		conn, err := sqlDB.Conn(ctx)
		if err != nil {
			t.Fatalf("conn %d: %v", i, err)
		}
		defer conn.Close()
		var queryOnly int
		if err := conn.QueryRowContext(ctx, "PRAGMA query_only").Scan(&queryOnly); err != nil || queryOnly != 1 {
			t.Fatalf("conn %d query_only = %d err=%v, want 1", i, queryOnly, err)
		}
		if _, err := conn.ExecContext(ctx, "INSERT INTO probe (v) VALUES (1)"); err == nil {
			t.Fatalf("conn %d accepted a write", i)
		}
	}
}

func TestReadDBFallsBackToMdb(t *testing.T) {
	Mdb = setupSeedTableTestDB(t)
	oldRead := MdbRead
	MdbRead = nil
	t.Cleanup(func() { MdbRead = oldRead })
	if ReadDB() != Mdb {
		t.Fatal("ReadDB without a read pool should return Mdb")
	}
}

// slowReadSQL is CPU bound and reads no table, so only connection
// availability, not SQLite locking, decides whether other work waits.
const slowReadSQL = "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < ?) SELECT count(*) FROM c"

// slowReadRows returns a row count that makes slowReadSQL take about target.
func slowReadRows(t *testing.T, db *gorm.DB, target time.Duration) int64 {
	t.Helper()
	n := int64(50_000)
	for {
		start := time.Now()
		var count int64
		if err := db.Raw(slowReadSQL, n).Scan(&count).Error; err != nil {
			t.Fatalf("calibrate slow read: %v", err)
		}
		if took := time.Since(start); took >= 50*time.Millisecond {
			return int64(float64(n) * float64(target) / float64(took))
		}
		n *= 4
	}
}

func waitInUse(t *testing.T, db *gorm.DB) {
	t.Helper()
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(5 * time.Second); sqlDB.Stats().InUse == 0; {
		if time.Now().After(deadline) {
			t.Fatal("slow read never took a connection")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestReadPoolDoesNotBlockWriterConnection(t *testing.T) {
	openPrimaryDBForTest(t)
	if err := Mdb.Exec("CREATE TABLE probe (v INTEGER)").Error; err != nil {
		t.Fatal(err)
	}
	rows := slowReadRows(t, MdbRead, 500*time.Millisecond)

	// writeDuringSlowRead starts slowReadSQL on slowDB, then writes and
	// reads back on Mdb. It reports how long the Mdb work took and whether
	// it finished while the slow read was still running.
	writeDuringSlowRead := func(slowDB *gorm.DB) (time.Duration, bool) {
		slowDone := make(chan time.Time, 1)
		go func() {
			var count int64
			if err := slowDB.Raw(slowReadSQL, rows).Scan(&count).Error; err != nil {
				t.Errorf("slow read: %v", err)
			}
			slowDone <- time.Now()
		}()
		waitInUse(t, slowDB)

		start := time.Now()
		if err := Mdb.Exec("INSERT INTO probe (v) VALUES (1)").Error; err != nil {
			t.Fatalf("write on writer: %v", err)
		}
		var count int64
		if err := Mdb.Raw("SELECT count(*) FROM probe").Scan(&count).Error; err != nil || count == 0 {
			t.Fatalf("read back on writer: count=%d err=%v", count, err)
		}
		finished := time.Now()
		return finished.Sub(start), (<-slowDone).After(finished)
	}

	took, overlapped := writeDuringSlowRead(MdbRead)
	if !overlapped {
		t.Fatalf("writer work took %s and only finished after the slow read on the read pool", took)
	}

	// Control: the same slow read on the single writer connection holds it,
	// so the writer work has to wait for it. This shows the check above
	// would catch a read path that blocks the writer.
	blockedFor, overlapped := writeDuringSlowRead(Mdb)
	if overlapped {
		t.Fatalf("control: writer work finished in %s while the slow read held the writer connection", blockedFor)
	}
	t.Logf("writer insert+select during a slow read: %s with the read pool, %s on the writer connection", took, blockedFor)
}
