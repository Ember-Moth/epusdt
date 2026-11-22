package dao

import (
	"runtime"

	"gorm.io/gorm"
)

// MdbRead is a read-only connection pool on the primary database. Mdb keeps
// a single connection, which serializes every query. In WAL mode SQLite
// lets readers run while the writer works, so moving slow dashboard and
// admin reads here keeps checkout status, order creation and callbacks from
// queuing behind them. Nil until SqliteReadPoolInit runs; callers use
// ReadDB.
var MdbRead *gorm.DB

// ReadDB returns the read-only pool, or Mdb when no pool is open (tests set
// only Mdb). A read here starts a new snapshot that includes every
// committed write, but not uncommitted work in a transaction on Mdb. Reads
// inside a transaction, and request paths that write and then read back,
// stay on Mdb.
func ReadDB() *gorm.DB {
	if MdbRead != nil {
		return MdbRead
	}
	return Mdb
}

// readPoolSize bounds concurrent readers: enough for a dashboard load that
// fans out several aggregates, small enough to leave CPU for the writer.
func readPoolSize() int {
	return min(max(runtime.NumCPU(), 2), 4)
}

// openReadPool opens query_only connections to path. The writer must have
// opened the file and switched it to WAL first; journal_mode is stored in
// the database file, so readers do not set it.
func openReadPool(path string, cfg *gorm.Config) (*gorm.DB, error) {
	db, err := openReadDB(path, cfg)
	if err != nil {
		return nil, err
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}
	n := readPoolSize()
	sqlDB.SetMaxOpenConns(n)
	sqlDB.SetMaxIdleConns(n)
	if err := sqlDB.Ping(); err != nil {
		_ = sqlDB.Close()
		return nil, err
	}
	return db, nil
}

// CloseReadPool closes MdbRead and clears it, so ReadDB falls back to Mdb.
func CloseReadPool() {
	if MdbRead == nil {
		return
	}
	if sqlDB, err := MdbRead.DB(); err == nil {
		_ = sqlDB.Close()
	}
	MdbRead = nil
}
