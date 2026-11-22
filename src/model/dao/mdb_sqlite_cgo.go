//go:build sqlite_cgo

package dao

import (
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func openDB(dsn string, cfg *gorm.Config) (*gorm.DB, error) {
	db, err := gorm.Open(sqlite.Open(dsn+"?_journal_mode=WAL&_busy_timeout=5000"), cfg)
	if err != nil {
		return nil, err
	}
	return db, nil
}

// openReadDB opens connections that reject writes (PRAGMA query_only).
func openReadDB(dsn string, cfg *gorm.Config) (*gorm.DB, error) {
	return gorm.Open(sqlite.Open(dsn+"?_busy_timeout=5000&_query_only=1"), cfg)
}
