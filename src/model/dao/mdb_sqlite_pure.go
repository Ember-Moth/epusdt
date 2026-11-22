//go:build !sqlite_cgo

package dao

import (
	"github.com/libtnb/sqlite"
	"gorm.io/gorm"
)

func openDB(dsn string, cfg *gorm.Config) (*gorm.DB, error) {
	// modernc.org/sqlite applies connection-local PRAGMAs through repeated
	// _pragma query parameters. The old _busy_timeout form was ignored by the
	// pure-Go driver, so pooled connections failed immediately with SQLITE_BUSY.
	db, err := gorm.Open(sqlite.Open(dsn+"?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)"), cfg)
	if err != nil {
		return nil, err
	}
	return db, nil
}

// openReadDB opens connections that reject writes (PRAGMA query_only).
func openReadDB(dsn string, cfg *gorm.Config) (*gorm.DB, error) {
	return gorm.Open(sqlite.Open(dsn+"?_pragma=busy_timeout(5000)&_pragma=query_only(1)"), cfg)
}
