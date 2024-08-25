package engine

import (
	"database/sql"
	"github.com/go-streamline/core/config"
	"github.com/pressly/goose/v3"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"path"
)

func newSQLiteDB(config *config.Config) (*gorm.DB, error) {
	return gorm.Open(sqlite.Open(path.Join(config.Workdir, "db", "go-streamline.db")), &gorm.Config{})
}

func runMigrations(db *sql.DB, migrationsDir string) error {
	if err := goose.SetDialect("sqlite3"); err != nil {
		return err
	}

	if err := goose.Up(db, migrationsDir); err != nil {
		return err
	}

	return nil
}
