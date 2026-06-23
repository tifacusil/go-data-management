package persistence

import (
	"fmt"

	migrations "github.com/AngelloFD/go-data-management/migrations"
	"github.com/golang-migrate/migrate/v4"
	mysqlmig "github.com/golang-migrate/migrate/v4/database/mysql"
	"github.com/golang-migrate/migrate/v4/source/iofs"

	"database/sql"
)

// Migrate aplica todas las migraciones pendientes (up) usando los SQL embebidos.
// Idempotente: si ya estan aplicadas, no hace nada (CLAUDE.md 2.4).
func Migrate(db *sql.DB) error {
	src, err := iofs.New(migrations.FS, ".")
	if err != nil {
		return fmt.Errorf("source migraciones: %w", err)
	}
	driver, err := mysqlmig.WithInstance(db, &mysqlmig.Config{})
	if err != nil {
		return fmt.Errorf("driver migrate: %w", err)
	}
	m, err := migrate.NewWithInstance("iofs", src, "mysql", driver)
	if err != nil {
		return fmt.Errorf("instancia migrate: %w", err)
	}
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("aplicando migraciones: %w", err)
	}
	return nil
}
