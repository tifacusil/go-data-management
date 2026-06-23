package persistence_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/AngelloFD/go-data-management/internal/model"
	"github.com/AngelloFD/go-data-management/internal/persistence"
	_ "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go"
	tcmysql "github.com/testcontainers/testcontainers-go/modules/mysql"
)

// TestMigrateAndPersist_Integration levanta un MySQL efimero, aplica migraciones
// y verifica: particiones creadas, insert ok, y dedup por message_id no-op
// (CLAUDE.md 3.4). Se salta con -short (no requiere Docker en CI rapido).
func TestMigrateAndPersist_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("integration: requiere Docker, saltado con -short")
	}
	ctx := context.Background()

	ctr, err := tcmysql.Run(ctx, "mysql:8.0",
		tcmysql.WithDatabase("data_management"),
		tcmysql.WithUsername("data_management"),
		tcmysql.WithPassword("test"),
	)
	if err != nil {
		t.Fatalf("arrancando mysql: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(ctr) })

	dsn, err := ctr.ConnectionString(ctx, "parseTime=true", "loc=UTC", "multiStatements=true")
	if err != nil {
		t.Fatalf("dsn: %v", err)
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	// Esperar a que MySQL acepte conexiones.
	deadline := time.Now().Add(60 * time.Second)
	for {
		if err = db.PingContext(ctx); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("mysql no respondio: %v", err)
		}
		time.Sleep(time.Second)
	}

	if err := persistence.Migrate(db); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	// Migrate idempotente: re-aplicar no debe fallar.
	if err := persistence.Migrate(db); err != nil {
		t.Fatalf("migrate 2da vez no debe fallar: %v", err)
	}

	// Particiones creadas en gnss_readings.
	var partCount int
	err = db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.PARTITIONS
		WHERE TABLE_SCHEMA = 'data_management' AND TABLE_NAME = 'gnss_readings'
		AND PARTITION_NAME IS NOT NULL`).Scan(&partCount)
	if err != nil {
		t.Fatalf("contando particiones: %v", err)
	}
	if partCount < 5 {
		t.Errorf("esperaba >=5 particiones en gnss_readings, got %d", partCount)
	}

	repo := persistence.New(db)
	e := &model.Envelope{
		MessageID:  "dup-1",
		ProjectID:  "demo",
		SensorID:   "GNSS-01",
		SensorType: model.SensorGNSS,
		Timestamp:  time.Date(2026, 5, 17, 0, 0, 0, 0, time.UTC),
		ReceivedAt: time.Date(2026, 5, 17, 0, 0, 1, 0, time.UTC),
	}
	x := 1.2
	p := &model.GNSSPayload{DisplacementXmm: &x}

	if err := repo.PersistGNSS(ctx, e, p); err != nil {
		t.Fatalf("insert 1: %v", err)
	}
	// Mismo message_id + timestamp -> INSERT IGNORE no-op (idempotencia).
	if err := repo.PersistGNSS(ctx, e, p); err != nil {
		t.Fatalf("insert dup: %v", err)
	}

	var rows int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM gnss_readings WHERE message_id = ?`, "dup-1").Scan(&rows); err != nil {
		t.Fatalf("contando: %v", err)
	}
	if rows != 1 {
		t.Errorf("dedup falló: esperaba 1 fila, got %d", rows)
	}
}
