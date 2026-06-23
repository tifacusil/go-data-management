package query_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/AngelloFD/go-data-management/internal/model"
	"github.com/AngelloFD/go-data-management/internal/persistence"
	"github.com/AngelloFD/go-data-management/internal/query"
	_ "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go"
	tcmysql "github.com/testcontainers/testcontainers-go/modules/mysql"
)

func ptr[T any](v T) *T { return &v }

// TestQuery_Integration inserta lecturas reales y verifica las 3 consultas
// (sensor readings paged, project readings, project latest) contra MySQL
// (CLAUDE.md 3.4). Valida el SQL builder de la query unificada.
func TestQuery_Integration(t *testing.T) {
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
		t.Fatalf("mysql: %v", err)
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

	deadline := time.Now().Add(60 * time.Second)
	for db.PingContext(ctx) != nil {
		if time.Now().After(deadline) {
			t.Fatal("mysql no respondio")
		}
		time.Sleep(time.Second)
	}
	if err := persistence.Migrate(db); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	repo := persistence.New(db)
	base := time.Date(2026, 5, 17, 0, 0, 0, 0, time.UTC)
	mk := func(id, sensor string, ts time.Time) *model.Envelope {
		return &model.Envelope{MessageID: id, ProjectID: "P-1", SensorID: sensor, Timestamp: ts, ReceivedAt: ts}
	}
	// Dos lecturas GNSS (mismo sensor, distinto tiempo) + una rainfall.
	must(t, repo.PersistGNSS(ctx, mk("g1", "GNSS-1", base), &model.GNSSPayload{DisplacementXmm: ptr(1.0), FixQuality: ptr("RTK_FIXED")}))
	must(t, repo.PersistGNSS(ctx, mk("g2", "GNSS-1", base.Add(time.Hour)), &model.GNSSPayload{DisplacementXmm: ptr(2.0), FixQuality: ptr("RTK_FIXED")}))
	must(t, repo.PersistRainfall(ctx, mk("r1", "RAIN-1", base), &model.RainfallPayload{RainfallMm: ptr(5.0)}))

	q := query.New(db)

	// sensor readings: GNSS-1 tiene 2.
	page, err := q.SensorReadings(ctx, "GNSS-1", query.Filter{}, 0, 10)
	if err != nil {
		t.Fatalf("sensor readings: %v", err)
	}
	if len(page.Items) != 2 {
		t.Errorf("GNSS-1 esperaba 2 lecturas, got %d", len(page.Items))
	}
	// orden timestamp DESC: la primera es la mas reciente.
	if !page.Items[0].Timestamp.After(page.Items[1].Timestamp) {
		t.Error("readings no ordenadas por timestamp DESC")
	}

	// project readings sin filtro: 3 (2 gnss + 1 rainfall).
	pp, err := q.ProjectReadings(ctx, "P-1", query.Filter{}, 0, 10)
	if err != nil {
		t.Fatalf("project readings: %v", err)
	}
	if len(pp.Items) != 3 {
		t.Errorf("P-1 esperaba 3 lecturas, got %d", len(pp.Items))
	}

	// project readings filtrado por sensor_type=rainfall: 1.
	pr, err := q.ProjectReadings(ctx, "P-1", query.Filter{SensorType: ptr("rainfall")}, 0, 10)
	if err != nil {
		t.Fatalf("project readings rainfall: %v", err)
	}
	if len(pr.Items) != 1 || pr.Items[0].SensorType != "rainfall" {
		t.Errorf("filtro rainfall fallo: %+v", pr.Items)
	}

	// project latest: 1 por sensor (GNSS-1 mas reciente + RAIN-1) = 2.
	latest, err := q.ProjectLatest(ctx, "P-1", query.Filter{})
	if err != nil {
		t.Fatalf("project latest: %v", err)
	}
	if len(latest) != 2 {
		t.Fatalf("latest esperaba 2 (1 por sensor), got %d", len(latest))
	}
	for _, r := range latest {
		if r.SensorType == "gnss" && !r.Timestamp.Equal(base.Add(time.Hour)) {
			t.Errorf("latest gnss debe ser la mas reciente, got %v", r.Timestamp)
		}
	}

	// paginacion: limit 1 -> hasMore -> total cuenta real.
	p1, err := q.SensorReadings(ctx, "GNSS-1", query.Filter{}, 0, 1)
	if err != nil {
		t.Fatalf("page limit 1: %v", err)
	}
	if len(p1.Items) != 1 || p1.Total != 2 {
		t.Errorf("paginacion: items=%d total=%d, esperaba 1 y 2", len(p1.Items), p1.Total)
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("persist: %v", err)
	}
}
