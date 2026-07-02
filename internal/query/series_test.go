package query

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestProjectSeries_CanonicalColumnPerType(t *testing.T) {
	cases := map[string]string{
		"gnss":              "cumulative_displacement_mm",
		"deep_displacement": "cumulative_tilt_mm",
		"rainfall":          "cumulative_rainfall_mm",
		"strain":            "strain_microstrain",
		"soil_pressure":     "pressure_kpa",
	}
	for st, wantCol := range cases {
		got, ok := ValueColumnFor(st)
		if !ok || got != wantCol {
			t.Errorf("%s: got %q ok=%v, want %q", st, got, ok, wantCol)
		}
	}
}

func TestProjectSeries_InvalidType(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	q := New(db)

	if _, err := q.ProjectSeries(context.Background(), "p1", "nope", "a", "b", 10); err == nil {
		t.Fatal("esperaba error por sensor_type invalido")
	}
}

func TestProjectSeries_QueriesCanonicalColumnAndOrdersAsc(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	q := New(db)

	ts := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	rows := sqlmock.NewRows([]string{"timestamp", "val"}).
		AddRow(ts, 1.5).
		AddRow(ts.Add(time.Hour), 2.0)

	// Debe consultar la columna canonica de gnss sobre gnss_readings, ORDER ASC.
	mock.ExpectQuery(regexp.QuoteMeta(
		"SELECT `timestamp`, `cumulative_displacement_mm` AS val FROM gnss_readings")).
		WithArgs("p1", "2026-06-01 00:00:00", "2026-06-02 00:00:00", 500).
		WillReturnRows(rows)

	got, err := q.ProjectSeries(context.Background(), "p1", "gnss",
		"2026-06-01 00:00:00", "2026-06-02 00:00:00", 500)
	if err != nil {
		t.Fatal(err)
	}
	if got.ValueColumn != "cumulative_displacement_mm" {
		t.Errorf("value_column = %q", got.ValueColumn)
	}
	if len(got.Points) != 2 || got.Points[0].Value != 1.5 {
		t.Errorf("points = %+v", got.Points)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}
