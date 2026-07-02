package grpcapi

import (
	"context"
	"io"
	"log/slog"
	"regexp"
	"testing"

	datamanagementv1 "github.com/AngelloFD/go-data-management/internal/grpcapi/gen/datamanagement/v1"
	"github.com/AngelloFD/go-data-management/internal/query"
	"github.com/DATA-DOG/go-sqlmock"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newServer(t *testing.T) (*ReadingServer, sqlmock.Sqlmock, func()) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	srv := NewReadingServer(query.New(db), slog.New(slog.NewTextHandler(io.Discard, nil)))
	return srv, mock, func() { _ = db.Close() }
}

func TestGetReadingSeries_MissingArgs(t *testing.T) {
	srv, _, done := newServer(t)
	defer done()

	_, err := srv.GetReadingSeries(context.Background(), &datamanagementv1.GetReadingSeriesRequest{
		SensorType: "gnss", WindowStart: "", WindowEnd: "",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument", status.Code(err))
	}
}

func TestGetReadingSeries_InvalidSensorType(t *testing.T) {
	srv, _, done := newServer(t)
	defer done()

	_, err := srv.GetReadingSeries(context.Background(), &datamanagementv1.GetReadingSeriesRequest{
		ProjectId: "p1", SensorType: "nope",
		WindowStart: "2026-06-01T00:00:00Z", WindowEnd: "2026-06-02T00:00:00Z",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument", status.Code(err))
	}
}

func TestGetReadingSeries_InvalidWindowFormat(t *testing.T) {
	srv, _, done := newServer(t)
	defer done()

	_, err := srv.GetReadingSeries(context.Background(), &datamanagementv1.GetReadingSeriesRequest{
		ProjectId: "p1", SensorType: "gnss",
		WindowStart: "not-a-date", WindowEnd: "2026-06-02T00:00:00Z",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument", status.Code(err))
	}
}

// Ventana vacia => respuesta sin puntos, NO error (no tripea el breaker del
// consumidor). Es el fix central de la violacion §1.2.
func TestGetReadingSeries_EmptyIsNotError(t *testing.T) {
	srv, mock, done := newServer(t)
	defer done()

	mock.ExpectQuery(regexp.QuoteMeta("FROM gnss_readings")).
		WithArgs("p1", "2026-06-01 00:00:00", "2026-06-02 00:00:00", 500).
		WillReturnRows(sqlmock.NewRows([]string{"timestamp", "val"}))

	resp, err := srv.GetReadingSeries(context.Background(), &datamanagementv1.GetReadingSeriesRequest{
		ProjectId: "p1", SensorType: "gnss",
		WindowStart: "2026-06-01T00:00:00Z", WindowEnd: "2026-06-02T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("esperaba nil error, got %v", err)
	}
	if len(resp.GetPoints()) != 0 {
		t.Errorf("points = %d, want 0", len(resp.GetPoints()))
	}
	if resp.GetValueColumn() != "cumulative_displacement_mm" {
		t.Errorf("value_column = %q", resp.GetValueColumn())
	}
}
