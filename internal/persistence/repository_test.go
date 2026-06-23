package persistence

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/AngelloFD/go-data-management/internal/model"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
)

func ptrF(f float64) *float64 { return &f }
func ptrS(s string) *string   { return &s }

func newTestRepo(t *testing.T) (*Repository, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return New(db), mock
}

func sampleEnvelope() *model.Envelope {
	return &model.Envelope{
		MessageID:  "m1",
		ProjectID:  "demo",
		SensorID:   "GNSS-01",
		SensorType: model.SensorGNSS,
		Timestamp:  time.Date(2026, 5, 17, 0, 0, 0, 0, time.UTC),
		ReceivedAt: time.Date(2026, 5, 17, 0, 0, 1, 0, time.UTC),
	}
}

func TestPersistGNSS_InsertOK(t *testing.T) {
	repo, mock := newTestRepo(t)
	e := sampleEnvelope()
	p := &model.GNSSPayload{DisplacementXmm: ptrF(1.2), FixQuality: ptrS("RTK_FIXED")}

	mock.ExpectExec("INSERT IGNORE INTO gnss_readings").
		WithArgs("m1", "demo", "GNSS-01", e.Timestamp, e.ReceivedAt,
			p.DisplacementXmm, p.DisplacementYmm, p.DisplacementZmm,
			p.DisplacementHorizontalMm, p.CumulativeDisplacementMm, p.FixQuality).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := repo.PersistGNSS(context.Background(), e, p); err != nil {
		t.Fatalf("esperaba nil, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectativas: %v", err)
	}
}

func TestPersistGNSS_DuplicateNoOp(t *testing.T) {
	repo, mock := newTestRepo(t)
	e := sampleEnvelope()
	p := &model.GNSSPayload{}

	// INSERT IGNORE de duplicado: 0 filas afectadas, sin error -> idempotente.
	mock.ExpectExec("INSERT IGNORE INTO gnss_readings").
		WillReturnResult(sqlmock.NewResult(0, 0))

	if err := repo.PersistGNSS(context.Background(), e, p); err != nil {
		t.Fatalf("duplicado debe ser no-op, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectativas: %v", err)
	}
}

func TestPersist_TransientErrorIsRetryable(t *testing.T) {
	repo, mock := newTestRepo(t)
	e := sampleEnvelope()

	mock.ExpectExec("INSERT IGNORE INTO gnss_readings").
		WillReturnError(&mysql.MySQLError{Number: 1213, Message: "deadlock"})

	err := repo.PersistGNSS(context.Background(), e, &model.GNSSPayload{})
	var re *RetryableError
	if !errors.As(err, &re) {
		t.Fatalf("deadlock debe ser RetryableError, got %T: %v", err, err)
	}
}

func TestPersist_PermanentErrorIsNonRetryable(t *testing.T) {
	repo, mock := newTestRepo(t)
	e := sampleEnvelope()

	// 1146 = ER_NO_SUCH_TABLE: permanente.
	mock.ExpectExec("INSERT IGNORE INTO gnss_readings").
		WillReturnError(&mysql.MySQLError{Number: 1146, Message: "no such table"})

	err := repo.PersistGNSS(context.Background(), e, &model.GNSSPayload{})
	var nre *NonRetryableError
	if !errors.As(err, &nre) {
		t.Fatalf("tabla inexistente debe ser NonRetryableError, got %T: %v", err, err)
	}
}

func TestPersistRainfall_InsertOK(t *testing.T) {
	repo, mock := newTestRepo(t)
	e := sampleEnvelope()
	e.SensorType = model.SensorRainfall
	p := &model.RainfallPayload{RainfallMm: ptrF(1.2)}

	mock.ExpectExec("INSERT IGNORE INTO rainfall_readings").
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := repo.PersistRainfall(context.Background(), e, p); err != nil {
		t.Fatalf("esperaba nil, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectativas: %v", err)
	}
}
