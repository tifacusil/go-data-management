package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/AngelloFD/go-data-management/internal/observability"
	"github.com/AngelloFD/go-data-management/internal/query"
)

type fakeQuery struct {
	paged   query.Paged
	latest  []query.Reading
	err     error
}

func (f fakeQuery) SensorReadings(_ context.Context, _ string, _ query.Filter, _, _ int) (query.Paged, error) {
	return f.paged, f.err
}
func (f fakeQuery) ProjectReadings(_ context.Context, _ string, _ query.Filter, _, _ int) (query.Paged, error) {
	return f.paged, f.err
}
func (f fakeQuery) ProjectLatest(_ context.Context, _ string, _ query.Filter) ([]query.Reading, error) {
	return f.latest, f.err
}

type fakeHealth struct{ mysqlErr, rabbitErr error }

func (f fakeHealth) CheckMySQL(context.Context) error    { return f.mysqlErr }
func (f fakeHealth) CheckRabbitMQ(context.Context) error { return f.rabbitErr }

func newTestServer(q QueryService, h HealthChecker) http.Handler {
	c := observability.NewCounter()
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	return NewServer(q, h, c, nil, log).Router()
}

func newTestServerWithMetrics(q QueryService, h HealthChecker, m *observability.Metrics) http.Handler {
	c := observability.NewCounter()
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	return NewServer(q, h, c, m, log).Router()
}

func do(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestHealth_Healthy(t *testing.T) {
	h := newTestServer(fakeQuery{}, fakeHealth{})
	rec := do(t, h, "/health")
	if rec.Code != http.StatusOK {
		t.Fatalf("esperaba 200, got %d", rec.Code)
	}
	var body map[string]any
	json.Unmarshal(rec.Body.Bytes(), &body)
	if body["status"] != "healthy" {
		t.Errorf("status=%v", body["status"])
	}
}

func TestHealth_Unhealthy(t *testing.T) {
	h := newTestServer(fakeQuery{}, fakeHealth{mysqlErr: errors.New("down")})
	rec := do(t, h, "/health")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("esperaba 503, got %d", rec.Code)
	}
}

func TestSensorReadings_OK(t *testing.T) {
	fq := fakeQuery{paged: query.Paged{Total: 1, Limit: 200, Items: []query.Reading{{SensorID: "S-1"}}}}
	h := newTestServer(fq, fakeHealth{})
	rec := do(t, h, "/api/v1/sensors/S-1/readings")
	if rec.Code != http.StatusOK {
		t.Fatalf("esperaba 200, got %d", rec.Code)
	}
	var p query.Paged
	json.Unmarshal(rec.Body.Bytes(), &p)
	if p.Total != 1 || len(p.Items) != 1 {
		t.Errorf("respuesta inesperada: %+v", p)
	}
}

func TestSensorReadings_BadParam(t *testing.T) {
	h := newTestServer(fakeQuery{}, fakeHealth{})
	rec := do(t, h, "/api/v1/sensors/S-1/readings?limit=0")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("esperaba 400, got %d", rec.Code)
	}
}

func TestProjectLatest_EmptyIsArray(t *testing.T) {
	h := newTestServer(fakeQuery{latest: nil}, fakeHealth{})
	rec := do(t, h, "/api/v1/projects/P-1/latest")
	if rec.Code != http.StatusOK {
		t.Fatalf("esperaba 200, got %d", rec.Code)
	}
	if got := rec.Body.String(); got != "[]\n" {
		t.Errorf("vacio debe ser [], got %q", got)
	}
}

func TestSensorReadings_InternalError(t *testing.T) {
	h := newTestServer(fakeQuery{err: errors.New("db boom")}, fakeHealth{})
	rec := do(t, h, "/api/v1/sensors/S-1/readings")
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("esperaba 500, got %d", rec.Code)
	}
}
