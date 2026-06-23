// Package httpapi expone la query API REST y el health check (CLAUDE.md 1.6).
// Solo endpoints externos/operacionales; la ingesta es via RabbitMQ.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/AngelloFD/go-data-management/internal/observability"
	"github.com/AngelloFD/go-data-management/internal/query"
	"github.com/go-chi/chi/v5"
)

// QueryService es lo que el server necesita de la capa query.
type QueryService interface {
	SensorReadings(ctx context.Context, sensorID string, f query.Filter, offset, limit int) (query.Paged, error)
	ProjectReadings(ctx context.Context, projectID string, f query.Filter, offset, limit int) (query.Paged, error)
	ProjectLatest(ctx context.Context, projectID string, f query.Filter) ([]query.Reading, error)
}

// HealthChecker verifica dependencias reales.
type HealthChecker interface {
	CheckMySQL(ctx context.Context) error
	CheckRabbitMQ(ctx context.Context) error
}

type Server struct {
	query   QueryService
	health  HealthChecker
	counter *observability.Counter
	log     *slog.Logger
}

func NewServer(q QueryService, h HealthChecker, c *observability.Counter, log *slog.Logger) *Server {
	return &Server{query: q, health: h, counter: c, log: log}
}

// Router arma las rutas conforme al OpenAPI de M0.
func (s *Server) Router() http.Handler {
	r := chi.NewRouter()
	r.Get("/health", s.handleHealth)
	r.Get("/api/v1/sensors/{sensor_id}/readings", s.handleSensorReadings)
	r.Get("/api/v1/projects/{project_id}/readings", s.handleProjectReadings)
	r.Get("/api/v1/projects/{project_id}/latest", s.handleProjectLatest)
	// TODO(M4): r.Handle("/metrics", promhttp.Handler())
	return r
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	mysqlErr := s.health.CheckMySQL(ctx)
	rabbitErr := s.health.CheckRabbitMQ(ctx)
	healthy := mysqlErr == nil && rabbitErr == nil

	body := map[string]any{
		"status":             statusStr(healthy),
		"mysql":              checkMap(mysqlErr),
		"rabbitmq":           checkMap(rabbitErr),
		"messages_processed": s.counter.Get(),
	}
	code := http.StatusOK
	if !healthy {
		code = http.StatusServiceUnavailable
	}
	writeJSON(w, code, body)
}

func (s *Server) handleSensorReadings(w http.ResponseWriter, r *http.Request) {
	p, err := query.ParseParams(r.URL.Query(), true)
	if err != nil {
		s.writeErr(w, err)
		return
	}
	sensorID := chi.URLParam(r, "sensor_id")
	res, err := s.query.SensorReadings(r.Context(), sensorID, p.Filter, p.Offset, p.Limit)
	if err != nil {
		s.writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) handleProjectReadings(w http.ResponseWriter, r *http.Request) {
	p, err := query.ParseParams(r.URL.Query(), true)
	if err != nil {
		s.writeErr(w, err)
		return
	}
	projectID := chi.URLParam(r, "project_id")
	res, err := s.query.ProjectReadings(r.Context(), projectID, p.Filter, p.Offset, p.Limit)
	if err != nil {
		s.writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) handleProjectLatest(w http.ResponseWriter, r *http.Request) {
	p, err := query.ParseParams(r.URL.Query(), false)
	if err != nil {
		s.writeErr(w, err)
		return
	}
	projectID := chi.URLParam(r, "project_id")
	res, err := s.query.ProjectLatest(r.Context(), projectID, p.Filter)
	if err != nil {
		s.writeErr(w, err)
		return
	}
	if res == nil {
		res = []query.Reading{}
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) writeErr(w http.ResponseWriter, err error) {
	var bad *query.BadRequest
	if errors.As(err, &bad) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": bad.Msg})
		return
	}
	s.log.Error("error en query", slog.String("error", err.Error()))
	writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "error interno"})
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}

func statusStr(ok bool) string {
	if ok {
		return "healthy"
	}
	return "unhealthy"
}

func checkMap(err error) map[string]any {
	if err == nil {
		return map[string]any{"ok": true}
	}
	return map[string]any{"ok": false, "error": err.Error()}
}
