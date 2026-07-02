// Package grpcapi expone las lecturas de data-management via gRPC para consumo
// sincrono de otros servicios (data-analysis), reemplazando el acceso directo a
// las tablas que viola database-per-service (CLAUDE.md §1.2). El servidor solo
// lee; no muta estado.
package grpcapi

import (
	"context"
	"log/slog"
	"time"

	datamanagementv1 "github.com/AngelloFD/go-data-management/internal/grpcapi/gen/datamanagement/v1"
	"github.com/AngelloFD/go-data-management/internal/query"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const defaultSeriesLimit = 500

// mysqlDatetime es el formato que espera la capa query (UTC, sin zona).
const mysqlDatetime = "2006-01-02 15:04:05"

// ReadingServer implementa datamanagement.v1.ReadingService sobre el query repo.
type ReadingServer struct {
	datamanagementv1.UnimplementedReadingServiceServer
	repo *query.Repository
	log  *slog.Logger
}

// NewReadingServer crea el servicer.
func NewReadingServer(repo *query.Repository, log *slog.Logger) *ReadingServer {
	return &ReadingServer{repo: repo, log: log}
}

// GetReadingSeries devuelve la serie (timestamp, valor canonico) de un
// sensor_type para un proyecto en una ventana. Serie vacia no es error: el
// consumidor la trata como no_data (evita tripear su circuit breaker).
func (s *ReadingServer) GetReadingSeries(ctx context.Context, req *datamanagementv1.GetReadingSeriesRequest) (*datamanagementv1.GetReadingSeriesResponse, error) {
	if req.GetProjectId() == "" || req.GetWindowStart() == "" || req.GetWindowEnd() == "" {
		return nil, status.Error(codes.InvalidArgument, "project_id, window_start, window_end requeridos")
	}
	if _, ok := query.ValueColumnFor(req.GetSensorType()); !ok {
		return nil, status.Errorf(codes.InvalidArgument, "sensor_type invalido: %q", req.GetSensorType())
	}

	from, err := parseWindow(req.GetWindowStart())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "window_start invalido: %v", err)
	}
	to, err := parseWindow(req.GetWindowEnd())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "window_end invalido: %v", err)
	}

	limit := int(req.GetLimit())
	if limit <= 0 || limit > defaultSeriesLimit {
		limit = defaultSeriesLimit
	}

	series, err := s.repo.ProjectSeries(ctx, req.GetProjectId(), req.GetSensorType(), from, to, limit)
	if err != nil {
		s.log.ErrorContext(ctx, "get_reading_series_failed",
			slog.String("project_id", req.GetProjectId()),
			slog.String("sensor_type", req.GetSensorType()),
			slog.String("error", err.Error()),
		)
		return nil, status.Error(codes.Internal, "error consultando lecturas")
	}

	points := make([]*datamanagementv1.SeriesPoint, 0, len(series.Points))
	for _, p := range series.Points {
		points = append(points, &datamanagementv1.SeriesPoint{
			Timestamp: p.Timestamp.UTC().Format(time.RFC3339),
			Value:     p.Value,
		})
	}

	s.log.InfoContext(ctx, "get_reading_series_ok",
		slog.String("project_id", req.GetProjectId()),
		slog.String("sensor_type", series.SensorType),
		slog.Int("points", len(points)),
	)

	return &datamanagementv1.GetReadingSeriesResponse{
		SensorType:  series.SensorType,
		ValueColumn: series.ValueColumn,
		Points:      points,
	}, nil
}

// parseWindow acepta ISO-8601/RFC3339 y lo normaliza al datetime UTC que espera
// la capa query.
func parseWindow(s string) (string, error) {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return "", err
	}
	return t.UTC().Format(mysqlDatetime), nil
}
