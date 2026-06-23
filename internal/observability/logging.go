// Package observability provee logging estructurado JSON, metricas y tracing.
// M0 cubre solo logging; metricas (M4) y tracing (M4) se anaden despues.
package observability

import (
	"context"
	"log/slog"
	"os"
)

// correlationKey es el tipo de clave de contexto para el correlation id
// (message_id / trace_id) que sigue un mensaje por todo el flujo (CLAUDE.md 1.8).
type correlationKey struct{}

// NewLogger crea un slog.Logger JSON con campos base service/instance/env.
// Todo log del servicio deriva de este (CLAUDE.md 5).
func NewLogger(service, instanceID, env string) *slog.Logger {
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	return slog.New(h).With(
		slog.String("service", service),
		slog.String("instance_id", instanceID),
		slog.String("env", env),
	)
}

// WithCorrelation guarda el correlation id en el contexto.
func WithCorrelation(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationKey{}, id)
}

// Correlation extrae el correlation id del contexto, "" si no hay.
func Correlation(ctx context.Context) string {
	if v, ok := ctx.Value(correlationKey{}).(string); ok {
		return v
	}
	return ""
}

// LoggerFrom devuelve el logger base con el correlation id del contexto
// adjuntado como campo trace_id, para trazabilidad por mensaje.
func LoggerFrom(ctx context.Context, base *slog.Logger) *slog.Logger {
	if id := Correlation(ctx); id != "" {
		return base.With(slog.String("trace_id", id))
	}
	return base
}
