package observability

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// InitTracer configura el tracer global con exporter stdout (sin collector
// externo; suficiente para el flujo Data Receiving -> RabbitMQ -> Data
// Management de la tesis, CLAUDE.md 5). Devuelve shutdown para flush.
func InitTracer(ctx context.Context, service, instanceID string) (func(context.Context) error, error) {
	exp, err := stdouttrace.New(stdouttrace.WithoutTimestamps())
	if err != nil {
		return nil, err
	}
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(service),
			semconv.ServiceInstanceID(instanceID),
		),
	)
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

// Tracer devuelve el tracer del servicio.
func Tracer() trace.Tracer {
	return otel.Tracer("data-management")
}
