package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/AngelloFD/go-data-management/internal/model"
	"github.com/AngelloFD/go-data-management/internal/observability"
	"github.com/AngelloFD/go-data-management/internal/persistence"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// backoffSchedule define el retry acotado para errores transitorios:
// 3 intentos totales, esperas 1s/5s/30s entre ellos (CLAUDE.md 4).
var backoffSchedule = []time.Duration{1 * time.Second, 5 * time.Second, 30 * time.Second}

// outcome es la decision de procesamiento de un mensaje.
type outcome int

const (
	outcomeAck outcome = iota // persistido (o duplicado no-op) -> ack
	outcomeDLQ                // invalido o transitorio agotado -> nack a DLQ
)

// validator valida bytes crudos contra el JSON Schema.
type validator interface {
	Validate(raw []byte) error
}

// route despacha un envelope ya parseado al persister.
type route interface {
	Route(ctx context.Context, e *model.Envelope) error
}

// processor encapsula la logica pura: validar -> parsear -> rutear con retry.
// Sin dependencia de AMQP para poder testearlo aislado.
type processor struct {
	validator validator
	router    route
	log       *slog.Logger
	metrics   *observability.Metrics // puede ser nil (tests unit)
	tracer    trace.Tracer           // puede ser nil (tests unit)
	sleep     func(time.Duration)    // inyectable para tests (no dormir de verdad)
}

func newProcessor(v validator, r route, log *slog.Logger) *processor {
	return &processor{validator: v, router: r, log: log, sleep: time.Sleep}
}

// withObservability adjunta metricas + tracer (lo llama el consumer real).
func (p *processor) withObservability(m *observability.Metrics, t trace.Tracer) *processor {
	p.metrics = m
	p.tracer = t
	return p
}

// process valida, parsea y persiste un mensaje crudo. Reintenta solo errores
// transitorios (backoff acotado). Devuelve la decision de ack/DLQ.
func (p *processor) process(ctx context.Context, raw []byte) outcome {
	// Validacion de contrato: invalido es permanente -> DLQ (CLAUDE.md 2.2).
	if err := p.validator.Validate(raw); err != nil {
		p.log.Warn("envelope invalido, a DLQ", slog.String("error", err.Error()))
		p.incInvalid()
		p.incDLQ()
		return outcomeDLQ
	}

	var e model.Envelope
	if err := json.Unmarshal(raw, &e); err != nil {
		p.log.Warn("parse envelope fallo, a DLQ", slog.String("error", err.Error()))
		p.incInvalid()
		p.incDLQ()
		return outcomeDLQ
	}

	// Correlation id = message_id en todos los logs del flujo (CLAUDE.md 1.8).
	ctx = observability.WithCorrelation(ctx, e.MessageID)

	// Span del flujo consume->persist; message_id como atributo para correlar
	// con el resto del pipeline (CLAUDE.md 5).
	if p.tracer != nil {
		var span trace.Span
		ctx, span = p.tracer.Start(ctx, "process_message",
			trace.WithAttributes(
				attribute.String("message_id", e.MessageID),
				attribute.String("sensor_type", string(e.SensorType)),
			))
		defer span.End()
		out := p.persistWithRetry(ctx, &e)
		if out == outcomeDLQ {
			span.SetStatus(codes.Error, "a DLQ")
		}
		return out
	}
	return p.persistWithRetry(ctx, &e)
}

// persistWithRetry intenta persistir con retry acotado y registra metricas.
func (p *processor) persistWithRetry(ctx context.Context, e *model.Envelope) outcome {
	log := observability.LoggerFrom(ctx, p.log).With(
		slog.String("sensor_type", string(e.SensorType)),
		slog.String("sensor_id", e.SensorID),
	)

	maxAttempts := len(backoffSchedule) + 1
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		start := time.Now()
		err := p.router.Route(ctx, e)
		p.observeLatency(time.Since(start))

		if err == nil {
			log.Info("persistido", slog.Int("attempt", attempt))
			p.incProcessed()
			return outcomeAck
		}

		var nonRetry *persistence.NonRetryableError
		if errors.As(err, &nonRetry) {
			log.Warn("error permanente, a DLQ", slog.String("error", err.Error()))
			p.incDLQ()
			return outcomeDLQ
		}

		var retry *persistence.RetryableError
		if !errors.As(err, &retry) {
			log.Warn("error no clasificado, a DLQ", slog.String("error", err.Error()))
			p.incDLQ()
			return outcomeDLQ
		}

		if attempt < maxAttempts {
			wait := backoffSchedule[attempt-1]
			log.Warn("error transitorio, reintentando",
				slog.Int("attempt", attempt),
				slog.Duration("backoff", wait),
				slog.String("error", err.Error()),
			)
			p.incRetried()
			select {
			case <-ctx.Done():
				log.Warn("ctx cancelado durante backoff, a DLQ")
				p.incDLQ()
				return outcomeDLQ
			default:
			}
			p.sleep(wait)
			continue
		}
		log.Error("reintentos agotados, a DLQ", slog.String("error", err.Error()))
		p.incDLQ()
		return outcomeDLQ
	}
	p.incDLQ()
	return outcomeDLQ
}

// Helpers nil-safe de metricas.
func (p *processor) incProcessed() {
	if p.metrics != nil {
		p.metrics.MessagesProcessed.Inc()
	}
}
func (p *processor) incDLQ() {
	if p.metrics != nil {
		p.metrics.MessagesDLQ.Inc()
	}
}
func (p *processor) incInvalid() {
	if p.metrics != nil {
		p.metrics.MessagesInvalid.Inc()
	}
}
func (p *processor) incRetried() {
	if p.metrics != nil {
		p.metrics.MessagesRetried.Inc()
	}
}
func (p *processor) observeLatency(d time.Duration) {
	if p.metrics != nil {
		p.metrics.PersistLatency.Observe(d.Seconds())
	}
}
