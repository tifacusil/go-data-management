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
	sleep     func(time.Duration) // inyectable para tests (no dormir de verdad)
}

func newProcessor(v validator, r route, log *slog.Logger) *processor {
	return &processor{validator: v, router: r, log: log, sleep: time.Sleep}
}

// process valida, parsea y persiste un mensaje crudo. Reintenta solo errores
// transitorios (backoff acotado). Devuelve la decision de ack/DLQ.
func (p *processor) process(ctx context.Context, raw []byte) outcome {
	// Validacion de contrato: invalido es permanente -> DLQ (CLAUDE.md 2.2).
	if err := p.validator.Validate(raw); err != nil {
		p.log.Warn("envelope invalido, a DLQ", slog.String("error", err.Error()))
		return outcomeDLQ
	}

	var e model.Envelope
	if err := json.Unmarshal(raw, &e); err != nil {
		p.log.Warn("parse envelope fallo, a DLQ", slog.String("error", err.Error()))
		return outcomeDLQ
	}

	// Correlation id = message_id en todos los logs del flujo (CLAUDE.md 1.8).
	ctx = observability.WithCorrelation(ctx, e.MessageID)
	log := observability.LoggerFrom(ctx, p.log).With(
		slog.String("sensor_type", string(e.SensorType)),
		slog.String("sensor_id", e.SensorID),
	)

	// Intentos: 1 inicial + reintentos por cada slot de backoff.
	maxAttempts := len(backoffSchedule) + 1
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err := p.router.Route(ctx, &e)
		if err == nil {
			log.Info("persistido", slog.Int("attempt", attempt))
			return outcomeAck
		}

		var nonRetry *persistence.NonRetryableError
		if errors.As(err, &nonRetry) {
			log.Warn("error permanente, a DLQ", slog.String("error", err.Error()))
			return outcomeDLQ
		}

		var retry *persistence.RetryableError
		if !errors.As(err, &retry) {
			// Desconocido: tratar como permanente para no reintentar en bucle.
			log.Warn("error no clasificado, a DLQ", slog.String("error", err.Error()))
			return outcomeDLQ
		}

		// Transitorio: backoff y reintentar si quedan intentos.
		if attempt < maxAttempts {
			wait := backoffSchedule[attempt-1]
			log.Warn("error transitorio, reintentando",
				slog.Int("attempt", attempt),
				slog.Duration("backoff", wait),
				slog.String("error", err.Error()),
			)
			select {
			case <-ctx.Done():
				log.Warn("ctx cancelado durante backoff, a DLQ")
				return outcomeDLQ
			default:
			}
			p.sleep(wait)
			continue
		}
		log.Error("reintentos agotados, a DLQ", slog.String("error", err.Error()))
		return outcomeDLQ
	}
	return outcomeDLQ
}
