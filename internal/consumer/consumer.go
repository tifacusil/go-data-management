// Package consumer consume envelopes de RabbitMQ, los valida contra el schema,
// los persiste de forma idempotente y aplica retry acotado + DLQ (CLAUDE.md
// 1.3, 2.2, 4). Ack manual: confirma solo tras persistir (CLAUDE.md 2.2).
package consumer

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/AngelloFD/go-data-management/internal/observability"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Consumer lee de la cola principal y procesa cada mensaje.
type Consumer struct {
	conn      *amqp.Connection
	ch        *amqp.Channel
	processor *processor
	log       *slog.Logger
	queue     string
	counter   *observability.Counter
}

// New conecta a RabbitMQ, declara la topologia y prepara el consumidor.
// counter cuenta mensajes persistidos con exito (lo expone /health); puede ser nil.
// metrics puede ser nil (tests); si no, el processor registra metricas + spans.
func New(amqpURL, queue string, v validator, r route, log *slog.Logger, counter *observability.Counter, metrics *observability.Metrics) (*Consumer, error) {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return nil, fmt.Errorf("conectando a rabbitmq: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("abriendo canal: %w", err)
	}
	// El servicio NO declara topologia: la cola, DLX y DLQ las pre-declara la
	// infraestructura (tesis-infra rabbitmq/definitions.json) y el usuario del
	// servicio no tiene permiso de configure sobre sensors.dlx / sensors.dead_letter.
	// Solo consume la cola existente (igual que el baseline). El dead-lettering
	// ocurre via x-dead-letter-exchange ya configurado en la cola (CLAUDE.md 4).
	// Un mensaje en vuelo por consumidor: backpressure simple y orden de retry.
	if err := ch.Qos(1, 0, false); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("qos: %w", err)
	}
	proc := newProcessor(v, r, log)
	if metrics != nil {
		proc = proc.withObservability(metrics, observability.Tracer())
	}
	return &Consumer{
		conn:      conn,
		ch:        ch,
		processor: proc,
		log:       log,
		queue:     queue,
		counter:   counter,
	}, nil
}

// Run consume hasta que ctx se cancele. Bloquea.
func (c *Consumer) Run(ctx context.Context) error {
	deliveries, err := c.ch.Consume(c.queue, "", false /*autoAck*/, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}
	c.log.Info("consumiendo", slog.String("queue", c.queue))

	for {
		select {
		case <-ctx.Done():
			c.log.Info("consumer detenido por ctx")
			return nil
		case d, ok := <-deliveries:
			if !ok {
				c.log.Warn("canal de deliveries cerrado")
				return nil
			}
			c.handle(ctx, d)
		}
	}
}

func (c *Consumer) handle(ctx context.Context, d amqp.Delivery) {
	switch c.processor.process(ctx, d.Body) {
	case outcomeAck:
		if err := d.Ack(false); err != nil {
			c.log.Error("ack fallo", slog.String("error", err.Error()))
			return
		}
		if c.counter != nil {
			c.counter.Inc()
		}
	case outcomeDLQ:
		// nack(requeue=false) -> la cola enruta a la DLX por su x-dead-letter-*.
		// Log a nivel error = senal de alerta para monitoring (CLAUDE.md 4).
		// El conteo dm_messages_dlq_total alimenta una regla de alerta Prometheus.
		c.log.Error("mensaje a dead-letter", slog.String("queue", QueueDeadLetter))
		if err := d.Nack(false, false); err != nil {
			c.log.Error("nack fallo", slog.String("error", err.Error()))
		}
	}
}

// Close cierra canal y conexion (drain en shutdown, CLAUDE.md M5).
func (c *Consumer) Close() error {
	if c.ch != nil {
		_ = c.ch.Close()
	}
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
