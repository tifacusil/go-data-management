// Package consumer consume envelopes de RabbitMQ, los valida contra el schema,
// los persiste de forma idempotente y aplica retry acotado + DLQ (CLAUDE.md
// 1.3, 2.2, 4). Ack manual: confirma solo tras persistir (CLAUDE.md 2.2).
package consumer

import (
	"context"
	"fmt"
	"log/slog"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Consumer lee de la cola principal y procesa cada mensaje.
type Consumer struct {
	conn      *amqp.Connection
	ch        *amqp.Channel
	processor *processor
	log       *slog.Logger
	queue     string
}

// New conecta a RabbitMQ, declara la topologia y prepara el consumidor.
func New(amqpURL, queue string, v validator, r route, log *slog.Logger) (*Consumer, error) {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return nil, fmt.Errorf("conectando a rabbitmq: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("abriendo canal: %w", err)
	}
	if err := declareTopology(ch); err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("declarando topologia: %w", err)
	}
	// Un mensaje en vuelo por consumidor: backpressure simple y orden de retry.
	if err := ch.Qos(1, 0, false); err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("qos: %w", err)
	}
	return &Consumer{
		conn:      conn,
		ch:        ch,
		processor: newProcessor(v, r, log),
		log:       log,
		queue:     queue,
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
		}
	case outcomeDLQ:
		// nack(requeue=false) -> la cola enruta a la DLX por su x-dead-letter-*.
		// TODO(M4): emitir alerta a monitoring al mandar a dead-letter (CLAUDE.md 4).
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
