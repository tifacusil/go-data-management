// Package consumer consume envelopes de RabbitMQ, los valida contra el schema,
// los persiste de forma idempotente y aplica retry acotado + DLQ (CLAUDE.md
// 1.3, 2.2, 4). Ack manual: confirma solo tras persistir (CLAUDE.md 2.2).
package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/AngelloFD/go-data-management/internal/observability"
	amqp "github.com/rabbitmq/amqp091-go"
)

// ErrConnClosed indica que la conexion al broker se cerro (RabbitMQ caido, red).
// El supervisor (RunWithReconnect) lo trata como senal para reconectar, no como
// fin normal: antes Run retornaba nil en este caso y el consumer quedaba muerto
// con el proceso vivo (pod Ready pero sin consumir) hasta un reinicio manual.
var ErrConnClosed = errors.New("conexion rabbitmq cerrada")

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
	// NotifyClose dispara si la conexion cae aunque el canal de deliveries no se
	// cierre limpio; junto al ramo !ok de abajo garantiza que una caida del
	// broker haga retornar ErrConnClosed (no nil) para que el supervisor reconecte.
	closeCh := c.conn.NotifyClose(make(chan *amqp.Error, 1))
	c.log.Info("consumiendo", slog.String("queue", c.queue))

	for {
		select {
		case <-ctx.Done():
			c.log.Info("consumer detenido por ctx")
			return nil
		case amqpErr := <-closeCh:
			if amqpErr == nil {
				return ErrConnClosed
			}
			return fmt.Errorf("%w: %v", ErrConnClosed, amqpErr)
		case d, ok := <-deliveries:
			if !ok {
				c.log.Warn("canal de deliveries cerrado")
				return ErrConnClosed
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

// DialConfig junta lo necesario para (re)conectar y consumir. Se usa con
// RunWithReconnect para sobrevivir caidas del broker sin reiniciar el proceso.
type DialConfig struct {
	URL       string
	Queue     string
	Validator validator
	Route     route
	Log       *slog.Logger
	Counter   *observability.Counter
	Metrics   *observability.Metrics
}

// RunWithReconnect consume para siempre, reconectando con backoff exponencial
// (cap 30s) cada vez que la conexion al broker se pierde. Solo retorna cuando
// ctx se cancela. Cierra el gap de resiliencia medido con chaos/kill-rabbitmq:
// matar RabbitMQ ya no deja el consumer muerto hasta un reinicio manual del pod
// (tolerancia a fallos, CLAUDE.md 2.1/2.2). El dial inicial tambien reintenta,
// asi que el servicio arranca aunque el broker aun no este listo.
func RunWithReconnect(ctx context.Context, cfg DialConfig) error {
	const (
		baseBackoff = 1 * time.Second
		maxBackoff  = 30 * time.Second
	)
	backoff := baseBackoff
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		c, err := New(cfg.URL, cfg.Queue, cfg.Validator, cfg.Route, cfg.Log, cfg.Counter, cfg.Metrics)
		if err != nil {
			cfg.Log.Warn("rabbitmq_conectar_fallo_reintentando",
				slog.String("error", err.Error()), slog.Duration("backoff", backoff))
			if !sleepCtx(ctx, backoff) {
				return ctx.Err()
			}
			backoff = nextBackoff(backoff, maxBackoff)
			continue
		}
		cfg.Log.Info("rabbitmq_conectado")
		backoff = baseBackoff // la sesion vivio: la proxima caida reconecta rapido
		runErr := c.Run(ctx)
		_ = c.Close()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		cfg.Log.Warn("consumer_conexion_perdida_reconectando",
			slog.String("error", errString(runErr)), slog.Duration("backoff", backoff))
		if !sleepCtx(ctx, backoff) {
			return ctx.Err()
		}
		backoff = nextBackoff(backoff, maxBackoff)
	}
}

func nextBackoff(cur, max time.Duration) time.Duration {
	if n := cur * 2; n < max {
		return n
	}
	return max
}

// sleepCtx duerme d, o retorna false si ctx se cancela antes.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func errString(err error) string {
	if err == nil {
		return "(nil)"
	}
	return err.Error()
}
