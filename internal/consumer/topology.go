package consumer

import amqp "github.com/rabbitmq/amqp091-go"

// Topologia AMQP. El consumidor declara cola principal + DLX + DLQ de forma
// idempotente para garantizar dead-lettering explicito (CLAUDE.md 4).
const (
	QueueMain      = "data_management.persist"
	ExchangeDLX    = "sensors.dlx"
	QueueDeadLetter = "sensors.dead_letter"
	dlRoutingKey   = "dead_letter"
)

// declareTopology declara DLX, DLQ y la cola principal con dead-lettering.
// Idempotente: declarar lo ya existente con los mismos args es no-op.
func declareTopology(ch *amqp.Channel) error {
	// Dead-letter exchange (direct) + cola de dead-letter.
	if err := ch.ExchangeDeclare(ExchangeDLX, "direct", true, false, false, false, nil); err != nil {
		return err
	}
	if _, err := ch.QueueDeclare(QueueDeadLetter, true, false, false, false, nil); err != nil {
		return err
	}
	if err := ch.QueueBind(QueueDeadLetter, dlRoutingKey, ExchangeDLX, false, nil); err != nil {
		return err
	}

	// Cola principal: enruta a la DLX cuando un mensaje es nack(requeue=false).
	args := amqp.Table{
		"x-dead-letter-exchange":    ExchangeDLX,
		"x-dead-letter-routing-key": dlRoutingKey,
	}
	if _, err := ch.QueueDeclare(QueueMain, true, false, false, false, args); err != nil {
		return err
	}
	return nil
}
