package consumer

import amqp "github.com/rabbitmq/amqp091-go"

// Nombres de la topologia AMQP. En produccion la declara la infraestructura
// (tesis-infra rabbitmq/definitions.json); el servicio solo consume QueueMain.
// QueueDeadLetter se usa solo para el log de alerta a dead-letter.
const (
	QueueMain       = "data_management.persist"
	ExchangeDLX     = "sensors.dlx"
	QueueDeadLetter = "sensors.dead_letter"
)

// declareTopologyForTest declara cola principal + DLX + DLQ para tests de
// integracion con un broker efimero (sin definitions.json). DEBE coincidir con
// tesis-infra/local/infra/rabbitmq/definitions.json para no divergir:
//   - sensors.dlx: exchange topic durable
//   - data_management.persist: x-dead-letter-exchange=sensors.dlx + ttl/max-length
//   - binding sensors.dlx -> sensors.dead_letter con routing key "#"
func declareTopologyForTest(ch *amqp.Channel) error {
	if err := ch.ExchangeDeclare(ExchangeDLX, "topic", true, false, false, false, nil); err != nil {
		return err
	}
	if _, err := ch.QueueDeclare(QueueDeadLetter, true, false, false, false, nil); err != nil {
		return err
	}
	if err := ch.QueueBind(QueueDeadLetter, "#", ExchangeDLX, false, nil); err != nil {
		return err
	}
	args := amqp.Table{
		"x-dead-letter-exchange": ExchangeDLX,
		"x-message-ttl":          int32(86400000),
		"x-max-length":           int32(1000000),
		"x-queue-type":           "classic",
	}
	if _, err := ch.QueueDeclare(QueueMain, true, false, false, false, args); err != nil {
		return err
	}
	return nil
}
