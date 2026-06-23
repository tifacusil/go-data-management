package consumer

import amqp "github.com/rabbitmq/amqp091-go"

// DeclareTopologyForTest expone declareTopologyForTest a los tests de
// integracion del paquete consumer_test (broker efimero sin definitions.json).
func DeclareTopologyForTest(ch *amqp.Channel) error {
	return declareTopologyForTest(ch)
}
