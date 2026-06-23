// Package health implementa los checks reales de dependencias para /health
// (CLAUDE.md 1.6): conectividad efectiva a MySQL y RabbitMQ, no 200 estatico.
package health

import (
	"context"
	"database/sql"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Checker agrupa los checks de dependencias.
type Checker struct {
	db        *sql.DB
	rabbitURL string
}

func New(db *sql.DB, rabbitURL string) *Checker {
	return &Checker{db: db, rabbitURL: rabbitURL}
}

// CheckMySQL hace un ping real al pool (SELECT 1 implicito).
func (c *Checker) CheckMySQL(ctx context.Context) error {
	if err := c.db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping mysql: %w", err)
	}
	return nil
}

// CheckRabbitMQ abre una conexion efimera y la cierra: verifica el broker vivo.
func (c *Checker) CheckRabbitMQ(_ context.Context) error {
	conn, err := amqp.Dial(c.rabbitURL)
	if err != nil {
		return fmt.Errorf("dial rabbitmq: %w", err)
	}
	defer conn.Close()
	if conn.IsClosed() {
		return fmt.Errorf("conexion rabbitmq cerrada")
	}
	return nil
}
