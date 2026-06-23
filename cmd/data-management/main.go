// Command data-management es el punto de entrada del microservicio.
// M0: config + logging. M1: persistencia. M2: consumer AMQP idempotente.
// Query API (M3), observabilidad (M4) y Consul (M5) en hitos posteriores.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/AngelloFD/go-data-management/internal/config"
	"github.com/AngelloFD/go-data-management/internal/consumer"
	"github.com/AngelloFD/go-data-management/internal/contract"
	"github.com/AngelloFD/go-data-management/internal/observability"
	"github.com/AngelloFD/go-data-management/internal/persistence"
	"github.com/AngelloFD/go-data-management/internal/router"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	if err := run(); err != nil {
		slog.New(slog.NewJSONHandler(os.Stderr, nil)).
			Error("fallo fatal", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	log := observability.NewLogger(cfg.Service.Name, cfg.Service.InstanceID, cfg.Service.Env)
	log.Info("servicio iniciando",
		slog.Int("http_port", cfg.HTTP.Port),
		slog.String("rabbitmq_queue", cfg.RabbitMQ.Queue),
		slog.Bool("consul_enabled", cfg.Consul.Enabled),
	)

	// DB pool.
	db, err := sql.Open("mysql", cfg.MySQL.DSN)
	if err != nil {
		return fmt.Errorf("abriendo mysql: %w", err)
	}
	defer db.Close()

	repo := persistence.New(db)
	rtr := router.New(repo)

	validator, err := contract.NewValidator()
	if err != nil {
		return fmt.Errorf("validator: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Consumer AMQP idempotente.
	var cons *consumer.Consumer
	if cfg.RabbitMQ.ListenersEnabled {
		amqpURL := fmt.Sprintf("amqp://%s:%s@%s:%d/",
			cfg.RabbitMQ.Username, cfg.RabbitMQ.Password, cfg.RabbitMQ.Host, cfg.RabbitMQ.Port)
		cons, err = consumer.New(amqpURL, cfg.RabbitMQ.Queue, validator, rtr, log)
		if err != nil {
			return fmt.Errorf("consumer: %w", err)
		}
		defer cons.Close()

		go func() {
			if err := cons.Run(ctx); err != nil {
				log.Error("consumer termino con error", slog.String("error", err.Error()))
				stop()
			}
		}()
	} else {
		log.Warn("listeners deshabilitados (RABBITMQ_LISTENERS_ENABLED=false)")
	}

	// TODO(M3): arrancar query API REST + /health.
	// TODO(M4): exponer /metrics + tracing OTel.
	// TODO(M5): registrar en Consul + heartbeat.

	log.Info("servicio listo")
	<-ctx.Done()

	log.Info("shutdown solicitado, cerrando")
	// TODO(M5): deregister Consul, cerrar HTTP server.
	return nil
}
