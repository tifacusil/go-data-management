// Command data-management es el punto de entrada del microservicio.
// M0: config + logging. M1: persistencia. M2: consumer AMQP idempotente.
// M3: query API REST + /health. Observabilidad (M4) y Consul (M5) despues.
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/AngelloFD/go-data-management/internal/config"
	"github.com/AngelloFD/go-data-management/internal/consul"
	"github.com/AngelloFD/go-data-management/internal/consumer"
	"github.com/AngelloFD/go-data-management/internal/contract"
	"github.com/AngelloFD/go-data-management/internal/health"
	"github.com/AngelloFD/go-data-management/internal/httpapi"
	"github.com/AngelloFD/go-data-management/internal/observability"
	"github.com/AngelloFD/go-data-management/internal/persistence"
	"github.com/AngelloFD/go-data-management/internal/query"
	"github.com/AngelloFD/go-data-management/internal/router"

	_ "github.com/go-sql-driver/mysql"
)

// version es el tag de version para Consul; se sobreescribe en build con
// -ldflags "-X main.version=...".
var version = "0.1.0"

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

	db, err := sql.Open("mysql", cfg.MySQL.DSN)
	if err != nil {
		return fmt.Errorf("abriendo mysql: %w", err)
	}
	defer db.Close()

	amqpURL := fmt.Sprintf("amqp://%s:%s@%s:%d/",
		cfg.RabbitMQ.Username, cfg.RabbitMQ.Password, cfg.RabbitMQ.Host, cfg.RabbitMQ.Port)

	// Tracing OTel (M4): flush al shutdown.
	tracerShutdown, err := observability.InitTracer(context.Background(), cfg.Service.Name, cfg.Service.InstanceID)
	if err != nil {
		return fmt.Errorf("init tracer: %w", err)
	}
	defer func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tracerShutdown(shutCtx)
	}()

	counter := observability.NewCounter()
	metrics := observability.NewMetrics()
	repo := persistence.New(db)
	rtr := router.New(repo)
	queryRepo := query.New(db)
	checker := health.New(db, amqpURL)

	validator, err := contract.NewValidator()
	if err != nil {
		return fmt.Errorf("validator: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Consumer AMQP idempotente (M2).
	var cons *consumer.Consumer
	if cfg.RabbitMQ.ListenersEnabled {
		cons, err = consumer.New(amqpURL, cfg.RabbitMQ.Queue, validator, rtr, log, counter, metrics)
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

	// Query API REST + /health (M3).
	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.HTTP.Port),
		Handler:           httpapi.NewServer(queryRepo, checker, counter, metrics, log).Router(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		log.Info("http escuchando", slog.String("addr", srv.Addr))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server error", slog.String("error", err.Error()))
			stop()
		}
	}()

	// Consul: registro + heartbeat; deregistra al cancelar ctx (M5).
	if cfg.Consul.Enabled {
		reg := consul.New(consul.Config{
			BaseURL:         cfg.Consul.BaseURL,
			Token:           cfg.Consul.Token,
			ServiceName:     cfg.Consul.ServiceName,
			ServiceIP:       cfg.Consul.ServiceIP,
			ServicePort:     cfg.HTTP.Port,
			HealthPath:      cfg.Consul.HealthPath,
			Interval:        cfg.Consul.Interval,
			Heartbeat:       cfg.Consul.Heartbeat,
			DeregisterAfter: cfg.Consul.DeregisterAfter,
			Version:         version,
			Env:             cfg.Service.Env,
		}, log)
		go reg.Run(ctx) // deregistra solo al cerrar ctx
	} else {
		log.Warn("consul deshabilitado (CONSUL_ENABLED=false)")
	}

	log.Info("servicio listo")
	<-ctx.Done()

	log.Info("shutdown solicitado, cerrando")
	// Dar tiempo a que el registrar deregistre antes de salir.
	time.Sleep(500 * time.Millisecond)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Warn("shutdown http", slog.String("error", err.Error()))
	}
	return nil
}
