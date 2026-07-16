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
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/AngelloFD/go-data-management/internal/config"
	"github.com/AngelloFD/go-data-management/internal/consumer"
	"github.com/AngelloFD/go-data-management/internal/contract"
	"github.com/AngelloFD/go-data-management/internal/grpcapi"
	datamanagementv1 "github.com/AngelloFD/go-data-management/internal/grpcapi/gen/datamanagement/v1"
	"github.com/AngelloFD/go-data-management/internal/health"
	"github.com/AngelloFD/go-data-management/internal/httpapi"
	"github.com/AngelloFD/go-data-management/internal/observability"
	"github.com/AngelloFD/go-data-management/internal/persistence"
	"github.com/AngelloFD/go-data-management/internal/query"
	"github.com/AngelloFD/go-data-management/internal/router"

	_ "github.com/go-sql-driver/mysql"
	"google.golang.org/grpc"
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
		slog.Int("grpc_port", cfg.GRPC.Port),
		slog.String("rabbitmq_queue", cfg.RabbitMQ.Queue),
	)

	db, err := sql.Open("mysql", cfg.MySQL.DSN)
	if err != nil {
		return fmt.Errorf("abriendo mysql: %w", err)
	}
	defer func() { _ = db.Close() }()

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

	// Consumer AMQP idempotente (M2), con reconexion automatica: si el broker
	// cae, el supervisor reconecta con backoff en vez de dejar el consumer muerto
	// hasta un reinicio manual del pod (tolerancia a fallos, CLAUDE.md 2.1/2.2).
	if cfg.RabbitMQ.ListenersEnabled {
		go func() {
			err := consumer.RunWithReconnect(ctx, consumer.DialConfig{
				URL:       amqpURL,
				Queue:     cfg.RabbitMQ.Queue,
				Validator: validator,
				Route:     rtr,
				Log:       log,
				Counter:   counter,
				Metrics:   metrics,
			})
			if err != nil && !errors.Is(err, context.Canceled) {
				log.Error("consumer supervisor termino con error", slog.String("error", err.Error()))
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

	// gRPC: ReadingService para consumo sincrono de data-analysis (reemplaza su
	// acceso directo a las tablas, CLAUDE.md §1.2). Interceptor Prometheus =>
	// metricas grpc_server_* en el mismo /metrics 8080 (CLAUDE.md §5).
	grpcSrv := grpc.NewServer(
		grpc.UnaryInterceptor(metrics.GRPCServer.UnaryServerInterceptor()),
	)
	datamanagementv1.RegisterReadingServiceServer(grpcSrv, grpcapi.NewReadingServer(queryRepo, log))
	// Preinicializa las series por metodo a 0 (evita gaps hasta la 1a llamada).
	metrics.GRPCServer.InitializeMetrics(grpcSrv)
	grpcLis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.GRPC.Port))
	if err != nil {
		return fmt.Errorf("grpc listen: %w", err)
	}
	go func() {
		log.Info("grpc escuchando", slog.String("addr", grpcLis.Addr().String()))
		if err := grpcSrv.Serve(grpcLis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			log.Error("grpc server error", slog.String("error", err.Error()))
			stop()
		}
	}()

	// Service discovery: DNS de Kubernetes (nombre de Service). Sin Consul: los
	// consumidores (data-analysis) resuelven data-management por DNS del cluster.

	log.Info("servicio listo")
	<-ctx.Done()

	log.Info("shutdown solicitado, cerrando")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Warn("shutdown http", slog.String("error", err.Error()))
	}
	grpcSrv.GracefulStop()
	return nil
}
