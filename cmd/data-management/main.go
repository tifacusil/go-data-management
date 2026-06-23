// Command data-management es el punto de entrada del microservicio.
// M0: carga config validada, inicializa logging JSON, arranca y queda a la
// espera de SIGTERM/SIGINT para shutdown limpio. Consumer (M2), query API (M3),
// observabilidad (M4) y Consul (M5) se cablean en hitos posteriores.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/AngelloFD/go-data-management/internal/config"
	"github.com/AngelloFD/go-data-management/internal/observability"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		// Sin logger todavia: error de boot va a stderr y corta.
		slog.New(slog.NewJSONHandler(os.Stderr, nil)).
			Error("fallo cargando config", slog.String("error", err.Error()))
		os.Exit(1)
	}

	log := observability.NewLogger(cfg.Service.Name, cfg.Service.InstanceID, cfg.Service.Env)
	log.Info("servicio iniciando",
		slog.Int("http_port", cfg.HTTP.Port),
		slog.String("rabbitmq_queue", cfg.RabbitMQ.Queue),
		slog.Bool("consul_enabled", cfg.Consul.Enabled),
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// TODO(M2): arrancar consumer AMQP idempotente.
	// TODO(M3): arrancar query API REST + /health.
	// TODO(M4): exponer /metrics + tracing OTel.
	// TODO(M5): registrar en Consul + heartbeat.

	log.Info("servicio listo")
	<-ctx.Done()

	log.Info("shutdown solicitado, cerrando")
	// TODO(M5): drain consumer, deregister Consul, cerrar pools.
}
