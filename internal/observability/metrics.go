package observability

import (
	"net/http"

	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics agrupa las metricas Prometheus del servicio (CLAUDE.md 5).
// Usa un registry propio (no el global) para evitar panics de doble registro
// y permitir multiples instancias en tests.
type Metrics struct {
	reg *prometheus.Registry

	MessagesProcessed prometheus.Counter   // throughput acumulado (rate => msg/s)
	MessagesDLQ       prometheus.Counter   // a dead-letter
	MessagesInvalid   prometheus.Counter   // rechazo por schema
	MessagesRetried   prometheus.Counter   // reintentos transitorios
	PersistLatency    prometheus.Histogram // latencia persistencia (segundos)

	// GRPCServer instrumenta el servidor gRPC (ReadingService): throughput,
	// latencia por metodo y errores por code. Se expone en el mismo /metrics.
	GRPCServer *grpcprom.ServerMetrics
}

// NewMetrics crea un registry propio con collectors de Go (cpu/mem/goroutines)
// + las metricas del servicio.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	// Collectors por defecto: process_cpu_seconds_total,
	// process_resident_memory_bytes (Linux), go_goroutines, etc.
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	// Metricas del servidor gRPC con histograma de latencia habilitado.
	// Registradas en el registry propio (no el global) => salen por /metrics 8080.
	grpcServer := grpcprom.NewServerMetrics(
		grpcprom.WithServerHandlingTimeHistogram(),
	)
	reg.MustRegister(grpcServer)

	f := promauto.With(reg)
	return &Metrics{
		reg:        reg,
		GRPCServer: grpcServer,
		MessagesProcessed: f.NewCounter(prometheus.CounterOpts{
			Name: "dm_messages_processed_total",
			Help: "Mensajes persistidos con exito.",
		}),
		MessagesDLQ: f.NewCounter(prometheus.CounterOpts{
			Name: "dm_messages_dlq_total",
			Help: "Mensajes enviados a dead-letter.",
		}),
		MessagesInvalid: f.NewCounter(prometheus.CounterOpts{
			Name: "dm_messages_invalid_total",
			Help: "Mensajes rechazados por validacion de schema.",
		}),
		MessagesRetried: f.NewCounter(prometheus.CounterOpts{
			Name: "dm_messages_retried_total",
			Help: "Reintentos por errores transitorios.",
		}),
		// Buckets cubren p50..p99 tipico de un INSERT (ms a cientos de ms).
		// Los percentiles se calculan con histogram_quantile en Prometheus.
		PersistLatency: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "dm_persist_latency_seconds",
			Help:    "Latencia de persistencia de un mensaje.",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		}),
	}
}

// Handler devuelve el http.Handler de /metrics ligado a este registry.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}
