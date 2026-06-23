package httpapi

import (
	"net/http"
	"strings"
	"testing"

	"github.com/AngelloFD/go-data-management/internal/observability"
)

// TestMetricsEndpoint verifica que /metrics exponga las metricas registradas
// y los collectors por defecto de Go (cpu/mem) en formato Prometheus.
func TestMetricsEndpoint(t *testing.T) {
	// Registrar metricas del servicio (mueve un contador para que aparezca).
	m := observability.NewMetrics()
	m.MessagesProcessed.Inc()
	m.MessagesDLQ.Inc()

	h := newTestServerWithMetrics(fakeQuery{}, fakeHealth{}, m)
	rec := do(t, h, "/metrics")
	if rec.Code != http.StatusOK {
		t.Fatalf("esperaba 200, got %d", rec.Code)
	}
	body := rec.Body.String()

	want := []string{
		"dm_messages_processed_total",
		"dm_messages_dlq_total",
		"dm_persist_latency_seconds",
		"go_goroutines", // collector por defecto
	}
	for _, w := range want {
		if !strings.Contains(body, w) {
			t.Errorf("/metrics no expone %q", w)
		}
	}
}
