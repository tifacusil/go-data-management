package consumer

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/AngelloFD/go-data-management/internal/observability"
	"github.com/AngelloFD/go-data-management/internal/persistence"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestProcessor_MetricsMove verifica que los contadores se muevan segun el
// resultado: exito -> processed; invalido -> invalid+dlq.
func TestProcessor_MetricsMove(t *testing.T) {
	m := observability.NewMetrics()
	p := newProcessor(fakeValidator{}, &fakeRouter{}, quietLogger()).withObservability(m, nil)
	p.sleep = func(_ time.Duration) {}

	if got := p.process(context.Background(), []byte(validRaw)); got != outcomeAck {
		t.Fatalf("esperaba ack, got %v", got)
	}
	if v := testutil.ToFloat64(m.MessagesProcessed); v != 1 {
		t.Errorf("processed=%v, esperaba 1", v)
	}

	// Invalido: invalid + dlq suben.
	pInvalid := newProcessor(fakeValidator{err: fmt.Errorf("no valida")}, &fakeRouter{}, quietLogger()).withObservability(m, nil)
	pInvalid.process(context.Background(), []byte(validRaw))
	if v := testutil.ToFloat64(m.MessagesInvalid); v != 1 {
		t.Errorf("invalid=%v, esperaba 1", v)
	}
	if v := testutil.ToFloat64(m.MessagesDLQ); v != 1 {
		t.Errorf("dlq=%v, esperaba 1", v)
	}
}

// TestProcessor_RetryMetric verifica que los reintentos transitorios cuenten.
func TestProcessor_RetryMetric(t *testing.T) {
	m := observability.NewMetrics()
	r := &fakeRouter{errs: []error{
		&persistence.RetryableError{Err: fmt.Errorf("deadlock")},
		nil,
	}}
	p := newProcessor(fakeValidator{}, r, quietLogger()).withObservability(m, nil)
	p.sleep = func(_ time.Duration) {}

	p.process(context.Background(), []byte(validRaw))
	if v := testutil.ToFloat64(m.MessagesRetried); v != 1 {
		t.Errorf("retried=%v, esperaba 1", v)
	}
}
