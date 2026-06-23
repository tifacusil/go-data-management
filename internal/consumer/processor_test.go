package consumer

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/AngelloFD/go-data-management/internal/model"
	"github.com/AngelloFD/go-data-management/internal/persistence"
)

type fakeValidator struct{ err error }

func (f fakeValidator) Validate(_ []byte) error { return f.err }

type fakeRouter struct {
	// errs se consumen en orden por intento; nil = exito.
	errs  []error
	calls int
}

func (f *fakeRouter) Route(_ context.Context, _ *model.Envelope) error {
	var err error
	if f.calls < len(f.errs) {
		err = f.errs[f.calls]
	}
	f.calls++
	return err
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

const validRaw = `{"schema_version":"1.0.0","message_id":"m1","project_id":"demo",
	"sensor_id":"R-1","sensor_type":"rainfall","timestamp":"2026-05-17T00:00:00Z",
	"received_at":"2026-05-17T00:00:01Z","payload":{"rainfall_mm":1.2}}`

func newProc(v validator, r route) *processor {
	p := newProcessor(v, r, quietLogger())
	p.sleep = func(time.Duration) {} // no dormir en tests
	return p
}

func TestProcess_SuccessAcks(t *testing.T) {
	p := newProc(fakeValidator{}, &fakeRouter{})
	if got := p.process(context.Background(), []byte(validRaw)); got != outcomeAck {
		t.Fatalf("esperaba ack, got %v", got)
	}
}

func TestProcess_InvalidSchemaToDLQ(t *testing.T) {
	p := newProc(fakeValidator{err: fmt.Errorf("no valida")}, &fakeRouter{})
	if got := p.process(context.Background(), []byte(validRaw)); got != outcomeDLQ {
		t.Fatalf("invalido debe ir a DLQ, got %v", got)
	}
}

func TestProcess_PermanentErrorToDLQ_NoRetry(t *testing.T) {
	r := &fakeRouter{errs: []error{&persistence.NonRetryableError{Err: fmt.Errorf("tabla mala")}}}
	p := newProc(fakeValidator{}, r)
	if got := p.process(context.Background(), []byte(validRaw)); got != outcomeDLQ {
		t.Fatalf("permanente debe ir a DLQ, got %v", got)
	}
	if r.calls != 1 {
		t.Errorf("permanente no debe reintentar: calls=%d, esperaba 1", r.calls)
	}
}

func TestProcess_TransientThenSuccessAcks(t *testing.T) {
	// 2 transitorios y luego exito (nil) -> ack al 3er intento.
	r := &fakeRouter{errs: []error{
		&persistence.RetryableError{Err: fmt.Errorf("deadlock")},
		&persistence.RetryableError{Err: fmt.Errorf("deadlock")},
		nil,
	}}
	p := newProc(fakeValidator{}, r)
	if got := p.process(context.Background(), []byte(validRaw)); got != outcomeAck {
		t.Fatalf("transitorio recuperado debe acks, got %v", got)
	}
	if r.calls != 3 {
		t.Errorf("esperaba 3 intentos, got %d", r.calls)
	}
}

func TestProcess_TransientExhaustedToDLQ(t *testing.T) {
	// 4 transitorios: 1 inicial + 3 reintentos, todos fallan -> DLQ.
	errs := make([]error, 4)
	for i := range errs {
		errs[i] = &persistence.RetryableError{Err: fmt.Errorf("deadlock")}
	}
	r := &fakeRouter{errs: errs}
	p := newProc(fakeValidator{}, r)
	if got := p.process(context.Background(), []byte(validRaw)); got != outcomeDLQ {
		t.Fatalf("transitorio agotado debe ir a DLQ, got %v", got)
	}
	if r.calls != 4 {
		t.Errorf("esperaba 4 intentos (1+3), got %d", r.calls)
	}
}
