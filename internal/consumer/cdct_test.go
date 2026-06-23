package consumer_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/AngelloFD/go-data-management/internal/contract"
	"github.com/AngelloFD/go-data-management/internal/model"
	"github.com/AngelloFD/go-data-management/internal/router"
)

// recordingRepo registra que se persistio sin tocar DB.
type recordingRepo struct{ persisted []string }

func (r *recordingRepo) PersistGNSS(_ context.Context, e *model.Envelope, _ *model.GNSSPayload) error {
	r.persisted = append(r.persisted, e.MessageID)
	return nil
}
func (r *recordingRepo) PersistRainfall(_ context.Context, e *model.Envelope, _ *model.RainfallPayload) error {
	r.persisted = append(r.persisted, e.MessageID)
	return nil
}
func (r *recordingRepo) PersistStrain(_ context.Context, e *model.Envelope, _ *model.StrainPayload) error {
	r.persisted = append(r.persisted, e.MessageID)
	return nil
}
func (r *recordingRepo) PersistSoilPressure(_ context.Context, e *model.Envelope, _ *model.SoilPressurePayload) error {
	r.persisted = append(r.persisted, e.MessageID)
	return nil
}
func (r *recordingRepo) PersistDeepDisplacement(_ context.Context, e *model.Envelope, _ *model.DeepDisplacementPayload) error {
	r.persisted = append(r.persisted, e.MessageID)
	return nil
}

// TestCDCT_ConsumerAcceptsProducerExamples es el contract test consumer-driven
// (CLAUDE.md 3.2/3.3) del flujo Data Receiving -> data_management.persist.
//
// El contrato compartido es el JSON Schema del envelope; los mensajes que el
// productor publica son los ejemplos en contracts/examples/. Este test ejecuta
// la ruta REAL del consumidor (validar schema -> parsear Envelope -> rutear a
// persistencia) sobre cada ejemplo y exige que TODOS sean aceptados y
// persistidos. Si el productor cambia un payload sin actualizar el schema, o el
// consumidor deja de aceptar un shape vigente, este test falla.
//
// Nota: sustituye a un message-pact con pact-go, bloqueado en Windows por el
// link CGO de libpact_ffi.
// TODO(SE-practice): migrar a pact-go (message pact + pact broker) cuando el
// toolchain CGO/FFI este disponible en CI Linux — ref CLAUDE.md 3.3.
func TestCDCT_ConsumerAcceptsProducerExamples(t *testing.T) {
	validator, err := contract.NewValidator()
	if err != nil {
		t.Fatalf("validator: %v", err)
	}
	repo := &recordingRepo{}
	rtr := router.New(repo)

	examples, err := filepath.Glob(filepath.Join("..", "..", "contracts", "examples", "*.json"))
	if err != nil {
		t.Fatalf("glob ejemplos: %v", err)
	}
	if len(examples) != 5 {
		t.Fatalf("esperaba 5 ejemplos del productor, got %d", len(examples))
	}

	for _, path := range examples {
		t.Run(filepath.Base(path), func(t *testing.T) {
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("leer %s: %v", path, err)
			}
			// 1. El consumidor valida contra el contrato (schema).
			if err := validator.Validate(raw); err != nil {
				t.Fatalf("consumidor rechaza ejemplo del productor (schema): %v", err)
			}
			// 2. El consumidor parsea y rutea (ruta real de produccion).
			var e model.Envelope
			if err := json.Unmarshal(raw, &e); err != nil {
				t.Fatalf("parse envelope: %v", err)
			}
			if err := rtr.Route(context.Background(), &e); err != nil {
				t.Fatalf("consumidor no pudo procesar ejemplo: %v", err)
			}
		})
	}

	if len(repo.persisted) != 5 {
		t.Errorf("esperaba 5 mensajes persistidos, got %d", len(repo.persisted))
	}
}
