package contract

import (
	"os"
	"testing"
)

// TestEmbeddedSchemaMatchesCanonical evita drift entre el schema embebido y el
// canonico en contracts/schemas/ (fuente de verdad del contrato).
func TestEmbeddedSchemaMatchesCanonical(t *testing.T) {
	embedded, err := schemaFS.ReadFile("envelope.schema.json")
	if err != nil {
		t.Fatalf("leyendo embebido: %v", err)
	}
	canonical, err := os.ReadFile("../../contracts/schemas/envelope.schema.json")
	if err != nil {
		t.Fatalf("leyendo canonico: %v", err)
	}
	if string(embedded) != string(canonical) {
		t.Fatal("schema embebido difiere del canonico en contracts/schemas/; re-copiar")
	}
}

func TestValidate(t *testing.T) {
	v, err := NewValidator()
	if err != nil {
		t.Fatalf("compilando: %v", err)
	}

	valid := `{
		"schema_version":"1.0.0","message_id":"m1","project_id":"demo",
		"sensor_id":"R-1","sensor_type":"rainfall",
		"timestamp":"2026-05-17T00:00:00Z","received_at":"2026-05-17T00:00:01Z",
		"payload":{"rainfall_mm":1.2}
	}`
	if err := v.Validate([]byte(valid)); err != nil {
		t.Errorf("valido rechazado: %v", err)
	}

	tests := map[string]string{
		"json roto":           `{not json`,
		"sensor_type malo":    `{"schema_version":"1","message_id":"m","project_id":"p","sensor_id":"s","sensor_type":"lava","timestamp":"2026-05-17T00:00:00Z","received_at":"2026-05-17T00:00:00Z","payload":{}}`,
		"falta message_id":    `{"schema_version":"1","project_id":"p","sensor_id":"s","sensor_type":"rainfall","timestamp":"2026-05-17T00:00:00Z","received_at":"2026-05-17T00:00:00Z","payload":{}}`,
		"payload campo extra": `{"schema_version":"1","message_id":"m","project_id":"p","sensor_id":"s","sensor_type":"rainfall","timestamp":"2026-05-17T00:00:00Z","received_at":"2026-05-17T00:00:00Z","payload":{"foo":1}}`,
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			if err := v.Validate([]byte(raw)); err == nil {
				t.Errorf("%s: esperaba error, got nil", name)
			}
		})
	}
}
