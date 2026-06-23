package httpapi

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/AngelloFD/go-data-management/internal/query"
)

// TestHTTPContract_ResponseShapes verifica que las respuestas de la query API
// cumplan la forma del contrato OpenAPI (contracts/openapi/data-management.yaml):
// PagedReadings {total, offset, limit, items[]}, Reading {project_id, sensor_id,
// sensor_type, timestamp, received_at, payload}, Health {status, mysql, rabbitmq,
// messages_processed}, Error {error}. Es el lado provider del contrato HTTP
// (CLAUDE.md 3.2). Sin pact broker; valida la forma directamente.
//
// TODO(SE-practice): elevar a consumer-driven HTTP pact (pact-go provider
// verification) cuando el toolchain FFI este disponible — ref CLAUDE.md 3.3.
func TestHTTPContract_PagedReadingsShape(t *testing.T) {
	fq := fakeQuery{paged: query.Paged{
		Total: 1, Offset: 0, Limit: 200,
		Items: []query.Reading{{
			ProjectID: "P-1", SensorID: "S-1", SensorType: "rainfall",
			Payload: json.RawMessage(`{"rainfall_mm":1.2}`),
		}},
	}}
	h := newTestServer(fq, fakeHealth{})
	rec := do(t, h, "/api/v1/sensors/S-1/readings")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}

	var body map[string]json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("json: %v", err)
	}
	for _, k := range []string{"total", "offset", "limit", "items"} {
		if _, ok := body[k]; !ok {
			t.Errorf("PagedReadings falta campo %q (contrato OpenAPI)", k)
		}
	}

	var items []map[string]json.RawMessage
	json.Unmarshal(body["items"], &items)
	if len(items) != 1 {
		t.Fatalf("esperaba 1 item")
	}
	for _, k := range []string{"project_id", "sensor_id", "sensor_type", "timestamp", "received_at", "payload"} {
		if _, ok := items[0][k]; !ok {
			t.Errorf("Reading falta campo %q (contrato OpenAPI)", k)
		}
	}
}

func TestHTTPContract_HealthShape(t *testing.T) {
	h := newTestServer(fakeQuery{}, fakeHealth{})
	rec := do(t, h, "/health")
	var body map[string]json.RawMessage
	json.Unmarshal(rec.Body.Bytes(), &body)
	for _, k := range []string{"status", "mysql", "rabbitmq", "messages_processed"} {
		if _, ok := body[k]; !ok {
			t.Errorf("Health falta campo %q (contrato OpenAPI)", k)
		}
	}
}

func TestHTTPContract_ErrorShape(t *testing.T) {
	h := newTestServer(fakeQuery{}, fakeHealth{})
	rec := do(t, h, "/api/v1/sensors/S-1/readings?limit=0")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("esperaba 400, got %d", rec.Code)
	}
	var body map[string]json.RawMessage
	json.Unmarshal(rec.Body.Bytes(), &body)
	if _, ok := body["error"]; !ok {
		t.Error("Error falta campo \"error\" (contrato OpenAPI)")
	}
}
