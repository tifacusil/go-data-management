// Package contract valida envelopes contra el JSON Schema vigente antes de
// procesarlos (CLAUDE.md 1.1: contract-first; 2.2: rechazo de invalidos a DLQ).
package contract

import (
	"bytes"
	"embed"
	"fmt"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// schemaFS embebe el schema para no depender de rutas en runtime.
//
//go:embed envelope.schema.json
var schemaFS embed.FS

// Validator valida bytes JSON de un envelope contra el schema compilado.
type Validator struct {
	schema *jsonschema.Schema
}

// NewValidator compila el schema embebido una vez al boot.
func NewValidator() (*Validator, error) {
	c := jsonschema.NewCompiler()
	data, err := schemaFS.ReadFile("envelope.schema.json")
	if err != nil {
		return nil, fmt.Errorf("leyendo schema embebido: %w", err)
	}
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("parseando schema: %w", err)
	}
	if err := c.AddResource("envelope.schema.json", doc); err != nil {
		return nil, fmt.Errorf("registrando schema: %w", err)
	}
	s, err := c.Compile("envelope.schema.json")
	if err != nil {
		return nil, fmt.Errorf("compilando schema: %w", err)
	}
	return &Validator{schema: s}, nil
}

// Validate devuelve error si los bytes no cumplen el schema. Un fallo aqui es
// permanente: el mensaje es invalido, no reintentar (va a DLQ).
func (v *Validator) Validate(raw []byte) error {
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("json invalido: %w", err)
	}
	if err := v.schema.Validate(doc); err != nil {
		return fmt.Errorf("envelope no valida contra schema: %w", err)
	}
	return nil
}
