// Package contracts_test verifica que los ejemplos en contracts/examples sigan
// validando contra el JSON Schema vigente del envelope (CLAUDE.md 3.2).
// Es el contract test minimo de M0: si el schema o un ejemplo divergen, falla.
package contracts_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

func TestExamplesValidateAgainstEnvelopeSchema(t *testing.T) {
	compiler := jsonschema.NewCompiler()
	schema, err := compiler.Compile("schemas/envelope.schema.json")
	if err != nil {
		t.Fatalf("compilando schema: %v", err)
	}

	examples, err := filepath.Glob("examples/*.json")
	if err != nil {
		t.Fatalf("listando ejemplos: %v", err)
	}
	if len(examples) != 5 {
		t.Fatalf("esperaba 5 ejemplos (1 por sensor_type), got %d", len(examples))
	}

	for _, path := range examples {
		t.Run(filepath.Base(path), func(t *testing.T) {
			f, err := os.Open(path)
			if err != nil {
				t.Fatalf("abriendo %s: %v", path, err)
			}
			defer f.Close()

			doc, err := jsonschema.UnmarshalJSON(f)
			if err != nil {
				t.Fatalf("parseando %s: %v", path, err)
			}
			if err := schema.Validate(doc); err != nil {
				t.Errorf("%s no valida contra el schema:\n%v", path, err)
			}
		})
	}
}
