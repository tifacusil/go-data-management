// Package router decodifica el payload segun sensor_type y lo manda al repo.
// Espejo de MessageRouter del baseline Java.
package router

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/AngelloFD/go-data-management/internal/model"
	"github.com/AngelloFD/go-data-management/internal/persistence"
)

// Persister es lo que el router necesita del repo (interfaz para testear).
type Persister interface {
	PersistGNSS(ctx context.Context, e *model.Envelope, p *model.GNSSPayload) error
	PersistRainfall(ctx context.Context, e *model.Envelope, p *model.RainfallPayload) error
	PersistStrain(ctx context.Context, e *model.Envelope, p *model.StrainPayload) error
	PersistSoilPressure(ctx context.Context, e *model.Envelope, p *model.SoilPressurePayload) error
	PersistDeepDisplacement(ctx context.Context, e *model.Envelope, p *model.DeepDisplacementPayload) error
}

type Router struct {
	repo Persister
}

func New(repo Persister) *Router {
	return &Router{repo: repo}
}

// Route despacha el envelope al persister correcto. Errores de decodificacion
// son permanentes (NonRetryableError); errores de DB ya vienen clasificados
// del repo (CLAUDE.md 2.2).
func (r *Router) Route(ctx context.Context, e *model.Envelope) error {
	if e == nil {
		return &persistence.NonRetryableError{Err: fmt.Errorf("envelope nulo")}
	}
	if !e.SensorType.Valid() {
		return &persistence.NonRetryableError{Err: fmt.Errorf("sensor_type no soportado: %q", e.SensorType)}
	}

	switch e.SensorType {
	case model.SensorGNSS:
		var p model.GNSSPayload
		if err := decode(e.Payload, &p); err != nil {
			return err
		}
		return r.repo.PersistGNSS(ctx, e, &p)
	case model.SensorRainfall:
		var p model.RainfallPayload
		if err := decode(e.Payload, &p); err != nil {
			return err
		}
		return r.repo.PersistRainfall(ctx, e, &p)
	case model.SensorStrain:
		var p model.StrainPayload
		if err := decode(e.Payload, &p); err != nil {
			return err
		}
		return r.repo.PersistStrain(ctx, e, &p)
	case model.SensorSoilPressure:
		var p model.SoilPressurePayload
		if err := decode(e.Payload, &p); err != nil {
			return err
		}
		return r.repo.PersistSoilPressure(ctx, e, &p)
	case model.SensorDeepDisplacement:
		var p model.DeepDisplacementPayload
		if err := decode(e.Payload, &p); err != nil {
			return err
		}
		return r.repo.PersistDeepDisplacement(ctx, e, &p)
	default:
		return &persistence.NonRetryableError{Err: fmt.Errorf("sensor_type no soportado: %q", e.SensorType)}
	}
}

func decode(raw []byte, v any) error {
	if err := json.Unmarshal(raw, v); err != nil {
		return &persistence.NonRetryableError{Err: fmt.Errorf("payload invalido: %w", err)}
	}
	return nil
}
