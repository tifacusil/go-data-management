// Package model define el envelope de RabbitMQ y los payloads por sensor_type.
// Espejo del contrato contracts/schemas/envelope.schema.json (CLAUDE.md 1.1).
// Punteros en los campos numericos: distinguen "ausente" de "cero".
package model

import (
	"encoding/json"
	"time"
)

// SensorType enumera los tipos soportados. Debe coincidir con el enum del schema.
type SensorType string

const (
	SensorGNSS             SensorType = "gnss"
	SensorRainfall         SensorType = "rainfall"
	SensorStrain           SensorType = "strain"
	SensorSoilPressure     SensorType = "soil_pressure"
	SensorDeepDisplacement SensorType = "deep_displacement"
)

// Valid indica si el sensor_type es uno de los soportados.
func (s SensorType) Valid() bool {
	switch s {
	case SensorGNSS, SensorRainfall, SensorStrain, SensorSoilPressure, SensorDeepDisplacement:
		return true
	default:
		return false
	}
}

// Envelope es el mensaje crudo recibido de la cola data_management.persist.
// payload queda como json.RawMessage para deserializarlo segun sensor_type (M2).
type Envelope struct {
	SchemaVersion           string          `json:"schema_version"`
	MessageID               string          `json:"message_id"`
	ProjectID               string          `json:"project_id"`
	SensorID                string          `json:"sensor_id"`
	SensorType              SensorType      `json:"sensor_type"`
	Timestamp               time.Time       `json:"timestamp"`
	ReceivedAt              time.Time       `json:"received_at"`
	SamplingIntervalSeconds *int            `json:"sampling_interval_seconds,omitempty"`
	Payload                 json.RawMessage `json:"payload"`
}

type GNSSPayload struct {
	DisplacementXmm          *float64 `json:"displacement_x_mm,omitempty"`
	DisplacementYmm          *float64 `json:"displacement_y_mm,omitempty"`
	DisplacementZmm          *float64 `json:"displacement_z_mm,omitempty"`
	DisplacementHorizontalMm *float64 `json:"displacement_horizontal_mm,omitempty"`
	CumulativeDisplacementMm *float64 `json:"cumulative_displacement_mm,omitempty"`
	FixQuality               *string  `json:"fix_quality,omitempty"`
}

type RainfallPayload struct {
	RainfallMm           *float64 `json:"rainfall_mm,omitempty"`
	CumulativeRainfallMm *float64 `json:"cumulative_rainfall_mm,omitempty"`
	IntervalMinutes      *int     `json:"interval_minutes,omitempty"`
}

type StrainPayload struct {
	StrainMicrostrain  *float64 `json:"strain_microstrain,omitempty"`
	TemperatureCelsius *float64 `json:"temperature_celsius,omitempty"`
}

type SoilPressurePayload struct {
	PressureKpa        *float64 `json:"pressure_kpa,omitempty"`
	TemperatureCelsius *float64 `json:"temperature_celsius,omitempty"`
}

type DeepDisplacementPayload struct {
	DepthM           *float64 `json:"depth_m,omitempty"`
	TiltXmm          *float64 `json:"tilt_x_mm,omitempty"`
	TiltYmm          *float64 `json:"tilt_y_mm,omitempty"`
	CumulativeTiltMm *float64 `json:"cumulative_tilt_mm,omitempty"`
}
