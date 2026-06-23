// Package query lee lecturas persistidas para la API REST de consulta.
// Espejo de las queries de GnssReadingMapper del baseline: UNION ALL sobre las
// 5 tablas normalizando columnas, o una sola tabla si se filtra por sensor_type.
package query

import (
	"encoding/json"
	"time"
)

// Reading es una lectura normalizada para la respuesta JSON. payload lleva solo
// los campos del sensor_type correspondiente (CLAUDE.md: contrato OpenAPI M0).
type Reading struct {
	ProjectID  string          `json:"project_id"`
	SensorID   string          `json:"sensor_id"`
	SensorType string          `json:"sensor_type"`
	Timestamp  time.Time       `json:"timestamp"`
	ReceivedAt time.Time       `json:"received_at"`
	Payload    json.RawMessage `json:"payload,omitempty"`
}

// Paged es la respuesta paginada de readings.
type Paged struct {
	Total  int64     `json:"total"`
	Offset int       `json:"offset"`
	Limit  int       `json:"limit"`
	Items  []Reading `json:"items"`
}

// row es la fila cruda de la query unificada: punteros para distinguir NULL.
type row struct {
	projectID  string
	sensorID   string
	sensorType string
	timestamp  time.Time
	receivedAt time.Time

	displacementXmm          *float64
	displacementYmm          *float64
	displacementZmm          *float64
	displacementHorizontalMm *float64
	cumulativeDisplacementMm *float64
	fixQuality               *string
	rainfallMm               *float64
	cumulativeRainfallMm     *float64
	intervalMinutes          *int64
	strainMicrostrain        *float64
	temperatureCelsius       *float64
	pressureKpa              *float64
	depthM                   *float64
	tiltXmm                  *float64
	tiltYmm                  *float64
	cumulativeTiltMm         *float64
}

// toReading construye la Reading con el payload especifico del sensor_type.
func (r row) toReading() Reading {
	m := map[string]any{}
	put := func(k string, v any) {
		switch x := v.(type) {
		case *float64:
			if x != nil {
				m[k] = *x
			}
		case *int64:
			if x != nil {
				m[k] = *x
			}
		case *string:
			if x != nil {
				m[k] = *x
			}
		}
	}
	switch r.sensorType {
	case "gnss":
		put("displacement_x_mm", r.displacementXmm)
		put("displacement_y_mm", r.displacementYmm)
		put("displacement_z_mm", r.displacementZmm)
		put("displacement_horizontal_mm", r.displacementHorizontalMm)
		put("cumulative_displacement_mm", r.cumulativeDisplacementMm)
		put("fix_quality", r.fixQuality)
	case "rainfall":
		put("rainfall_mm", r.rainfallMm)
		put("cumulative_rainfall_mm", r.cumulativeRainfallMm)
		put("interval_minutes", r.intervalMinutes)
	case "strain":
		put("strain_microstrain", r.strainMicrostrain)
		put("temperature_celsius", r.temperatureCelsius)
	case "soil_pressure":
		put("pressure_kpa", r.pressureKpa)
		put("temperature_celsius", r.temperatureCelsius)
	case "deep_displacement":
		put("depth_m", r.depthM)
		put("tilt_x_mm", r.tiltXmm)
		put("tilt_y_mm", r.tiltYmm)
		put("cumulative_tilt_mm", r.cumulativeTiltMm)
	}
	payload, _ := json.Marshal(m)
	return Reading{
		ProjectID:  r.projectID,
		SensorID:   r.sensorID,
		SensorType: r.sensorType,
		Timestamp:  r.timestamp,
		ReceivedAt: r.receivedAt,
		Payload:    payload,
	}
}
