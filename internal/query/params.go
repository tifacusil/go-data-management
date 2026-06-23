package query

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	defaultLimit = 200
	maxLimit     = 1000
)

var supportedSensorTypes = map[string]bool{
	"gnss": true, "rainfall": true, "strain": true,
	"soil_pressure": true, "deep_displacement": true,
}

// BadRequest marca un error de validacion de parametros -> HTTP 400.
type BadRequest struct{ Msg string }

func (e *BadRequest) Error() string { return e.Msg }

// ParsedParams son los parametros validados de una consulta de readings.
type ParsedParams struct {
	Filter Filter
	Offset int
	Limit  int
}

// ParseParams valida y normaliza los query params (espejo del baseline).
// Aplica defaults, rangos, y la regla fix_quality solo-gnss.
func ParseParams(q url.Values, withPaging bool) (ParsedParams, error) {
	var p ParsedParams

	from, err := parseTime(q.Get("from"))
	if err != nil {
		return p, &BadRequest{"from invalido: " + err.Error()}
	}
	to, err := parseTime(q.Get("to"))
	if err != nil {
		return p, &BadRequest{"to invalido: " + err.Error()}
	}
	if from != nil && to != nil && *from > *to {
		return p, &BadRequest{"from debe ser <= to"}
	}

	sensorType, err := normalizeSensorType(q.Get("sensor_type"))
	if err != nil {
		return p, err
	}

	fixQuality := emptyToNil(q.Get("fix_quality"))
	if fixQuality != nil && sensorType != nil && *sensorType != "gnss" {
		return p, &BadRequest{"fix_quality solo aplica para sensor_type=gnss"}
	}

	p.Filter = Filter{SensorType: sensorType, From: from, To: to, FixQuality: fixQuality}

	if withPaging {
		p.Offset, err = parseOffset(q.Get("offset"))
		if err != nil {
			return p, err
		}
		p.Limit, err = parseLimit(q.Get("limit"))
		if err != nil {
			return p, err
		}
	}
	return p, nil
}

// parseTime parsea ISO-8601 y lo convierte a "2006-01-02 15:04:05" en UTC para
// comparar con las columnas DATETIME (guardadas en UTC).
func parseTime(s string) (*string, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return nil, fmt.Errorf("usar ISO-8601 (RFC3339)")
	}
	out := t.UTC().Format("2006-01-02 15:04:05")
	return &out, nil
}

func normalizeSensorType(s string) (*string, error) {
	s = strings.ToLower(strings.TrimSpace(s))
	if s == "" {
		return nil, nil
	}
	if !supportedSensorTypes[s] {
		return nil, &BadRequest{"sensor_type no soportado: " + s}
	}
	return &s, nil
}

func parseOffset(s string) (int, error) {
	if s == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 0 {
		return 0, &BadRequest{"offset debe ser >= 0"}
	}
	return n, nil
}

func parseLimit(s string) (int, error) {
	if s == "" {
		return defaultLimit, nil
	}
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return 0, &BadRequest{"limit debe ser > 0"}
	}
	if n > maxLimit {
		return 0, &BadRequest{fmt.Sprintf("limit excede maximo: %d", maxLimit)}
	}
	return n, nil
}

func emptyToNil(s string) *string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return &s
}
