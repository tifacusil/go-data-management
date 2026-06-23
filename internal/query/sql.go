package query

import (
	"fmt"
	"strings"
)

// allSensorTypes en orden estable.
var allSensorTypes = []string{"gnss", "rainfall", "strain", "soil_pressure", "deep_displacement"}

// tableOf mapea sensor_type -> tabla.
var tableOf = map[string]string{
	"gnss":              "gnss_readings",
	"rainfall":          "rainfall_readings",
	"strain":            "strain_readings",
	"soil_pressure":     "soil_pressure_readings",
	"deep_displacement": "deep_displacement_readings",
}

// selectFor devuelve el SELECT de una tabla normalizado a 22 columnas (NULL en
// las que no aplican), filtrando por filterCol (= sensor_id o project_id) + rango
// de tiempo + fix_quality (solo gnss). Los args se acumulan en orden.
func selectFor(sensorType, filterCol, filterVal string, from, to, fixQuality *string, args *[]any) string {
	var b strings.Builder
	// columnas sin prefijo de tabla (consulta de una sola tabla).
	fmt.Fprintf(&b, "SELECT %s FROM %s WHERE %s = ?",
		normalizedColsNoPrefix(sensorType), tableOf[sensorType], filterCol)
	*args = append(*args, filterVal)
	if from != nil {
		b.WriteString(" AND timestamp >= ?")
		*args = append(*args, *from)
	}
	if to != nil {
		b.WriteString(" AND timestamp <= ?")
		*args = append(*args, *to)
	}
	if sensorType == "gnss" && fixQuality != nil {
		b.WriteString(" AND fix_quality = ?")
		*args = append(*args, *fixQuality)
	}
	return b.String()
}

// normalizedColsNoPrefix: las 22 columnas normalizadas sin prefijo de tabla.
func normalizedColsNoPrefix(sensorType string) string {
	base := []string{
		"project_id AS projectId",
		"sensor_id AS sensorId",
		fmt.Sprintf("'%s' AS sensorType", sensorType),
		"timestamp AS ts",
		"received_at AS receivedAt",
	}
	order := []string{
		"displacementXmm", "displacementYmm", "displacementZmm",
		"displacementHorizontalMm", "cumulativeDisplacementMm", "fixQuality",
		"rainfallMm", "cumulativeRainfallMm", "intervalMinutes",
		"strainMicrostrain", "temperatureCelsius", "pressureKpa",
		"depthM", "tiltXmm", "tiltYmm", "cumulativeTiltMm",
	}
	specific := specificCols(sensorType)
	for _, alias := range order {
		if col, ok := specific[alias]; ok {
			base = append(base, col+" AS "+alias)
		} else {
			base = append(base, "NULL AS "+alias)
		}
	}
	return strings.Join(base, ", ")
}

// typesToQuery devuelve los sensor_type a unir: el filtrado, o todos.
func typesToQuery(sensorType *string) []string {
	if sensorType != nil {
		return []string{*sensorType}
	}
	return allSensorTypes
}

// buildUnion arma el UNION ALL de los SELECT por tipo.
func buildUnion(filterCol, filterVal string, sensorType, from, to, fixQuality *string, args *[]any) string {
	parts := make([]string, 0, 5)
	for _, st := range typesToQuery(sensorType) {
		parts = append(parts, selectFor(st, filterCol, filterVal, from, to, fixQuality, args))
	}
	return strings.Join(parts, "\nUNION ALL\n")
}
