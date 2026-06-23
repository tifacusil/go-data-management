package query

import (
	"fmt"
	"strings"
)

// latestSelectFor arma el SELECT de la ultima lectura por sensor de una tabla,
// uniendo contra un subquery de MAX(timestamp) agrupado por sensor_id.
// Espejo de selectLatestByProject del baseline. Solo filtra por project_id.
func latestSelectFor(sensorType, projectID string, f Filter, args *[]any) string {
	tbl := tableOf[sensorType]
	// El SELECT externo reusa selectFor para la proyeccion normalizada de 22 cols,
	// pero con alias de tabla "t". Reconstruimos columnas con prefijo.
	colExpr := normalizedCols(sensorType, "t")

	var inner strings.Builder
	fmt.Fprintf(&inner, "SELECT sensor_id, MAX(timestamp) AS max_ts FROM %s WHERE project_id = ?", tbl)
	*args = append(*args, projectID)
	if f.From != nil {
		inner.WriteString(" AND timestamp >= ?")
		*args = append(*args, *f.From)
	}
	if f.To != nil {
		inner.WriteString(" AND timestamp <= ?")
		*args = append(*args, *f.To)
	}
	if sensorType == "gnss" && f.FixQuality != nil {
		inner.WriteString(" AND fix_quality = ?")
		*args = append(*args, *f.FixQuality)
	}
	inner.WriteString(" GROUP BY sensor_id")

	var b strings.Builder
	fmt.Fprintf(&b, "SELECT %s FROM %s t JOIN (%s) m ON t.sensor_id = m.sensor_id AND t.timestamp = m.max_ts WHERE t.project_id = ?",
		colExpr, tbl, inner.String())
	*args = append(*args, projectID)
	if f.From != nil {
		b.WriteString(" AND t.timestamp >= ?")
		*args = append(*args, *f.From)
	}
	if f.To != nil {
		b.WriteString(" AND t.timestamp <= ?")
		*args = append(*args, *f.To)
	}
	if sensorType == "gnss" && f.FixQuality != nil {
		b.WriteString(" AND t.fix_quality = ?")
		*args = append(*args, *f.FixQuality)
	}
	return b.String()
}

// normalizedCols genera las 22 columnas normalizadas con prefijo de tabla,
// en el mismo orden que scanRows espera.
func normalizedCols(sensorType, prefix string) string {
	p := func(c string) string { return prefix + "." + c }
	base := []string{
		p("project_id") + " AS projectId",
		p("sensor_id") + " AS sensorId",
		fmt.Sprintf("'%s' AS sensorType", sensorType),
		p("timestamp") + " AS ts",
		p("received_at") + " AS receivedAt",
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
			base = append(base, p(col)+" AS "+alias)
		} else {
			base = append(base, "NULL AS "+alias)
		}
	}
	return strings.Join(base, ", ")
}

// specificCols mapea alias -> columna real por sensor_type.
func specificCols(sensorType string) map[string]string {
	switch sensorType {
	case "gnss":
		return map[string]string{
			"displacementXmm": "displacement_x_mm", "displacementYmm": "displacement_y_mm",
			"displacementZmm": "displacement_z_mm", "displacementHorizontalMm": "displacement_horizontal_mm",
			"cumulativeDisplacementMm": "cumulative_displacement_mm", "fixQuality": "fix_quality",
		}
	case "rainfall":
		return map[string]string{
			"rainfallMm": "rainfall_mm", "cumulativeRainfallMm": "cumulative_rainfall_mm",
			"intervalMinutes": "interval_minutes",
		}
	case "strain":
		return map[string]string{
			"strainMicrostrain": "strain_microstrain", "temperatureCelsius": "temperature_celsius",
		}
	case "soil_pressure":
		return map[string]string{
			"pressureKpa": "pressure_kpa", "temperatureCelsius": "temperature_celsius",
		}
	case "deep_displacement":
		return map[string]string{
			"depthM": "depth_m", "tiltXmm": "tilt_x_mm", "tiltYmm": "tilt_y_mm",
			"cumulativeTiltMm": "cumulative_tilt_mm",
		}
	}
	return nil
}
