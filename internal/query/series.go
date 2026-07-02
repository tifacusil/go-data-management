package query

import (
	"context"
	"fmt"
	"time"
)

// canonicalValueCol mapea sensor_type -> columna canonica devuelta por la serie
// gRPC (GetReadingSeries). Es la columna representativa del tipo, la misma que
// data-analysis solia leer directo (CLAUDE.md §1.2). Fuente unica de verdad:
// si el esquema fisico cambia, se ajusta aqui y data-analysis no se entera.
var canonicalValueCol = map[string]string{
	"gnss":              "cumulative_displacement_mm",
	"deep_displacement": "cumulative_tilt_mm",
	"rainfall":          "cumulative_rainfall_mm",
	"strain":            "strain_microstrain",
	"soil_pressure":     "pressure_kpa",
}

// SeriesPoint es un punto de la serie temporal: timestamp + valor canonico.
type SeriesPoint struct {
	Timestamp time.Time
	Value     float64
}

// Series es la respuesta de ProjectSeries.
type Series struct {
	SensorType  string
	ValueColumn string
	Points      []SeriesPoint
}

// ValueColumnFor devuelve la columna canonica de un sensor_type y si es valido.
func ValueColumnFor(sensorType string) (string, bool) {
	col, ok := canonicalValueCol[sensorType]
	return col, ok
}

// ProjectSeries devuelve la serie temporal (timestamp, valor canonico) de un
// sensor_type para un proyecto en una ventana [from, to], ordenada ASC.
// Reemplaza el SELECT directo que data-analysis hacia sobre estas tablas.
// from/to en formato datetime UTC "2006-01-02 15:04:05". limit acota filas.
func (q *Repository) ProjectSeries(ctx context.Context, projectID, sensorType, from, to string, limit int) (Series, error) {
	col, ok := canonicalValueCol[sensorType]
	if !ok {
		return Series{}, fmt.Errorf("sensor_type invalido: %q", sensorType)
	}
	tbl := tableOf[sensorType]

	sqlStr := fmt.Sprintf(
		"SELECT `timestamp`, `%s` AS val FROM %s "+
			"WHERE project_id = ? AND `timestamp` BETWEEN ? AND ? AND `%s` IS NOT NULL "+
			"ORDER BY `timestamp` ASC LIMIT ?",
		col, tbl, col)

	rows, err := q.db.QueryContext(ctx, sqlStr, projectID, from, to, limit)
	if err != nil {
		return Series{}, err
	}
	defer func() { _ = rows.Close() }()

	points := make([]SeriesPoint, 0, limit)
	for rows.Next() {
		var p SeriesPoint
		if err := rows.Scan(&p.Timestamp, &p.Value); err != nil {
			return Series{}, err
		}
		points = append(points, p)
	}
	if err := rows.Err(); err != nil {
		return Series{}, err
	}
	return Series{SensorType: sensorType, ValueColumn: col, Points: points}, nil
}
