package query

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// Repository ejecuta las consultas de lectura sobre MySQL.
type Repository struct {
	db *sql.DB
}

func New(db *sql.DB) *Repository { return &Repository{db: db} }

// Filter agrupa los filtros comunes de las consultas. Punteros = ausente.
type Filter struct {
	SensorType *string
	From       *string // datetime UTC "2006-01-02 15:04:05"
	To         *string
	FixQuality *string
}

// scanRows escanea el resultado de la query unificada (22 columnas en orden fijo).
func scanRows(rows *sql.Rows) ([]Reading, error) {
	defer func() { _ = rows.Close() }()
	var out []Reading
	for rows.Next() {
		var r row
		if err := rows.Scan(
			&r.projectID, &r.sensorID, &r.sensorType, &r.timestamp, &r.receivedAt,
			&r.displacementXmm, &r.displacementYmm, &r.displacementZmm,
			&r.displacementHorizontalMm, &r.cumulativeDisplacementMm, &r.fixQuality,
			&r.rainfallMm, &r.cumulativeRainfallMm, &r.intervalMinutes,
			&r.strainMicrostrain, &r.temperatureCelsius, &r.pressureKpa,
			&r.depthM, &r.tiltXmm, &r.tiltYmm, &r.cumulativeTiltMm,
		); err != nil {
			return nil, err
		}
		out = append(out, r.toReading())
	}
	return out, rows.Err()
}

// readingsPage consulta una pagina de lecturas filtradas por filterCol/filterVal.
// Hace probe (limit+1) para saber si hay mas sin contar siempre (igual baseline).
func (q *Repository) readingsPage(ctx context.Context, filterCol, filterVal string, f Filter, offset, limit int) (Paged, error) {
	var args []any
	union := buildUnion(filterCol, filterVal, f.SensorType, f.From, f.To, f.FixQuality, &args)

	probe := limit + 1
	sqlStr := fmt.Sprintf(
		"SELECT * FROM (\n%s\n) r ORDER BY ts DESC, sensorId ASC, sensorType ASC LIMIT ? OFFSET ?",
		union)
	args = append(args, probe, offset)

	rows, err := q.db.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		return Paged{}, err
	}
	items, err := scanRows(rows)
	if err != nil {
		return Paged{}, err
	}

	hasMore := len(items) > limit
	if hasMore {
		items = items[:limit]
	}

	var total int64
	if hasMore {
		total, err = q.count(ctx, filterCol, filterVal, f)
		if err != nil {
			return Paged{}, err
		}
	} else {
		total = int64(offset + len(items))
	}

	return Paged{Total: total, Offset: offset, Limit: limit, Items: items}, nil
}

// count cuenta el total de lecturas para el filtro (solo se llama si hay mas pagina).
func (q *Repository) count(ctx context.Context, filterCol, filterVal string, f Filter) (int64, error) {
	var args []any
	parts := make([]string, 0, 5)
	for _, st := range typesToQuery(f.SensorType) {
		var b strings.Builder
		fmt.Fprintf(&b, "SELECT 1 FROM %s WHERE %s = ?", tableOf[st], filterCol)
		args = append(args, filterVal)
		if f.From != nil {
			b.WriteString(" AND timestamp >= ?")
			args = append(args, *f.From)
		}
		if f.To != nil {
			b.WriteString(" AND timestamp <= ?")
			args = append(args, *f.To)
		}
		if st == "gnss" && f.FixQuality != nil {
			b.WriteString(" AND fix_quality = ?")
			args = append(args, *f.FixQuality)
		}
		parts = append(parts, b.String())
	}
	sqlStr := "SELECT COUNT(*) FROM (\n" + strings.Join(parts, "\nUNION ALL\n") + "\n) r"
	var total int64
	err := q.db.QueryRowContext(ctx, sqlStr, args...).Scan(&total)
	return total, err
}

// SensorReadings devuelve una pagina de lecturas de un sensor.
func (q *Repository) SensorReadings(ctx context.Context, sensorID string, f Filter, offset, limit int) (Paged, error) {
	return q.readingsPage(ctx, "sensor_id", sensorID, f, offset, limit)
}

// ProjectReadings devuelve una pagina de lecturas de un proyecto.
func (q *Repository) ProjectReadings(ctx context.Context, projectID string, f Filter, offset, limit int) (Paged, error) {
	return q.readingsPage(ctx, "project_id", projectID, f, offset, limit)
}

// ProjectLatest devuelve la ultima lectura por sensor de un proyecto.
// Port del JOIN con MAX(timestamp) por sensor del baseline.
func (q *Repository) ProjectLatest(ctx context.Context, projectID string, f Filter) ([]Reading, error) {
	var args []any
	parts := make([]string, 0, 5)
	for _, st := range typesToQuery(f.SensorType) {
		parts = append(parts, latestSelectFor(st, projectID, f, &args))
	}
	sqlStr := "SELECT * FROM (\n" + strings.Join(parts, "\nUNION ALL\n") +
		"\n) latest ORDER BY sensorType ASC, sensorId ASC"
	rows, err := q.db.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(rows)
}
