// Package persistence escribe lecturas de sensores a MySQL.
// Idempotencia por INSERT IGNORE keyed por message_id: un reintento del mismo
// mensaje es no-op (CLAUDE.md 1.3). Clasifica errores transitorios vs
// permanentes para que el consumidor decida retry vs DLQ (CLAUDE.md 2.2).
package persistence

import (
	"context"
	"database/sql"
	"time"

	"github.com/AngelloFD/go-data-management/internal/model"
)

// Repository persiste lecturas. Una sola instancia comparte el *sql.DB (pool).
type Repository struct {
	db *sql.DB
}

func New(db *sql.DB) *Repository {
	return &Repository{db: db}
}

// utc convierte el instante a UTC; todas las columnas DATETIME se guardan en UTC
// (igual que el baseline, que mapea Instant -> LocalDateTime en ZoneOffset.UTC).
func utc(t time.Time) time.Time { return t.UTC() }

func (r *Repository) PersistGNSS(ctx context.Context, e *model.Envelope, p *model.GNSSPayload) error {
	const q = `INSERT IGNORE INTO gnss_readings
		(message_id, project_id, sensor_id, timestamp, received_at,
		 displacement_x_mm, displacement_y_mm, displacement_z_mm,
		 displacement_horizontal_mm, cumulative_displacement_mm, fix_quality)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	return r.insert(ctx, q,
		e.MessageID, e.ProjectID, e.SensorID, utc(e.Timestamp), utc(e.ReceivedAt),
		p.DisplacementXmm, p.DisplacementYmm, p.DisplacementZmm,
		p.DisplacementHorizontalMm, p.CumulativeDisplacementMm, p.FixQuality,
	)
}

func (r *Repository) PersistRainfall(ctx context.Context, e *model.Envelope, p *model.RainfallPayload) error {
	const q = `INSERT IGNORE INTO rainfall_readings
		(message_id, project_id, sensor_id, timestamp, received_at,
		 rainfall_mm, cumulative_rainfall_mm, interval_minutes)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
	return r.insert(ctx, q,
		e.MessageID, e.ProjectID, e.SensorID, utc(e.Timestamp), utc(e.ReceivedAt),
		p.RainfallMm, p.CumulativeRainfallMm, p.IntervalMinutes,
	)
}

func (r *Repository) PersistStrain(ctx context.Context, e *model.Envelope, p *model.StrainPayload) error {
	const q = `INSERT IGNORE INTO strain_readings
		(message_id, project_id, sensor_id, timestamp, received_at,
		 strain_microstrain, temperature_celsius)
		VALUES (?, ?, ?, ?, ?, ?, ?)`
	return r.insert(ctx, q,
		e.MessageID, e.ProjectID, e.SensorID, utc(e.Timestamp), utc(e.ReceivedAt),
		p.StrainMicrostrain, p.TemperatureCelsius,
	)
}

func (r *Repository) PersistSoilPressure(ctx context.Context, e *model.Envelope, p *model.SoilPressurePayload) error {
	const q = `INSERT IGNORE INTO soil_pressure_readings
		(message_id, project_id, sensor_id, timestamp, received_at,
		 pressure_kpa, temperature_celsius)
		VALUES (?, ?, ?, ?, ?, ?, ?)`
	return r.insert(ctx, q,
		e.MessageID, e.ProjectID, e.SensorID, utc(e.Timestamp), utc(e.ReceivedAt),
		p.PressureKpa, p.TemperatureCelsius,
	)
}

func (r *Repository) PersistDeepDisplacement(ctx context.Context, e *model.Envelope, p *model.DeepDisplacementPayload) error {
	const q = `INSERT IGNORE INTO deep_displacement_readings
		(message_id, project_id, sensor_id, timestamp, received_at,
		 depth_m, tilt_x_mm, tilt_y_mm, cumulative_tilt_mm)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
	return r.insert(ctx, q,
		e.MessageID, e.ProjectID, e.SensorID, utc(e.Timestamp), utc(e.ReceivedAt),
		p.DepthM, p.TiltXmm, p.TiltYmm, p.CumulativeTiltMm,
	)
}

// insert ejecuta un INSERT IGNORE y distingue "fila escrita" de "fila
// ignorada". Sin esa distincion un duplicado era indistinguible de un insert
// exitoso: AP6 solo podia verificarse por ausencia de efecto (conteo de filas),
// no por una traza positiva como la de early-warning.
func (r *Repository) insert(ctx context.Context, q string, args ...any) error {
	res, err := r.db.ExecContext(ctx, q, args...)
	if err != nil {
		return classifyDBError(err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		// El driver siempre reporta filas afectadas para un INSERT; si no pudo,
		// la fila si se escribio y no hay nada que reintentar.
		return nil
	}
	if n == 0 {
		return ErrIgnored
	}
	return nil
}

// Ping verifica conectividad real a MySQL (lo usa el health check, CLAUDE.md 1.6).
func (r *Repository) Ping(ctx context.Context) error {
	return r.db.PingContext(ctx)
}
