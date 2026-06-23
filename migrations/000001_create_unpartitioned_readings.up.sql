-- Tablas de lecturas de volumen moderado: sin particionamiento.
-- rainfall, strain, soil_pressure (CLAUDE.md 2.4: particionar solo alto volumen).
-- Dedup por message_id via UNIQUE -> INSERT IGNORE no-op en reintentos (CLAUDE.md 1.3).

CREATE TABLE IF NOT EXISTS rainfall_readings (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    message_id VARCHAR(64),
    project_id VARCHAR(64) NOT NULL,
    sensor_id VARCHAR(64) NOT NULL,
    timestamp DATETIME NOT NULL,
    received_at DATETIME NOT NULL,
    rainfall_mm DOUBLE,
    cumulative_rainfall_mm DOUBLE,
    interval_minutes INT,
    UNIQUE KEY uk_rainfall_message (message_id),
    INDEX idx_rainfall_sensor_ts (sensor_id, timestamp),
    INDEX idx_rainfall_project_ts (project_id, timestamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS strain_readings (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    message_id VARCHAR(64),
    project_id VARCHAR(64) NOT NULL,
    sensor_id VARCHAR(64) NOT NULL,
    timestamp DATETIME NOT NULL,
    received_at DATETIME NOT NULL,
    strain_microstrain DOUBLE,
    temperature_celsius DOUBLE,
    UNIQUE KEY uk_strain_message (message_id),
    INDEX idx_strain_sensor_ts (sensor_id, timestamp),
    INDEX idx_strain_project_ts (project_id, timestamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS soil_pressure_readings (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    message_id VARCHAR(64),
    project_id VARCHAR(64) NOT NULL,
    sensor_id VARCHAR(64) NOT NULL,
    timestamp DATETIME NOT NULL,
    received_at DATETIME NOT NULL,
    pressure_kpa DOUBLE,
    temperature_celsius DOUBLE,
    UNIQUE KEY uk_soil_pressure_message (message_id),
    INDEX idx_soil_pressure_sensor_ts (sensor_id, timestamp),
    INDEX idx_soil_pressure_project_ts (project_id, timestamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
