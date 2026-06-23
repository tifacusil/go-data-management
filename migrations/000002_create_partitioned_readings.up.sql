-- Tablas de alto volumen: particionadas por anio (CLAUDE.md 2.4, ref B2).
-- gnss, deep_displacement.
--
-- Restriccion MySQL: toda PRIMARY/UNIQUE KEY de una tabla particionada debe
-- incluir la columna de particion. Por eso:
--   - PRIMARY KEY (id, timestamp)
--   - UNIQUE KEY (message_id, timestamp)  <- dedup sigue valido: mismo mensaje
--     reintenta con el mismo timestamp, asi INSERT IGNORE es no-op (CLAUDE.md 1.3).
-- Rango por RANGE (YEAR(timestamp)). pmax captura lo no contemplado; anadir
-- particiones nuevas via migracion al cambiar de anio (no DDL manual en prod).

CREATE TABLE IF NOT EXISTS gnss_readings (
    id BIGINT AUTO_INCREMENT,
    message_id VARCHAR(64),
    project_id VARCHAR(64) NOT NULL,
    sensor_id VARCHAR(64) NOT NULL,
    timestamp DATETIME NOT NULL,
    received_at DATETIME NOT NULL,
    displacement_x_mm DOUBLE,
    displacement_y_mm DOUBLE,
    displacement_z_mm DOUBLE,
    displacement_horizontal_mm DOUBLE,
    cumulative_displacement_mm DOUBLE,
    fix_quality VARCHAR(16),
    PRIMARY KEY (id, timestamp),
    UNIQUE KEY uk_gnss_message (message_id, timestamp),
    INDEX idx_gnss_sensor_ts (sensor_id, timestamp),
    INDEX idx_gnss_project_ts (project_id, timestamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
PARTITION BY RANGE (YEAR(timestamp)) (
    PARTITION p2024 VALUES LESS THAN (2025),
    PARTITION p2025 VALUES LESS THAN (2026),
    PARTITION p2026 VALUES LESS THAN (2027),
    PARTITION p2027 VALUES LESS THAN (2028),
    PARTITION pmax  VALUES LESS THAN MAXVALUE
);

CREATE TABLE IF NOT EXISTS deep_displacement_readings (
    id BIGINT AUTO_INCREMENT,
    message_id VARCHAR(64),
    project_id VARCHAR(64) NOT NULL,
    sensor_id VARCHAR(64) NOT NULL,
    timestamp DATETIME NOT NULL,
    received_at DATETIME NOT NULL,
    depth_m DOUBLE,
    tilt_x_mm DOUBLE,
    tilt_y_mm DOUBLE,
    cumulative_tilt_mm DOUBLE,
    PRIMARY KEY (id, timestamp),
    UNIQUE KEY uk_deep_displacement_message (message_id, timestamp),
    INDEX idx_deep_displacement_sensor_ts (sensor_id, timestamp),
    INDEX idx_deep_displacement_project_ts (project_id, timestamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
PARTITION BY RANGE (YEAR(timestamp)) (
    PARTITION p2024 VALUES LESS THAN (2025),
    PARTITION p2025 VALUES LESS THAN (2026),
    PARTITION p2026 VALUES LESS THAN (2027),
    PARTITION p2027 VALUES LESS THAN (2028),
    PARTITION pmax  VALUES LESS THAN MAXVALUE
);
