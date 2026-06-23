# Migraciones — data-management

Migraciones versionadas con [`golang-migrate`](https://github.com/golang-migrate/migrate) (CLAUDE.md 2.4).
Nunca cambios manuales de esquema en produccion.

## Aplicar

```bash
# instalar CLI una vez
go install -tags mysql github.com/golang-migrate/migrate/v4/cmd/migrate@latest

# up / down (DSN sin parseTime para la CLI)
migrate -path ./migrations -database "mysql://USER:PASS@tcp(HOST:3306)/data_management" up
migrate -path ./migrations -database "mysql://USER:PASS@tcp(HOST:3306)/data_management" down 1
```

Tambien aplicables embebidas al boot (ver `internal/persistence/migrate.go`, opcional).

## Particionamiento

`gnss_readings` y `deep_displacement_readings` estan particionadas `RANGE (YEAR(timestamp))`
(alto volumen, CLAUDE.md 2.4 / B2). Las demas no.

**Mantenimiento:** al entrar un nuevo anio, anadir una migracion que haga
`ALTER TABLE ... REORGANIZE PARTITION pmax INTO (...)` para crear la particion
del anio y un nuevo `pmax`. No dejar que todo caiga en `pmax`.

## Database-per-service (CLAUDE.md 1.2)

> **Violacion del baseline:** el sistema actual usa un MySQL unico compartido con el
> usuario `landslide`. Esto es *Shared Persistency*, un bad smell validado
> empiricamente (Taibi & Lenarduzzi 2018) y prohibido por CLAUDE.md 1.2.

La reescritura usa un **esquema y usuario propios** con permisos minimos. La migracion
fisica del dato desde el MySQL compartido es incremental (Strangler Fig) y excede el
alcance de este servicio aislado.

Crear esquema + usuario (lo ejecuta un admin, no `migrate`, porque requiere privilegios
de creacion de usuario):

```sql
CREATE DATABASE IF NOT EXISTS data_management
  CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE USER IF NOT EXISTS 'data_management'@'%' IDENTIFIED BY '<password>';

-- Permisos minimos: solo DML sobre su propio esquema. Sin acceso a otros esquemas.
GRANT SELECT, INSERT, UPDATE, DELETE ON data_management.* TO 'data_management'@'%';
-- DDL (migraciones) lo aplica un rol separado de migracion, no el runtime.
FLUSH PRIVILEGES;
```

> El usuario de runtime no tiene DDL: las migraciones corren con un usuario de
> migracion aparte para limitar el blast radius.
