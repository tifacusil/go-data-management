# ROADMAP — Reescritura de `data-management` (Java → Go)

> Alcance: **solo** el microservicio `data-management`. Baseline = `data-management-demo` (Spring Boot 3.5 + MyBatis + MySQL + RabbitMQ + Consul).
> Reglas guía: `CLAUDE.md` (prácticas SE no negociables, sección 1–8).
> Objetivo de la tesis: validar mejora empírica de rendimiento (throughput, latencia, CPU, memoria) aplicando buenas prácticas SE, marco El Fellah et al. (2026).

---

## 0. Análisis del baseline (qué hace el demo hoy)

Flujo:

```
RabbitMQ queue data_management.persist (manual ack)
  → parse JSON Envelope
  → MessageRouter.route() switch sensor_type
  → PersistenceService.persistX() → INSERT IGNORE tabla por sensor
  → ack si ok / nack(requeue=false) si no-recuperable / nack(requeue=true) si transitorio
REST query API (sensor/project readings paged, project latest) + Swagger
ConsulAutoRegistrar (register + heartbeat 10s + health check /health real)
```

5 tipos de sensor: `gnss`, `rainfall`, `strain`, `soil_pressure`, `deep_displacement`.
Cada uno → tabla propia, dedup por `UNIQUE(message_id)` vía `INSERT IGNORE`.

### Cumplimiento actual vs CLAUDE.md

| Práctica (CLAUDE.md) | Estado baseline | Acción reescritura |
|---|---|---|
| Contract-first (§1.1) | ✗ No hay `contracts/` | Crear JSON Schema envelope + OpenAPI query API **antes** de handlers |
| Database-per-service (§1.2) | ⚠ MySQL único compartido, user `landslide` | Documentar violación + esquema/usuario propio `data_management` permisos mínimos. Migración Strangler (incremental) |
| Idempotencia consumidor (§1.3) | ◑ Solo `INSERT IGNORE` (DB-level) | Mantener + dedup explícito por `message_id`, log de duplicado descartado |
| Versionado semántico contratos (§1.4) | ✗ `schema_version` ignorado | Validar `schema_version` en envelope, aceptar major actual + gracia |
| Resiliencia llamadas síncronas (§1.5) | N/A (servicio no hace gRPC saliente hoy) | No aplica por ahora; dejar hook si futura llamada a project-management |
| Health checks reales (§1.6) | ✓ MySQL + RabbitMQ real | Portar igual a Go |
| Config externalizada (§1.7) | ✓ env vars | Portar; mover a struct config validada al boot |
| Logging estructurado + correlation ID (§1.8) | ✗ | `slog` JSON + `message_id`/`trace_id` en todo log |
| Retry + backoff acotado (§2.2, §4) | ✗ requeue infinito vía Rabbit | Retry máx 3 (1s/5s/30s) → DLQ `sensors.dead_letter` vía DLX |
| Dead Letter Queue (§4) | ◑ delegado a topología Rabbit | Declarar DLX/DLQ + alerta a monitoring |
| Métricas Prometheus (§5) | ✗ solo contador en memoria | `/metrics`: CPU, mem residente, throughput, latencia p50/p90/p95/p99 |
| Distributed tracing OTel (§5) | ✗ | OpenTelemetry, `message_id` como `trace_id` |
| Migraciones versionadas (§2.4) | ✗ `schema.sql` manual | `golang-migrate` |
| Particionamiento por año (§2.4) | ✗ | Particionar tablas alto volumen (gnss, deep_displacement) por año |
| Testing pirámide (§3) | ✗ solo test vacío | Unit + contract (JSON Schema) + CDCT (pact-go message) + integration (testcontainers) |

**Leyenda:** ✓ cumple · ◑ parcial · ✗ ausente · ⚠ viola, señalado por CLAUDE.md

---

## 1. Stack Go objetivo

| Capa | Java baseline | Go elegido | Razón |
|---|---|---|---|
| Runtime | Spring Boot | stdlib + libs mínimas | Menos overhead → hipótesis de mejora |
| AMQP | spring-amqp | `github.com/rabbitmq/amqp091-go` | Cliente oficial, control manual de ack/DLX |
| DB | MyBatis | `database/sql` + `jackc/pgx` o `go-sql-driver/mysql` | MySQL en baseline → `go-sql-driver/mysql` |
| Migraciones | manual | `golang-migrate/migrate` | §2.4 |
| HTTP | Spring MVC | stdlib `net/http` + `chi` (router liviano) | Query API + /health + /metrics |
| Config | Spring `@Value` | `env` + struct validada (`caarlos0/env` o stdlib) | §1.7 |
| Logging | slf4j | `log/slog` (stdlib) JSON | §1.8 |
| Métricas | — | `prometheus/client_golang` | §5 |
| Tracing | — | `go.opentelemetry.io/otel` | §5 |
| Contract test | — | JSON Schema validator (`santhosh-tekuri/jsonschema`) | §3.2 |
| CDCT | — | `pact-foundation/pact-go` (message pact) | §3.3 |
| Integration | testcontainers-java | `testcontainers-go` | §3.4 |
| OpenAPI | springdoc | spec escrita a mano en `contracts/openapi/` | contract-first |

> **Nota DB:** baseline usa MySQL. CLAUDE.md cita `pgx` como ejemplo pero el sistema es MySQL — mantener MySQL salvo decisión explícita de migrar a Postgres (sería un cambio de alcance separado).

---

## 2. Estructura de proyecto propuesta

```
go-data-management/
├── cmd/data-management/main.go      # wiring, graceful shutdown
├── internal/
│   ├── config/                      # struct config + validación al boot
│   ├── consumer/                    # AMQP listener, ack/nack/DLX, retry
│   ├── router/                      # route por sensor_type
│   ├── persistence/                 # repos por sensor, INSERT IGNORE
│   ├── model/                       # Envelope + payloads
│   ├── query/                       # handlers REST query API
│   ├── health/                      # health checks reales
│   ├── observability/              # slog, prometheus, otel
│   └── consul/                      # auto-register + heartbeat
├── contracts/
│   ├── schemas/envelope.schema.json
│   ├── openapi/data-management.yaml
│   └── examples/
├── migrations/                      # golang-migrate *.up.sql / *.down.sql
├── deploy/                          # systemd unit / Dockerfile
├── go.mod
└── ROADMAP.md
```

---

## 3. Hitos (milestones)

Cada hito es entregable independiente y verificable. Orden respeta contract-first y dependencias.

### M0 — Bootstrap + contratos (contract-first)
**Dependencia:** ninguna. **Bloquea:** todo lo demás (CLAUDE.md §1.1).
- `go mod init`, layout `cmd/`+`internal/`.
- `contracts/schemas/envelope.schema.json` (Draft 2020-12, `schema_version` semántico, 5 payloads).
- `contracts/openapi/data-management.yaml` para query API + `/health`.
- `contracts/examples/*.json` (1 por sensor_type).
- Config struct validada al boot (falla rápido si falta secret).
- `slog` JSON estructurado base (timestamp, level, service, instance_id, event).
- **Done:** schema valida los 5 ejemplos; OpenAPI lint pasa; binario arranca y loguea JSON.

### M1 — Persistencia + migraciones
**Dependencia:** M0. **Aplica:** §2.4, §1.2.
- `golang-migrate`: portar `schema.sql` a migraciones versionadas; tablas con `UNIQUE(message_id)`.
- Particionamiento por año en `gnss_readings` y `deep_displacement_readings`.
- Esquema/usuario `data_management` propio, permisos mínimos (DB-per-service). Documentar la violación del MySQL compartido + plan Strangler.
- Repos por sensor con `INSERT IGNORE` keyed por `message_id`; clasificación error transitorio vs permanente (portar lógica `wrapDbException`).
- **Done:** `migrate up/down` idempotente; unit tests de repos con sqlmock; insert duplicado → no-op verificado.

### M2 — Consumer idempotente + resiliencia
**Dependencia:** M1. **Aplica:** §1.3, §2.2, §4.
- Listener AMQP `data_management.persist`, ack manual.
- Parse Envelope → validar contra JSON Schema (rechazo a DLQ si inválido) → router por `sensor_type`.
- Idempotencia: dedup por `message_id` (DB `INSERT IGNORE` + log de duplicado).
- Retry acotado: máx 3 intentos, backoff 1s/5s/30s; agotado → `sensors.dlx` → `sensors.dead_letter`.
- Declarar DLX/DLQ + alerta a monitoring al enviar a dead-letter.
- Correlation ID (`message_id`/`trace_id`) en todos los logs del flujo.
- **Done:** unit tests de router/ack-policy; integration test (testcontainers RabbitMQ+MySQL): ingesta→persistencia, mensaje inválido→DLQ, transitorio→retry.

### M3 — Query API REST
**Dependencia:** M1 (lee), M0 (OpenAPI). **Aplica:** contract-first, §1.6.
- Portar endpoints: `GET /api/v1/sensors/{sensor_id}/readings`, `/projects/{project_id}/readings` (paged), `/projects/{project_id}/latest`.
- Validación de params (limit/offset/from-to/sensor_type/fix_quality) idéntica al baseline.
- Conformidad con OpenAPI de M0.
- `/health` real (MySQL `SELECT 1` + RabbitMQ conn open + messages_processed).
- **Done:** handler tests; respuestas validan contra OpenAPI; paginación/probe-limit equivalente al baseline.

### M4 — Observabilidad completa
**Dependencia:** M2, M3. **Aplica:** §5.
- `/metrics` Prometheus: `cpu_user_seconds_total`, `process_resident_memory_bytes`, throughput msg/s, latencia persistencia p50/p90/p95/p99, contadores ack/nack/dlq/duplicados.
- OpenTelemetry tracing flujo consume→persist, `message_id` como `trace_id`.
- **Done:** `/metrics` scrapeable; traza visible end-to-end de un mensaje.

### M5 — Service discovery + deploy
**Dependencia:** M3 (health). **Aplica:** §1.5 (registro Consul).
- Portar `ConsulAutoRegistrar`: register + heartbeat 10s + deregister en shutdown.
- ID `data-management-<ip>-<port>`, tags `version-`, `lang-go`, `env-`.
- Graceful shutdown (drain consumer, deregister, cerrar pools).
- `Dockerfile` + systemd unit en `deploy/`.
- **Done:** servicio aparece healthy en Consul catalog; SIGTERM → deregister limpio.

### M6 — Testing CDCT + cierre pirámide
**Dependencia:** M2, M3. **Aplica:** §3.
- Contract tests: ejemplos en `contracts/examples/` siguen válidos contra schema vigente.
- CDCT con `pact-go`: message pact productor(data-receiving)↔consumidor(data-management) en `data_management.persist`; HTTP pact para query API.
- Cobertura mínima: unit (lógica), contract, CDCT, integration críticos.
- **Done:** suite verde en CI; pacts publicados/verificados.

### M7 — Benchmark comparativo (objetivo tesis)
**Dependencia:** M0–M6 completos. **Aplica:** marco El Fellah et al. (2026).
- Carga idéntica contra baseline Java y reescritura Go.
- Medir throughput, latencia (p50/p95/p99), CPU, memoria residente.
- Reporte comparativo reproducible (mismo dataset, mismo hardware).
- **Done:** tabla de métricas Java vs Go + análisis vs hipótesis (+throughput, −latencia, −memoria).

---

## 4. Grafo de dependencias

```
M0 ──┬─→ M1 ──┬─→ M2 ──┬─→ M4 ──→ M7
     │        │        │
     └─→ M3 ──┴────────┴─→ M6
              │
              └─→ M5
```

M0 bloquea todo (contract-first). M1 y M3 paralelizables tras M0. M7 cierra al final.

---

## 5. Riesgos / decisiones abiertas

1. **MySQL vs Postgres** — baseline MySQL; CLAUDE.md ejemplifica `pgx`. Recomendación: **mantener MySQL** (no inflar alcance). Confirmar.
2. **DB-per-service** — MySQL compartido viola §1.2. Reescritura usa usuario/esquema propio, pero migración física del dato es Strangler incremental fuera del alcance de un solo servicio.
3. **Particionamiento** — requiere DDL MySQL (RANGE por año). Validar volúmenes reales antes de M1.
4. **gRPC saliente** — hoy data-management no llama a otros servicios. Circuit breaker (§1.5) no aplica todavía; dejar punto de extensión.

---

## 6. TODO trazables en código

Donde una práctica no pueda aplicarse en su hito (CLAUDE.md §7), dejar:

```go
// TODO(SE-practice): <qué falta> — <por qué> — ref CLAUDE.md §<n>
```

Ej. si M2 entra sin pact: `// TODO(SE-practice): falta CDCT pact-go, solo contract test JSON Schema — ref §3.3`.
