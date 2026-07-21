// Package config carga y valida la configuracion del servicio desde variables
// de entorno al arranque. Fail-fast: si falta un secreto requerido o un valor
// es invalido, el binario no arranca (CLAUDE.md 1.7).
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Config es la configuracion completa del servicio, resuelta una sola vez al boot.
type Config struct {
	Service  Service
	HTTP     HTTP
	GRPC     GRPC
	RabbitMQ RabbitMQ
	MySQL    MySQL
}

type Service struct {
	Name       string // nombre logico del servicio
	InstanceID string // id de instancia para logs/metricas
	Env        string // dev|staging|prod
}

type HTTP struct {
	Port int
}

type GRPC struct {
	Port int
}

type RabbitMQ struct {
	Host             string
	Port             int
	Username         string
	Password         string // secreto requerido
	Queue            string
	ListenersEnabled bool
	Prefetch         int // QoS del canal (mensajes sin ack en vuelo)
	Workers          int // goroutines concurrentes que persisten a MySQL
}

type MySQL struct {
	// DSN se construye desde MYSQL_HOST/PORT/DATABASE/USER/PASSWORD (convencion
	// de tesis-infra). Incluye parseTime=true&loc=UTC fijos.
	DSN string
	// Pool: acota conexiones abiertas al pool. Con procesamiento concurrente el
	// consumer abre hasta Workers conexiones en paralelo; sin tope el pool crece
	// sin limite y puede agotar max_connections de MySQL.
	MaxOpenConns int
	MaxIdleConns int
}

// buildDSN arma el DSN de go-sql-driver/mysql desde partes.
func buildDSN(user, pass, host string, port int, db string) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&loc=UTC", user, pass, host, port, db)
}

// Load lee el entorno, aplica defaults y valida. Devuelve error agregado con
// todos los problemas encontrados para no obligar a corregir uno por uno.
func Load() (*Config, error) {
	var errs []string

	// MySQL: convencion split de tesis-infra. DSN se arma desde las partes.
	mysqlHost := getenv("MYSQL_HOST", "localhost")
	mysqlPort := getint("MYSQL_PORT", 3306, &errs)
	mysqlDB := getenv("MYSQL_DATABASE", "data_management")
	mysqlUser := getenv("MYSQL_USER", "")
	mysqlPass := os.Getenv("MYSQL_PASSWORD")

	cfg := &Config{
		Service: Service{
			Name:       getenv("SERVICE_NAME", "data-management"),
			InstanceID: getenv("INSTANCE_ID", hostnameOr("data-management-local")),
			Env:        getenv("ENV", "dev"),
		},
		HTTP: HTTP{
			Port: getint("HTTP_PORT", 8080, &errs),
		},
		GRPC: GRPC{
			Port: getint("GRPC_PORT", 9090, &errs),
		},
		RabbitMQ: RabbitMQ{
			Host:             getenv("RABBITMQ_HOST", "localhost"),
			Port:             getint("RABBITMQ_PORT", 5672, &errs),
			Username:         getenv("RABBITMQ_USER", "data_management"),
			Password:         os.Getenv("RABBITMQ_PASSWORD"),
			Queue:            getenv("RABBITMQ_QUEUE", "data_management.persist"),
			ListenersEnabled: getbool("RABBITMQ_LISTENERS_ENABLED", true),
			// Defaults espejo del baseline Java (prefetch 50, concurrency 4-8):
			// el persist es I/O-bound a MySQL, no CPU-bound, asi que la
			// concurrencia -no mas cores- es lo que sube el throughput.
			Prefetch: getint("RABBITMQ_LISTENER_PREFETCH", 50, &errs),
			Workers:  getint("RABBITMQ_LISTENER_CONCURRENCY", 8, &errs),
		},
		MySQL: MySQL{
			DSN: buildDSN(mysqlUser, mysqlPass, mysqlHost, mysqlPort, mysqlDB),
			// Tope del pool alineado a la concurrencia del consumer + margen para
			// la query API / health / gRPC que comparten el mismo *sql.DB.
			MaxOpenConns: getint("MYSQL_MAX_OPEN_CONNS", 16, &errs),
			MaxIdleConns: getint("MYSQL_MAX_IDLE_CONNS", 8, &errs),
		},
	}

	// Secretos / requeridos.
	if cfg.RabbitMQ.Password == "" {
		errs = append(errs, "RABBITMQ_PASSWORD requerido")
	}
	if mysqlUser == "" {
		errs = append(errs, "MYSQL_USER requerido")
	}
	if mysqlPass == "" {
		errs = append(errs, "MYSQL_PASSWORD requerido")
	}

	if len(errs) > 0 {
		return nil, fmt.Errorf("config invalida:\n  - %s", strings.Join(errs, "\n  - "))
	}
	return cfg, nil
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getbool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}

func getint(key string, def int, errs *[]string) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		*errs = append(*errs, fmt.Sprintf("%s debe ser entero, got %q", key, v))
		return def
	}
	return n
}

func hostnameOr(def string) string {
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	return def
}
