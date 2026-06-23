// Package config carga y valida la configuracion del servicio desde variables
// de entorno al arranque. Fail-fast: si falta un secreto requerido o un valor
// es invalido, el binario no arranca (CLAUDE.md 1.7).
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config es la configuracion completa del servicio, resuelta una sola vez al boot.
type Config struct {
	Service  Service
	HTTP     HTTP
	RabbitMQ RabbitMQ
	MySQL    MySQL
	Consul   Consul
}

type Service struct {
	Name       string // nombre logico del servicio
	InstanceID string // id de instancia para logs/metricas
	Env        string // dev|staging|prod
}

type HTTP struct {
	Port int
}

type RabbitMQ struct {
	Host            string
	Port            int
	Username        string
	Password        string // secreto requerido
	Queue           string
	ListenersEnabled bool
}

type MySQL struct {
	// DSN se construye desde MYSQL_HOST/PORT/DATABASE/USER/PASSWORD (convencion
	// de tesis-infra). Incluye parseTime=true&loc=UTC fijos.
	DSN string
}

// buildDSN arma el DSN de go-sql-driver/mysql desde partes.
func buildDSN(user, pass, host string, port int, db string) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&loc=UTC", user, pass, host, port, db)
}

type Consul struct {
	Enabled         bool
	BaseURL         string
	Token           string
	ServiceName     string
	ServiceIP       string
	HealthPath      string
	Interval        time.Duration
	Heartbeat       time.Duration
	DeregisterAfter time.Duration
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

	// Consul: HOST/PORT -> base URL.
	consulURL := fmt.Sprintf("http://%s:%d", getenv("CONSUL_HOST", "localhost"), getint("CONSUL_PORT", 8500, &errs))

	cfg := &Config{
		Service: Service{
			Name:       getenv("SERVICE_NAME", "data-management"),
			InstanceID: getenv("INSTANCE_ID", hostnameOr("data-management-local")),
			Env:        getenv("ENV", "dev"),
		},
		HTTP: HTTP{
			Port: getint("HTTP_PORT", 8080, &errs),
		},
		RabbitMQ: RabbitMQ{
			Host:             getenv("RABBITMQ_HOST", "localhost"),
			Port:             getint("RABBITMQ_PORT", 5672, &errs),
			Username:         getenv("RABBITMQ_USER", "data_management"),
			Password:         os.Getenv("RABBITMQ_PASSWORD"),
			Queue:            getenv("RABBITMQ_QUEUE", "data_management.persist"),
			ListenersEnabled: getbool("RABBITMQ_LISTENERS_ENABLED", true),
		},
		MySQL: MySQL{
			DSN: buildDSN(mysqlUser, mysqlPass, mysqlHost, mysqlPort, mysqlDB),
		},
		Consul: Consul{
			Enabled:         getbool("CONSUL_ENABLED", true),
			BaseURL:         consulURL,
			Token:           os.Getenv("CONSUL_TOKEN"),
			ServiceName:     getenv("CONSUL_SERVICE_NAME", "data-management"),
			ServiceIP:       os.Getenv("SERVICE_IP"),
			HealthPath:      getenv("CONSUL_HEALTH_PATH", "/health"),
			Interval:        getdur("CONSUL_INTERVAL", 10*time.Second, &errs),
			Heartbeat:       getdur("CONSUL_HEARTBEAT", 10*time.Second, &errs),
			DeregisterAfter: getdur("CONSUL_DEREGISTER_AFTER", 30*time.Second, &errs),
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

func getdur(key string, def time.Duration, errs *[]string) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		*errs = append(*errs, fmt.Sprintf("%s debe ser duracion (ej. 10s), got %q", key, v))
		return def
	}
	return d
}

func hostnameOr(def string) string {
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	return def
}
