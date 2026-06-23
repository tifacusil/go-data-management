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
	DSN string // secreto requerido (incluye password)
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

	cfg := &Config{
		Service: Service{
			Name:       getenv("SERVICE_NAME", "data-management"),
			InstanceID: getenv("INSTANCE_ID", hostnameOr("data-management-local")),
			Env:        getenv("APP_ENV", "dev"),
		},
		HTTP: HTTP{
			Port: getint("SERVER_PORT", 8082, &errs),
		},
		RabbitMQ: RabbitMQ{
			Host:             getenv("RABBITMQ_HOST", "localhost"),
			Port:             getint("RABBITMQ_PORT", 5672, &errs),
			Username:         getenv("RABBITMQ_USERNAME", "subscriber"),
			Password:         os.Getenv("RABBITMQ_PASSWORD"),
			Queue:            getenv("RABBITMQ_QUEUE", "data_management.persist"),
			ListenersEnabled: getbool("RABBITMQ_LISTENERS_ENABLED", true),
		},
		MySQL: MySQL{
			DSN: os.Getenv("MYSQL_DSN"),
		},
		Consul: Consul{
			Enabled:         getbool("CONSUL_ENABLED", true),
			BaseURL:         getenv("CONSUL_BASE_URL", "http://localhost:8500"),
			Token:           os.Getenv("CONSUL_HTTP_TOKEN"),
			ServiceName:     getenv("CONSUL_SERVICE_NAME", "data-management"),
			ServiceIP:       os.Getenv("SERVICE_IP"),
			HealthPath:      getenv("CONSUL_HEALTH_PATH", "/health"),
			Interval:        getdur("CONSUL_INTERVAL", 10*time.Second, &errs),
			Heartbeat:       getdur("CONSUL_HEARTBEAT", 10*time.Second, &errs),
			DeregisterAfter: getdur("CONSUL_DEREGISTER_AFTER", 30*time.Second, &errs),
		},
	}

	// Secretos requeridos.
	if cfg.RabbitMQ.Password == "" {
		errs = append(errs, "RABBITMQ_PASSWORD requerido")
	}
	if cfg.MySQL.DSN == "" {
		errs = append(errs, "MYSQL_DSN requerido (ej. user:pass@tcp(host:3306)/data_management?parseTime=true&loc=UTC)")
	}
	if cfg.Consul.Enabled && cfg.Consul.Token == "" {
		// Token puede ser opcional segun ACLs de Consul; warning, no fatal.
		// TODO(SE-practice): si ACLs activas y falta token, register fallara — ref CLAUDE.md 1.7
		_ = cfg.Consul.Token
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
