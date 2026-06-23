// Package consul registra el servicio en Consul con health check HTTP y
// heartbeat periodico, y lo deregistra en el shutdown (CLAUDE.md 1.5, M5).
// Port de ConsulAutoRegistrar del baseline Java.
package consul

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"
)

// Config son los parametros de registro.
type Config struct {
	BaseURL         string
	Token           string
	ServiceName     string
	ServiceIP       string // si vacio, autodetecta IPv4
	ServicePort     int
	HealthPath      string
	Interval        time.Duration // cada cuanto Consul chequea /health
	Heartbeat       time.Duration // cada cuanto re-registramos
	DeregisterAfter time.Duration
	Version         string // tag version-
	Env             string // tag env-
}

type Registrar struct {
	cfg       Config
	http      *http.Client
	log       *slog.Logger
	serviceID string
	ip        string
}

func New(cfg Config, log *slog.Logger) *Registrar {
	return &Registrar{
		cfg:  cfg,
		http: &http.Client{Timeout: 5 * time.Second},
		log:  log,
	}
}

// Run registra al inicio y mantiene heartbeat hasta que ctx se cancele; al
// terminar, deregistra. Bloquea: ejecutar en goroutine.
func (r *Registrar) Run(ctx context.Context) {
	r.ip = resolveIP(r.cfg.ServiceIP)
	r.serviceID = buildServiceID(r.cfg.ServiceName, r.ip, r.cfg.ServicePort)

	if err := r.register(ctx); err != nil {
		r.log.Warn("consul register inicial fallo", slog.String("error", err.Error()))
	} else {
		r.log.Info("consul register ok", slog.String("service_id", r.serviceID))
	}

	t := time.NewTicker(r.cfg.Heartbeat)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			r.deregister()
			return
		case <-t.C:
			if err := r.register(ctx); err != nil {
				r.log.Warn("consul heartbeat fallo", slog.String("error", err.Error()))
			}
		}
	}
}

func (r *Registrar) register(ctx context.Context) error {
	body := map[string]any{
		"ID":      r.serviceID,
		"Name":    r.cfg.ServiceName,
		"Address": r.ip,
		"Port":    r.cfg.ServicePort,
		"Tags": []string{
			"version-" + r.cfg.Version,
			"lang-go",
			"env-" + r.cfg.Env,
		},
		"Check": map[string]any{
			"HTTP":                           fmt.Sprintf("http://%s:%d%s", r.ip, r.cfg.ServicePort, r.cfg.HealthPath),
			"Interval":                       durStr(r.cfg.Interval),
			"DeregisterCriticalServiceAfter": durStr(r.cfg.DeregisterAfter),
		},
	}
	payload, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut,
		r.cfg.BaseURL+"/v1/agent/service/register", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	r.auth(req)

	res, err := r.http.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode/100 != 2 {
		return fmt.Errorf("status %d", res.StatusCode)
	}
	return nil
}

func (r *Registrar) deregister() {
	if r.serviceID == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPut,
		r.cfg.BaseURL+"/v1/agent/service/deregister/"+r.serviceID, nil)
	if err != nil {
		return
	}
	r.auth(req)
	res, err := r.http.Do(req)
	if err != nil {
		r.log.Warn("consul deregister error", slog.String("error", err.Error()))
		return
	}
	defer res.Body.Close()
	r.log.Info("consul deregister", slog.String("service_id", r.serviceID), slog.Int("status", res.StatusCode))
}

func (r *Registrar) auth(req *http.Request) {
	if r.cfg.Token != "" {
		req.Header.Set("X-Consul-Token", r.cfg.Token)
	}
}

// buildServiceID = name-ip-port saneado (CLAUDE.md M5).
func buildServiceID(name, ip string, port int) string {
	raw := fmt.Sprintf("%s-%s-%d", name, ip, port)
	return sanitize(raw)
}

func sanitize(s string) string {
	var b strings.Builder
	for _, c := range s {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '.' || c == '-' {
			b.WriteRune(c)
		} else {
			b.WriteRune('-')
		}
	}
	return b.String()
}

// resolveIP devuelve la IP configurada o autodetecta la primera IPv4 no-loopback.
func resolveIP(configured string) string {
	if strings.TrimSpace(configured) != "" {
		return strings.TrimSpace(configured)
	}
	ifaces, err := net.Interfaces()
	if err == nil {
		for _, ni := range ifaces {
			if ni.Flags&net.FlagUp == 0 || ni.Flags&net.FlagLoopback != 0 {
				continue
			}
			addrs, _ := ni.Addrs()
			for _, a := range addrs {
				if ipnet, ok := a.(*net.IPNet); ok {
					if v4 := ipnet.IP.To4(); v4 != nil && !v4.IsLoopback() && !v4.IsLinkLocalUnicast() {
						return v4.String()
					}
				}
			}
		}
	}
	return "127.0.0.1"
}

// durStr formatea una duracion al estilo Consul ("10s").
func durStr(d time.Duration) string {
	return d.String()
}
