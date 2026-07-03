package observability

import (
	"net/http/httptest"
	"strings"
	"testing"

	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// stubHealth registra un servicio real para que InitializeMetrics tenga metodos
// que preinicializar (el interceptor solo emite series de metodos registrados).
type stubHealth struct {
	healthpb.UnimplementedHealthServer
}

func TestGRPCServerMetricsExposed(t *testing.T) {
	m := NewMetrics()
	srv := grpc.NewServer(grpc.UnaryInterceptor(m.GRPCServer.UnaryServerInterceptor()))
	healthpb.RegisterHealthServer(srv, &stubHealth{})
	m.GRPCServer.InitializeMetrics(srv)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/metrics", nil)
	m.Handler().ServeHTTP(rec, req)
	body := rec.Body.String()
	if !strings.Contains(body, "grpc_server_") {
		t.Fatalf("/metrics no expone grpc_server_*; got:\n%s", body[:min(len(body), 400)])
	}
}
