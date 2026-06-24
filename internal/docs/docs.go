// Package docs sirve la especificación OpenAPI y una UI de Swagger.
//
// El spec YAML se embebe en el binario con go:embed. go:embed no permite rutas
// con "..", así que se embebe la copia local internal/docs/openapi.yaml; la
// fuente canónica es contracts/openapi/data-management.yaml (mantener en sync).
// Los assets de Swagger UI se cargan desde un CDN, por lo que no se añaden
// dependencias nuevas al módulo.
package docs

import (
	_ "embed"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
)

//go:embed openapi.yaml
var openapiSpec []byte

// swaggerUI carga JS/CSS desde el CDN y apunta al spec con una URL RELATIVA
// ("openapi.yaml"), por lo que resuelve contra el directorio actual. Esto la
// hace robusta tras cualquier reverse proxy / prefix (p.ej. /api/v1/data/docs/)
// sin recompilar; por eso la UI debe servirse siempre con barra final (/docs/).
const swaggerUI = `<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <title>data-management-service — API docs</title>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui.css">
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js" crossorigin></script>
  <script>
    window.onload = () => {
      window.ui = SwaggerUIBundle({
        url: "openapi.yaml",
        dom_id: "#swagger-ui",
      });
    };
  </script>
</body>
</html>`

// Routes devuelve el sub-router de documentación. Montarlo en /docs:
//
//	r.Mount("/docs", docs.Routes())
//
// expone GET /docs (UI) y GET /docs/openapi.yaml (spec).
func Routes() chi.Router {
	r := chi.NewRouter()
	r.Get("/", serveUI)
	r.Get("/openapi.yaml", serveSpec)
	return r
}

func serveUI(w http.ResponseWriter, r *http.Request) {
	// La UI usa una URL RELATIVA para el spec ("openapi.yaml"), que el navegador
	// resuelve contra el directorio actual. Para que la base sea /docs/ (y no la
	// raíz), la página debe servirse con barra final. Si llega sin barra,
	// redirigimos con un Location RELATIVO ("docs/"): así el navegador lo
	// resuelve contra su URL pública actual y se preserva cualquier prefix del
	// gateway (p.ej. /api/v1/data/docs/). r.RequestURI es el path tal cual llegó
	// por HTTP (sin el strip de chi.Mount), así refleja /docs vs /docs/.
	p := r.RequestURI
	if i := strings.IndexByte(p, '?'); i >= 0 {
		p = p[:i]
	}
	if p == "" || p[len(p)-1] != '/' {
		w.Header().Set("Location", lastSegment(p)+"/")
		w.WriteHeader(http.StatusMovedPermanently)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(swaggerUI))
}

// lastSegment devuelve el último segmento de un path (sin barras), p.ej.
// "/docs" -> "docs". Se usa para construir un redirect relativo.
func lastSegment(p string) string {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] == '/' {
			return p[i+1:]
		}
	}
	return p
}

func serveSpec(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/yaml")
	_, _ = w.Write(openapiSpec)
}
