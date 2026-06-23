// Package migrations embebe los archivos SQL para poder aplicarlos
// programaticamente (al boot o en tests de integracion) sin depender del CLI.
package migrations

import "embed"

// FS contiene los *.up.sql / *.down.sql versionados.
//
//go:embed *.sql
var FS embed.FS
