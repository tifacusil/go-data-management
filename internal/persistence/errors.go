package persistence

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"net"

	"github.com/go-sql-driver/mysql"
)

// RetryableError marca un fallo transitorio de DB: el consumidor debe reintentar
// (backoff acotado) en lugar de mandar a DLQ (CLAUDE.md 2.2, 4).
type RetryableError struct{ Err error }

func (e *RetryableError) Error() string { return "retryable: " + e.Err.Error() }
func (e *RetryableError) Unwrap() error { return e.Err }

// NonRetryableError marca un fallo permanente: reintentar no ayuda, va a DLQ.
type NonRetryableError struct{ Err error }

func (e *NonRetryableError) Error() string { return "non-retryable: " + e.Err.Error() }
func (e *NonRetryableError) Unwrap() error { return e.Err }

// classifyDBError decide si un error de DB es transitorio (retryable) o no.
// Espejo de PersistenceService.wrapDbException del baseline Java.
func classifyDBError(err error) error {
	if err == nil {
		return nil
	}

	// Errores de red / conexion -> transitorios.
	var netErr net.Error
	if errors.As(err, &netErr) {
		return &RetryableError{Err: err}
	}
	if errors.Is(err, sql.ErrConnDone) || errors.Is(err, driver.ErrBadConn) {
		return &RetryableError{Err: err}
	}

	// Codigos MySQL transitorios conocidos.
	var myErr *mysql.MySQLError
	if errors.As(err, &myErr) {
		if transientMySQLCodes[myErr.Number] {
			return &RetryableError{Err: err}
		}
		return &NonRetryableError{Err: err}
	}

	// Desconocido: tratar como no-recuperable para no reintentar en bucle.
	return &NonRetryableError{Err: err}
}

// transientMySQLCodes: errores que valen un reintento (deadlock, lock timeout,
// conexion perdida, too many connections, server gone).
// Ref: https://dev.mysql.com/doc/mysql-errors/
var transientMySQLCodes = map[uint16]bool{
	1040: true, // ER_CON_COUNT_ERROR (too many connections)
	1205: true, // ER_LOCK_WAIT_TIMEOUT
	1213: true, // ER_LOCK_DEADLOCK
	2002: true, // CR_CONNECTION_ERROR
	2003: true, // CR_CONN_HOST_ERROR
	2006: true, // CR_SERVER_GONE_ERROR
	2013: true, // CR_SERVER_LOST
}
