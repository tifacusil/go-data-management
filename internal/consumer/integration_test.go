package consumer_test

import (
	"context"
	"database/sql"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/AngelloFD/go-data-management/internal/consumer"
	"github.com/AngelloFD/go-data-management/internal/contract"
	"github.com/AngelloFD/go-data-management/internal/persistence"
	"github.com/AngelloFD/go-data-management/internal/router"
	_ "github.com/go-sql-driver/mysql"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/testcontainers/testcontainers-go"
	tcmysql "github.com/testcontainers/testcontainers-go/modules/mysql"
	tcrabbit "github.com/testcontainers/testcontainers-go/modules/rabbitmq"
)

// TestConsumer_Integration levanta RabbitMQ + MySQL efimeros y verifica el
// flujo completo: mensaje valido -> persistido; mensaje invalido -> DLQ
// (CLAUDE.md 3.4). El retry transitorio se cubre en processor_test (unit).
func TestConsumer_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("integration: requiere Docker, saltado con -short")
	}
	ctx := context.Background()

	// --- MySQL ---
	mysqlC, err := tcmysql.Run(ctx, "mysql:8.0",
		tcmysql.WithDatabase("data_management"),
		tcmysql.WithUsername("data_management"),
		tcmysql.WithPassword("test"),
	)
	if err != nil {
		t.Fatalf("mysql: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(mysqlC) })

	dsn, err := mysqlC.ConnectionString(ctx, "parseTime=true", "loc=UTC", "multiStatements=true")
	if err != nil {
		t.Fatalf("dsn: %v", err)
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	waitDB(t, ctx, db)
	if err := persistence.Migrate(db); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	// --- RabbitMQ ---
	rabbitC, err := tcrabbit.Run(ctx, "rabbitmq:3.13-management")
	if err != nil {
		t.Fatalf("rabbitmq: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(rabbitC) })
	amqpURL, err := rabbitC.AmqpURL(ctx)
	if err != nil {
		t.Fatalf("amqp url: %v", err)
	}

	// --- declarar topologia (en prod la hace tesis-infra/definitions.json) ---
	setupConn, err := amqp.Dial(amqpURL)
	if err != nil {
		t.Fatalf("setup dial: %v", err)
	}
	setupCh, err := setupConn.Channel()
	if err != nil {
		t.Fatalf("setup channel: %v", err)
	}
	if err := consumer.DeclareTopologyForTest(setupCh); err != nil {
		t.Fatalf("declarar topologia: %v", err)
	}
	_ = setupConn.Close()

	// --- wiring ---
	v, err := contract.NewValidator()
	if err != nil {
		t.Fatalf("validator: %v", err)
	}
	repo := persistence.New(db)
	rtr := router.New(repo)
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))

	cons, err := consumer.New(amqpURL, consumer.QueueMain, v, rtr, log, nil, nil)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	t.Cleanup(func() { cons.Close() })

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = cons.Run(runCtx) }()

	// --- publicar mensajes ---
	pubConn, err := amqp.Dial(amqpURL)
	if err != nil {
		t.Fatalf("pub dial: %v", err)
	}
	defer pubConn.Close()
	pubCh, err := pubConn.Channel()
	if err != nil {
		t.Fatalf("pub channel: %v", err)
	}

	valid := `{"schema_version":"1.0.0","message_id":"ok-1","project_id":"demo",
		"sensor_id":"R-1","sensor_type":"rainfall","timestamp":"2026-05-17T00:00:00Z",
		"received_at":"2026-05-17T00:00:01Z","payload":{"rainfall_mm":1.2}}`
	invalid := `{"sensor_type":"lava","message_id":"bad-1"}`

	publish(t, pubCh, valid)
	publish(t, pubCh, invalid)

	// --- assert: valido persistido ---
	if !eventually(t, 30*time.Second, func() bool {
		var n int
		_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM rainfall_readings WHERE message_id = ?`, "ok-1").Scan(&n)
		return n == 1
	}) {
		t.Fatal("mensaje valido no persistido")
	}

	// --- assert: invalido en DLQ ---
	// QueueDeclarePassive cierra el canal si falla; reabrir uno por sondeo.
	if !eventually(t, 30*time.Second, func() bool {
		ch, err := pubConn.Channel()
		if err != nil {
			return false
		}
		defer ch.Close()
		q, err := ch.QueueDeclarePassive(consumer.QueueDeadLetter, true, false, false, false, nil)
		if err != nil {
			return false
		}
		return q.Messages >= 1
	}) {
		t.Fatal("mensaje invalido no llego a dead_letter")
	}
}

func waitDB(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		if err := db.PingContext(ctx); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("mysql no respondio")
		}
		time.Sleep(time.Second)
	}
}

func publish(t *testing.T, ch *amqp.Channel, body string) {
	t.Helper()
	err := ch.PublishWithContext(context.Background(), "", consumer.QueueMain, false, false,
		amqp.Publishing{ContentType: "application/json", Body: []byte(body)})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
}

func eventually(t *testing.T, d time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(500 * time.Millisecond)
	}
	return false
}
