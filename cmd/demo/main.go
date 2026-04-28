package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"time"

	ledger "../.."
)

const demoDSN = "postgres://demo:demo@localhost:5432/mal_ledger?sslmode=disable"

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// This is intentionally a database/sql handle backed by a no-op connector.
	// It lets the appendix demonstrate dependency wiring without requiring a
	// live PostgreSQL server or a third-party driver in this minimal milestone.
	db := sql.OpenDB(noopConnector{dsn: demoDSN})
	defer db.Close()

	var service ledger.LedgerService = ledger.NewPostgresLedgerService(db)

	settlementAccountID := "00000000-0000-0000-0000-000000000001"
	aliceAccountID := "00000000-0000-0000-0000-0000000000a1"
	bobAccountID := "00000000-0000-0000-0000-0000000000b1"

	// In this minimal model, a deposit is represented as a transfer from an
	// internal settlement account into a user account. Production systems usually
	// reconcile this account against an external bank, card processor, or payment
	// network settlement file.
	deposit := ledger.PostTransactionRequest{
		SourceAccountID:      settlementAccountID,
		DestinationAccountID: aliceAccountID,
		Amount:               100_00,
		IdempotencyKey:       "demo-deposit-alice-10000",
	}

	transfer := ledger.PostTransactionRequest{
		SourceAccountID:      aliceAccountID,
		DestinationAccountID: bobAccountID,
		Amount:               25_00,
		IdempotencyKey:       "demo-transfer-alice-to-bob-2500",
	}

	duplicateTransfer := transfer

	fmt.Println("demo ledger service initialized with dummy database/sql DSN")
	demonstratePostTransaction(ctx, service, "Deposit", deposit, "settlement account funds Alice")
	demonstratePostTransaction(ctx, service, "Transfer", transfer, "Alice pays Bob")
	demonstratePostTransaction(ctx, service, "Duplicate Request", duplicateTransfer, "same payload and idempotency key safely replay instead of posting twice")
}

func demonstratePostTransaction(ctx context.Context, service ledger.LedgerService, label string, req ledger.PostTransactionRequest, purpose string) {
	// This function intentionally does not execute the call against the no-op
	// database. Against a real PostgreSQL connection, this is the exact call site:
	//
	//     err := service.PostTransaction(ctx, req)
	//
	// The duplicate request uses the same IdempotencyKey as the first transfer.
	// repository.go reserves that key through PostgreSQL's primary key, so a retry
	// cannot reach the debit/credit insert path twice.
	_ = ctx
	_ = service

	fmt.Printf("\n%s: %s\n", label, purpose)
	fmt.Printf("PostTransaction(ctx, ledger.PostTransactionRequest{SourceAccountID: %q, DestinationAccountID: %q, Amount: %d, IdempotencyKey: %q})\n",
		req.SourceAccountID,
		req.DestinationAccountID,
		req.Amount,
		req.IdempotencyKey,
	)
}

type noopConnector struct {
	dsn string
}

func (c noopConnector) Connect(context.Context) (driver.Conn, error) {
	return noopConn{dsn: c.dsn}, nil
}

func (c noopConnector) Driver() driver.Driver {
	return noopDriver{}
}

type noopDriver struct{}

func (noopDriver) Open(string) (driver.Conn, error) {
	return noopConn{}, nil
}

type noopConn struct {
	dsn string
}

func (noopConn) Prepare(string) (driver.Stmt, error) {
	return nil, fmt.Errorf("demo connection does not execute SQL")
}

func (noopConn) Close() error {
	return nil
}

func (noopConn) Begin() (driver.Tx, error) {
	return nil, fmt.Errorf("demo connection does not open transactions")
}

func (noopConn) Ping(context.Context) error {
	return nil
}
