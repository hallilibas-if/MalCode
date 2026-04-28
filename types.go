// Package ledger defines the public domain contract for the minimal
// double-entry ledger service.
//
// This package is intentionally small and dependency-light. It owns the core
// financial vocabulary shared by API handlers, service implementations, and
// tests, but it does not know how PostgreSQL queries are executed. Keeping this
// boundary clean lets later milestones add storage code without forcing callers
// to depend on a concrete database implementation.
package ledger

import (
	"context"
	"encoding/json"
	"time"
)

// EntryType describes which side of the ledger an entry belongs to.
//
// The database enforces the same values with a CHECK constraint. Modeling them
// as constants in Go keeps service code from scattering raw strings and makes
// invalid double-entry construction easier to catch in review and tests.
type EntryType string

const (
	// EntryTypeDebit decreases an account's dynamically calculated balance.
	EntryTypeDebit EntryType = "DEBIT"

	// EntryTypeCredit increases an account's dynamically calculated balance.
	EntryTypeCredit EntryType = "CREDIT"
)

// Account is the Go representation of the accounts table.
//
// IDs are represented as strings at the contract boundary even though
// PostgreSQL stores them as UUIDs. This keeps the domain package free of UUID
// library dependencies until the persistence layer needs concrete parsing.
type Account struct {
	ID        string    `json:"id" db:"id"`
	OwnerID   string    `json:"owner_id" db:"owner_id"`
	Currency  string    `json:"currency" db:"currency"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
}

// Entry is an immutable ledger row from the entries table.
//
// A logical transfer is represented by two entries with the same TransactionID:
// one debit on the source account and one credit on the destination account.
// Amount is stored in minor units, such as cents, to avoid floating-point
// rounding errors in financial calculations.
type Entry struct {
	ID            string    `json:"id" db:"id"`
	AccountID     string    `json:"account_id" db:"account_id"`
	TransactionID string    `json:"transaction_id" db:"transaction_id"`
	Amount        int64     `json:"amount" db:"amount"`
	Type          EntryType `json:"type" db:"type"`
	CreatedAt     time.Time `json:"created_at" db:"created_at"`
}

// IdempotencyKey is the Go representation of the idempotency_keys table.
//
// The nullable response fields are nil while the first request is still being
// processed. After the ledger transaction commits, they are populated so safe
// client retries can replay the original outcome instead of posting duplicate
// ledger entries.
type IdempotencyKey struct {
	Key                string           `json:"key" db:"key"`
	RequestHash        string           `json:"request_hash" db:"request_hash"`
	ResponseStatusCode *int             `json:"response_status_code,omitempty" db:"response_status_code"`
	ResponsePayload    *json.RawMessage `json:"response_payload,omitempty" db:"response_payload"`
	CreatedAt          time.Time        `json:"created_at" db:"created_at"`
	CompletedAt        *time.Time       `json:"completed_at,omitempty" db:"completed_at"`
}

// CreateAccountRequest is the strict API payload for opening a ledger account.
//
// OwnerID is the external customer, wallet, merchant, or user identifier. The
// service keeps it as an opaque string so the ledger remains independent from a
// specific identity provider. Currency must be an ISO-4217 uppercase code; the
// service and database schema enforce that rule outside this data contract.
type CreateAccountRequest struct {
	OwnerID  string `json:"owner_id"`
	Currency string `json:"currency"`
}

// PostTransactionRequest is the strict API payload for a two-sided transfer.
//
// Amount is a positive minor-unit integer. IdempotencyKey is required for every
// write so callers can safely retry after network timeouts without risking a
// duplicate debit or credit.
type PostTransactionRequest struct {
	SourceAccountID      string `json:"source_account_id"`
	DestinationAccountID string `json:"destination_account_id"`
	Amount               int64  `json:"amount"`
	IdempotencyKey       string `json:"idempotency_key"`
}

// BalanceResponse returns the current account balance calculated from entries.
//
// Balances are not cached on the account model in this minimal service. The
// implementation should calculate Balance as sum(credits) - sum(debits) for the
// requested account so the response reflects the immutable ledger history.
type BalanceResponse struct {
	AccountID string `json:"account_id"`
	Currency  string `json:"currency"`
	Balance   int64  `json:"balance"`
}

// LedgerService defines the business contract for the ledger.
//
// The interface exists to decouple callers from the PostgreSQL implementation.
// API handlers, background workers, and tests can depend on this contract while
// the concrete storage-backed service remains replaceable. That separation makes
// unit tests deterministic through mocks or fakes, keeps database transaction
// details out of HTTP code, and leaves room for future persistence changes
// without changing the public service boundary.
//
// Every method accepts context.Context so callers can propagate deadlines,
// cancellations, request-scoped tracing, and shutdown signals into the eventual
// database implementation.
type LedgerService interface {
	// CreateAccount creates a single-currency account owned by req.OwnerID.
	CreateAccount(ctx context.Context, req CreateAccountRequest) (*Account, error)

	// PostTransaction atomically records one debit and one credit entry.
	PostTransaction(ctx context.Context, req PostTransactionRequest) error

	// FetchBalance calculates and returns the current balance for accountID.
	FetchBalance(ctx context.Context, accountID string) (*BalanceResponse, error)
}
