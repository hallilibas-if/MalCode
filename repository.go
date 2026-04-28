package ledger

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"
)

const (
	defaultSerializableAttempts  = 5
	postgresSerializationFailure = "40001"
)

var (
	// ErrNilDatabase is returned when the service is used without a database handle.
	ErrNilDatabase = errors.New("ledger: nil database")

	// ErrInvalidAccountID is returned when an account identifier is empty.
	ErrInvalidAccountID = errors.New("ledger: account id is required")

	// ErrInvalidAccountOwner is returned when account creation omits the owner id.
	ErrInvalidAccountOwner = errors.New("ledger: owner_id is required")

	// ErrInvalidCurrency is returned when account creation omits the currency code.
	ErrInvalidCurrency = errors.New("ledger: currency is required")

	// ErrInvalidAmount is returned when a transaction amount is zero or negative.
	ErrInvalidAmount = errors.New("ledger: amount must be positive")

	// ErrSameAccountTransfer prevents a transfer from debiting and crediting the same account.
	ErrSameAccountTransfer = errors.New("ledger: source and destination accounts must differ")

	// ErrMissingIdempotencyKey is returned when a write request cannot be safely retried.
	ErrMissingIdempotencyKey = errors.New("ledger: idempotency_key is required")

	// ErrAccountNotFound is returned when a requested account does not exist.
	ErrAccountNotFound = errors.New("ledger: account not found")

	// ErrCurrencyMismatch is returned when the two transfer accounts use different currencies.
	ErrCurrencyMismatch = errors.New("ledger: source and destination currencies differ")

	// ErrInsufficientFunds is returned when the source balance cannot cover the debit.
	ErrInsufficientFunds = errors.New("ledger: insufficient funds")

	// ErrIdempotencyConflict is returned when a client reuses a key for a different request.
	ErrIdempotencyConflict = errors.New("ledger: idempotency key reused with a different request")

	// ErrIdempotencyInProgress is returned when a matching idempotency row exists but is incomplete.
	ErrIdempotencyInProgress = errors.New("ledger: idempotent request is already in progress")
)

// PostgresLedgerService is the PostgreSQL-backed implementation of LedgerService.
//
// The service intentionally depends only on *sql.DB. The application entrypoint
// is responsible for importing and registering the concrete PostgreSQL driver.
// This keeps the domain package free from driver-specific coupling while still
// allowing us to use database/sql transactions and context-aware queries.
type PostgresLedgerService struct {
	db *sql.DB
}

var _ LedgerService = (*PostgresLedgerService)(nil)

// NewPostgresLedgerService constructs a LedgerService backed by PostgreSQL.
func NewPostgresLedgerService(db *sql.DB) *PostgresLedgerService {
	return &PostgresLedgerService{db: db}
}

// CreateAccount inserts a new single-currency ledger account.
func (s *PostgresLedgerService) CreateAccount(ctx context.Context, req CreateAccountRequest) (*Account, error) {
	if err := s.validateDB(); err != nil {
		return nil, err
	}
	if req.OwnerID == "" {
		return nil, ErrInvalidAccountOwner
	}
	if req.Currency == "" {
		return nil, ErrInvalidCurrency
	}

	const query = `
		INSERT INTO accounts (owner_id, currency)
		VALUES ($1, $2)
		RETURNING id::text, owner_id, currency, created_at
	`

	var account Account
	err := s.db.QueryRowContext(ctx, query, req.OwnerID, req.Currency).
		Scan(&account.ID, &account.OwnerID, &account.Currency, &account.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("create account: %w", err)
	}

	return &account, nil
}

// PostTransaction atomically moves funds from SourceAccountID to DestinationAccountID.
//
// Each successful call writes exactly two immutable entries sharing one
// transaction_id: a DEBIT on the source account and a CREDIT on the destination
// account. The idempotency key is recorded in the same database transaction so a
// successful retry cannot post a duplicate transfer.
func (s *PostgresLedgerService) PostTransaction(ctx context.Context, req PostTransactionRequest) error {
	if err := s.validateDB(); err != nil {
		return err
	}
	if err := validatePostTransactionRequest(req); err != nil {
		return err
	}

	requestHash, err := hashPostTransactionRequest(req)
	if err != nil {
		return err
	}

	return s.execSerializableWithRetry(ctx, func(tx *sql.Tx) error {
		shouldPost, err := ensureIdempotencyKey(ctx, tx, req.IdempotencyKey, requestHash)
		if err != nil {
			return err
		}
		if !shouldPost {
			return nil
		}

		sourceAccount, destinationAccount, err := lockTransferAccounts(ctx, tx, req.SourceAccountID, req.DestinationAccountID)
		if err != nil {
			return err
		}
		if sourceAccount.Currency != destinationAccount.Currency {
			return ErrCurrencyMismatch
		}

		sourceBalance, err := calculateBalanceForUpdate(ctx, tx, sourceAccount.ID)
		if err != nil {
			return err
		}
		if sourceBalance < req.Amount {
			return ErrInsufficientFunds
		}

		transactionID, err := createTransactionID(ctx, tx)
		if err != nil {
			return err
		}

		if err := insertDoubleEntry(ctx, tx, transactionID, req); err != nil {
			return err
		}

		return completeIdempotencyKey(ctx, tx, req.IdempotencyKey, transactionID)
	})
}

// FetchBalance calculates an account balance from immutable ledger entries.
//
// The accounts table deliberately has no cached balance column in this minimal
// schema. Reading the balance from entries keeps the source of truth auditable:
// credits increase the balance, debits decrease it.
func (s *PostgresLedgerService) FetchBalance(ctx context.Context, accountID string) (*BalanceResponse, error) {
	if err := s.validateDB(); err != nil {
		return nil, err
	}
	if accountID == "" {
		return nil, ErrInvalidAccountID
	}

	const query = `
		SELECT
			a.id::text,
			a.currency,
			COALESCE(
				SUM(
					CASE
						WHEN e.type = 'CREDIT' THEN e.amount
						WHEN e.type = 'DEBIT' THEN -e.amount
						ELSE 0
					END
				),
				0
			)::bigint AS balance
		FROM accounts a
		LEFT JOIN entries e ON e.account_id = a.id
		WHERE a.id = $1::uuid
		GROUP BY a.id, a.currency
	`

	var balance BalanceResponse
	err := s.db.QueryRowContext(ctx, query, accountID).
		Scan(&balance.AccountID, &balance.Currency, &balance.Balance)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrAccountNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("fetch balance: %w", err)
	}

	return &balance, nil
}

func (s *PostgresLedgerService) validateDB() error {
	if s == nil || s.db == nil {
		return ErrNilDatabase
	}
	return nil
}

func validatePostTransactionRequest(req PostTransactionRequest) error {
	if req.SourceAccountID == "" || req.DestinationAccountID == "" {
		return ErrInvalidAccountID
	}
	if req.SourceAccountID == req.DestinationAccountID {
		return ErrSameAccountTransfer
	}
	if req.Amount <= 0 {
		return ErrInvalidAmount
	}
	if req.IdempotencyKey == "" {
		return ErrMissingIdempotencyKey
	}
	return nil
}

func (s *PostgresLedgerService) execSerializableWithRetry(ctx context.Context, fn func(tx *sql.Tx) error) error {
	var lastErr error
	for attempt := 0; attempt < defaultSerializableAttempts; attempt++ {
		lastErr = s.execSerializableOnce(ctx, fn)
		if lastErr == nil {
			return nil
		}
		if !hasPostgresCode(lastErr, postgresSerializationFailure) {
			return lastErr
		}
		if attempt < defaultSerializableAttempts-1 {
			if err := sleepWithContext(ctx, retryDelay(attempt)); err != nil {
				return err
			}
		}
	}

	return fmt.Errorf("serializable transaction failed after %d attempts: %w", defaultSerializableAttempts, lastErr)
}

func (s *PostgresLedgerService) execSerializableOnce(ctx context.Context, fn func(tx *sql.Tx) error) error {
	// Serializable isolation makes PostgreSQL prove that concurrent ledger
	// transactions are equivalent to some one-at-a-time order. That matters for
	// financial transfers because weaker isolation can allow write skew or stale
	// aggregate reads when multiple requests spend from the same account. When
	// PostgreSQL cannot prove a safe order, it returns SQLSTATE 40001 and the
	// caller retries the whole transaction from the beginning.
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := fn(tx); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit ledger transaction: %w", err)
	}

	return nil
}

func ensureIdempotencyKey(ctx context.Context, tx *sql.Tx, key string, requestHash string) (bool, error) {
	const insertQuery = `
		INSERT INTO idempotency_keys (key, request_hash)
		VALUES ($1, $2)
		ON CONFLICT (key) DO NOTHING
	`

	result, err := tx.ExecContext(ctx, insertQuery, key, requestHash)
	if err != nil {
		return false, fmt.Errorf("insert idempotency key: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("check idempotency insert: %w", err)
	}
	if rowsAffected == 1 {
		return true, nil
	}

	const selectQuery = `
		SELECT request_hash, completed_at
		FROM idempotency_keys
		WHERE key = $1
		FOR UPDATE
	`

	var existingHash string
	var completedAt sql.NullTime
	err = tx.QueryRowContext(ctx, selectQuery, key).Scan(&existingHash, &completedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return false, ErrMissingIdempotencyKey
	}
	if err != nil {
		return false, fmt.Errorf("read idempotency key: %w", err)
	}
	if existingHash != requestHash {
		return false, ErrIdempotencyConflict
	}
	if !completedAt.Valid {
		return false, ErrIdempotencyInProgress
	}

	return false, nil
}

func completeIdempotencyKey(ctx context.Context, tx *sql.Tx, key string, transactionID string) error {
	payload, err := json.Marshal(map[string]string{"transaction_id": transactionID})
	if err != nil {
		return fmt.Errorf("marshal idempotency response: %w", err)
	}

	const query = `
		UPDATE idempotency_keys
		SET
			response_status_code = 201,
			response_payload = $2::jsonb,
			completed_at = now()
		WHERE key = $1
	`

	if _, err := tx.ExecContext(ctx, query, key, payload); err != nil {
		return fmt.Errorf("complete idempotency key: %w", err)
	}
	return nil
}

func lockTransferAccounts(ctx context.Context, tx *sql.Tx, sourceAccountID string, destinationAccountID string) (Account, Account, error) {
	firstID, secondID := sourceAccountID, destinationAccountID
	if secondID < firstID {
		firstID, secondID = secondID, firstID
	}

	firstAccount, err := lockAccount(ctx, tx, firstID)
	if err != nil {
		return Account{}, Account{}, err
	}
	secondAccount, err := lockAccount(ctx, tx, secondID)
	if err != nil {
		return Account{}, Account{}, err
	}

	if firstAccount.ID == sourceAccountID {
		return firstAccount, secondAccount, nil
	}
	return secondAccount, firstAccount, nil
}

func lockAccount(ctx context.Context, tx *sql.Tx, accountID string) (Account, error) {
	// SELECT ... FOR UPDATE takes a row-level lock on the account until commit.
	// In this ledger every balance-changing operation must lock the account row
	// before reading its dynamic entry balance and inserting new entries. That
	// prevents TOCTOU races where two concurrent transactions both read the same
	// spendable balance and then both debit it.
	const query = `
		SELECT id::text, owner_id, currency, created_at
		FROM accounts
		WHERE id = $1::uuid
		FOR UPDATE
	`

	var account Account
	err := tx.QueryRowContext(ctx, query, accountID).
		Scan(&account.ID, &account.OwnerID, &account.Currency, &account.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Account{}, ErrAccountNotFound
	}
	if err != nil {
		return Account{}, fmt.Errorf("lock account %s: %w", accountID, err)
	}

	return account, nil
}

func calculateBalanceForUpdate(ctx context.Context, tx *sql.Tx, sourceAccountID string) (int64, error) {
	// The source account row has already been selected FOR UPDATE before this
	// aggregate is read. That row lock is the service's per-account spending
	// mutex: every debit from the same account must acquire it before checking
	// funds and inserting entries. Without the lock, two concurrent requests can
	// both observe the same pre-debit balance and overdraw the account between
	// time-of-check and time-of-use.
	const query = `
		SELECT COALESCE(
			SUM(
				CASE
					WHEN type = 'CREDIT' THEN amount
					WHEN type = 'DEBIT' THEN -amount
					ELSE 0
				END
			),
			0
		)::bigint
		FROM entries
		WHERE account_id = $1::uuid
	`

	var balance int64
	if err := tx.QueryRowContext(ctx, query, sourceAccountID).Scan(&balance); err != nil {
		return 0, fmt.Errorf("calculate source balance: %w", err)
	}
	return balance, nil
}

func createTransactionID(ctx context.Context, tx *sql.Tx) (string, error) {
	var transactionID string
	if err := tx.QueryRowContext(ctx, `SELECT gen_random_uuid()::text`).Scan(&transactionID); err != nil {
		return "", fmt.Errorf("generate transaction id: %w", err)
	}
	return transactionID, nil
}

func insertDoubleEntry(ctx context.Context, tx *sql.Tx, transactionID string, req PostTransactionRequest) error {
	const query = `
		INSERT INTO entries (account_id, transaction_id, amount, type)
		VALUES
			($1::uuid, $2::uuid, $3, $4),
			($5::uuid, $2::uuid, $3, $6)
	`

	_, err := tx.ExecContext(
		ctx,
		query,
		req.SourceAccountID,
		transactionID,
		req.Amount,
		string(EntryTypeDebit),
		req.DestinationAccountID,
		string(EntryTypeCredit),
	)
	if err != nil {
		return fmt.Errorf("insert double-entry rows: %w", err)
	}
	return nil
}

func hashPostTransactionRequest(req PostTransactionRequest) (string, error) {
	canonical := struct {
		SourceAccountID      string `json:"source_account_id"`
		DestinationAccountID string `json:"destination_account_id"`
		Amount               int64  `json:"amount"`
	}{
		SourceAccountID:      req.SourceAccountID,
		DestinationAccountID: req.DestinationAccountID,
		Amount:               req.Amount,
	}

	payload, err := json.Marshal(canonical)
	if err != nil {
		return "", fmt.Errorf("marshal request hash: %w", err)
	}

	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func hasPostgresCode(err error, code string) bool {
	for err != nil {
		if sqlState, ok := err.(interface{ SQLState() string }); ok && sqlState.SQLState() == code {
			return true
		}
		if reflectedCode(err) == code {
			return true
		}
		err = errors.Unwrap(err)
	}
	return false
}

func reflectedCode(err error) string {
	value := reflect.ValueOf(err)
	if value.Kind() == reflect.Pointer {
		value = value.Elem()
	}
	if !value.IsValid() || value.Kind() != reflect.Struct {
		return ""
	}

	field := value.FieldByName("Code")
	if !field.IsValid() || field.Kind() != reflect.String {
		return ""
	}

	return field.String()
}

func retryDelay(attempt int) time.Duration {
	delay := 50 * time.Millisecond
	for i := 0; i < attempt; i++ {
		delay *= 2
		if delay >= time.Second {
			return time.Second
		}
	}
	return delay
}

func sleepWithContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
