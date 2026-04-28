-- Minimal PostgreSQL schema for a double-entry ledger service.
-- Amounts are stored as BIGINT minor units (for example, cents) to avoid
-- floating-point rounding errors. Application code is responsible for
-- interpreting the minor-unit scale for each currency.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS accounts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- External owner/customer/user identifier. Kept as TEXT so the ledger is
    -- not coupled to one identity provider or users table in this minimal build.
    owner_id TEXT NOT NULL,

    -- ISO-4217 currency code. Each account is single-currency; cross-currency
    -- movement should be modeled as separate ledger transactions plus FX logic.
    currency VARCHAR(3) NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT accounts_currency_iso_code_check
        CHECK (currency ~ '^[A-Z]{3}$')
);

CREATE TABLE IF NOT EXISTS entries (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- RESTRICT preserves ledger history: accounts with posted entries must not
    -- disappear and leave orphaned financial records.
    account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,

    -- Shared by the debit and credit rows that make up one logical transfer.
    -- The service must insert both rows atomically in one database transaction.
    transaction_id UUID NOT NULL,

    -- Positive-only minor units. A negative amount would allow callers to invert
    -- debit/credit semantics and corrupt balances; direction belongs in type.
    amount BIGINT NOT NULL,

    -- A row is intentionally single-sided: either DEBIT or CREDIT, never both.
    -- This keeps balance calculation explicit and auditable:
    -- credits increase an account, debits decrease an account.
    type TEXT NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT entries_amount_positive_check
        CHECK (amount > 0),

    CONSTRAINT entries_type_check
        CHECK (type IN ('DEBIT', 'CREDIT')),

    -- This minimal service models each transaction as exactly one debit row and
    -- one credit row inserted atomically by application code. The unique pair
    -- prevents accidentally posting two debits or two credits for the same
    -- transaction_id.
    CONSTRAINT entries_one_row_per_side_per_transaction_unique
        UNIQUE (transaction_id, type)
);

CREATE TABLE IF NOT EXISTS idempotency_keys (
    -- Client-supplied key for retry protection. PRIMARY KEY makes duplicate
    -- retries converge on the original result instead of posting twice.
    key VARCHAR(255) PRIMARY KEY,

    -- Hash of the canonical request body. Reusing the same key for a different
    -- request is a client error and must not return a cached response.
    request_hash TEXT NOT NULL,

    -- These are NULL while the first request is still processing, then populated
    -- once the ledger transaction commits and the response can be replayed.
    response_status_code INTEGER,

    response_payload JSONB,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,

    CONSTRAINT idempotency_keys_response_pair_check
        CHECK (
            (response_status_code IS NULL AND response_payload IS NULL AND completed_at IS NULL)
            OR
            (
                response_status_code BETWEEN 100 AND 599
                AND response_payload IS NOT NULL
                AND completed_at IS NOT NULL
            )
        )
);

-- Balance reads aggregate entries by account_id, so this index keeps
-- SUM(CASE type ...) queries targeted to one account's ledger rows.
CREATE INDEX IF NOT EXISTS idx_entries_account_id
    ON entries(account_id);

-- Transaction lookups need to fetch the linked debit and credit rows together.
CREATE INDEX IF NOT EXISTS idx_entries_transaction_id
    ON entries(transaction_id);

-- Useful for account statements and paginated ledger history.
CREATE INDEX IF NOT EXISTS idx_entries_account_created_at
    ON entries(account_id, created_at DESC);
