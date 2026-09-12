-- =============================================================================
-- PayCore payments-core Schema (Section 5.1)
-- Single ACID source of truth for merchants, payments, double-entry ledger,
-- authoritative idempotency keys, and transactional outbox.
-- All monetary amounts are BIGINT in minor units (paise).
-- =============================================================================

CREATE TABLE merchants (
    id              UUID PRIMARY KEY,
    name            VARCHAR(255) NOT NULL,
    api_key_hash    VARCHAR(64)  NOT NULL UNIQUE,
    webhook_url     TEXT,
    webhook_secret  TEXT,
    fee_bps         INTEGER      NOT NULL DEFAULT 200 CHECK (fee_bps >= 0 AND fee_bps <= 10000),
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE accounts (
    id              UUID PRIMARY KEY,
    merchant_id     UUID REFERENCES merchants(id),
    type            VARCHAR(32)  NOT NULL CHECK (type IN ('ASSET', 'LIABILITY', 'REVENUE', 'EXTERNAL')),
    currency        VARCHAR(3)   NOT NULL DEFAULT 'INR',
    name            VARCHAR(128) NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (merchant_id, name, currency)
);

CREATE UNIQUE INDEX idx_accounts_system_name_currency
    ON accounts (name, currency)
    WHERE merchant_id IS NULL;

CREATE TABLE account_balances (
    account_id      UUID PRIMARY KEY REFERENCES accounts(id),
    balance         BIGINT      NOT NULL DEFAULT 0,
    version         BIGINT      NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE payments (
    id              UUID PRIMARY KEY,
    merchant_id     UUID        NOT NULL REFERENCES merchants(id),
    amount          BIGINT      NOT NULL CHECK (amount > 0),
    currency        VARCHAR(3)  NOT NULL DEFAULT 'INR',
    status          VARCHAR(32) NOT NULL CHECK (status IN (
                        'CREATED',
                        'AUTHORIZING',
                        'AUTHORIZED',
                        'CAPTURED',
                        'VOIDED',
                        'FAILED'
                    )),
    captured_amount BIGINT      NOT NULL DEFAULT 0 CHECK (captured_amount >= 0),
    refunded_amount BIGINT      NOT NULL DEFAULT 0 CHECK (refunded_amount >= 0),
    psp_ref         VARCHAR(128),
    metadata        JSONB       NOT NULL DEFAULT '{}'::jsonb,
    version         BIGINT      NOT NULL DEFAULT 0,
    event_seq       BIGINT      NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_refunded_le_captured CHECK (refunded_amount <= captured_amount),
    CONSTRAINT chk_captured_le_amount   CHECK (captured_amount <= amount)
);

CREATE INDEX idx_payments_merchant_created ON payments (merchant_id, created_at DESC, id DESC);
CREATE INDEX idx_payments_stuck_authorizing ON payments (status, updated_at) WHERE status = 'AUTHORIZING';
CREATE INDEX idx_payments_psp_ref ON payments (psp_ref) WHERE psp_ref IS NOT NULL;

CREATE TABLE payment_attempts (
    id              UUID PRIMARY KEY,
    payment_id      UUID         NOT NULL REFERENCES payments(id),
    attempt_no      INTEGER      NOT NULL CHECK (attempt_no > 0),
    psp_ref         VARCHAR(128) NOT NULL UNIQUE,
    outcome         VARCHAR(32)  NOT NULL CHECK (outcome IN ('AUTHORIZED', 'DECLINED', 'TIMEOUT', 'ERROR')),
    latency_ms      BIGINT       NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (payment_id, attempt_no)
);

CREATE TABLE ledger_transactions (
    id              UUID PRIMARY KEY,
    payment_id      UUID REFERENCES payments(id),
    kind            VARCHAR(32)  NOT NULL CHECK (kind IN ('CAPTURE', 'REFUND', 'PAYOUT', 'REVERSAL')),
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    posted_by       VARCHAR(128) NOT NULL
);

CREATE INDEX idx_ledger_txns_payment_id ON ledger_transactions (payment_id) WHERE payment_id IS NOT NULL;

-- Partitioned by range on created_at (monthly partitions + default catch-all)
CREATE TABLE ledger_entries (
    id              UUID        NOT NULL,
    txn_id          UUID        NOT NULL REFERENCES ledger_transactions(id),
    account_id      UUID        NOT NULL REFERENCES accounts(id),
    direction       VARCHAR(16) NOT NULL CHECK (direction IN ('DEBIT', 'CREDIT')),
    amount          BIGINT      NOT NULL CHECK (amount > 0),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

CREATE INDEX idx_ledger_entries_txn_id ON ledger_entries (txn_id);
CREATE INDEX idx_ledger_entries_account_id ON ledger_entries (account_id, created_at DESC);

CREATE TABLE ledger_entries_y2026m09 PARTITION OF ledger_entries
    FOR VALUES FROM ('2026-09-01 00:00:00+00') TO ('2026-10-01 00:00:00+00');
CREATE TABLE ledger_entries_y2026m10 PARTITION OF ledger_entries
    FOR VALUES FROM ('2026-10-01 00:00:00+00') TO ('2026-11-01 00:00:00+00');
CREATE TABLE ledger_entries_y2026m11 PARTITION OF ledger_entries
    FOR VALUES FROM ('2026-11-01 00:00:00+00') TO ('2026-12-01 00:00:00+00');
CREATE TABLE ledger_entries_y2026m12 PARTITION OF ledger_entries
    FOR VALUES FROM ('2026-12-01 00:00:00+00') TO ('2027-01-01 00:00:00+00');
CREATE TABLE ledger_entries_default PARTITION OF ledger_entries DEFAULT;

-- 1. Append-only trigger on ledger_entries (Section 4.2 #6 & 5.1)
CREATE OR REPLACE FUNCTION reject_ledger_entry_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'ledger_entries is strictly append-only (% attempted on entry %). Post a reversing transaction instead.',
        TG_OP, OLD.id
        USING ERRCODE = '23000';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_ledger_entries_append_only
    BEFORE UPDATE OR DELETE ON ledger_entries
    FOR EACH ROW
    EXECUTE FUNCTION reject_ledger_entry_mutation();

-- 2. Deferred constraint trigger enforcing SUM(DEBIT) = SUM(CREDIT) per ledger transaction at commit time (Section 5.1)
CREATE OR REPLACE FUNCTION verify_ledger_txn_balanced()
RETURNS TRIGGER AS $$
DECLARE
    v_debits  BIGINT;
    v_credits BIGINT;
    v_count   INTEGER;
BEGIN
    SELECT
        COALESCE(SUM(CASE WHEN direction = 'DEBIT'  THEN amount ELSE 0 END), 0),
        COALESCE(SUM(CASE WHEN direction = 'CREDIT' THEN amount ELSE 0 END), 0),
        COUNT(*)
    INTO v_debits, v_credits, v_count
    FROM ledger_entries
    WHERE txn_id = NEW.txn_id;

    IF v_count < 2 THEN
        RAISE EXCEPTION 'Ledger transaction % has fewer than 2 entries (count=%)', NEW.txn_id, v_count
            USING ERRCODE = '23514';
    END IF;

    IF v_debits <> v_credits THEN
        RAISE EXCEPTION 'Unbalanced ledger transaction %: SUM(DEBIT)=% != SUM(CREDIT)=%',
            NEW.txn_id, v_debits, v_credits
            USING ERRCODE = '23514';
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER trg_ledger_txn_balanced
    AFTER INSERT ON ledger_entries
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW
    EXECUTE FUNCTION verify_ledger_txn_balanced();

-- Authoritative Idempotency Store (Section 5.1 & 5.2)
CREATE TABLE idempotency_keys (
    merchant_id     UUID         NOT NULL REFERENCES merchants(id),
    key             VARCHAR(128) NOT NULL,
    request_hash    VARCHAR(64)  NOT NULL,
    status          VARCHAR(32)  NOT NULL CHECK (status IN ('IN_PROGRESS', 'COMPLETED')),
    response_code   INTEGER,
    response_body   TEXT,
    locked_until    TIMESTAMPTZ  NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (merchant_id, key)
);

-- Transactional Outbox (Section 5.1), partitioned monthly by created_at
CREATE TABLE outbox_events (
    id              UUID         NOT NULL,
    aggregate_id    UUID         NOT NULL,
    event_seq       BIGINT       NOT NULL,
    event_type      VARCHAR(64)  NOT NULL,
    schema_version  INTEGER      NOT NULL DEFAULT 1,
    payload         BYTEA        NOT NULL,
    trace_context   JSONB        NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    published_at    TIMESTAMPTZ,
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

CREATE INDEX idx_outbox_unpublished
    ON outbox_events (created_at ASC, id ASC)
    WHERE published_at IS NULL;

CREATE INDEX idx_outbox_aggregate_seq
    ON outbox_events (aggregate_id, event_seq ASC);

CREATE TABLE outbox_events_y2026m09 PARTITION OF outbox_events
    FOR VALUES FROM ('2026-09-01 00:00:00+00') TO ('2026-10-01 00:00:00+00');
CREATE TABLE outbox_events_y2026m10 PARTITION OF outbox_events
    FOR VALUES FROM ('2026-10-01 00:00:00+00') TO ('2026-11-01 00:00:00+00');
CREATE TABLE outbox_events_y2026m11 PARTITION OF outbox_events
    FOR VALUES FROM ('2026-11-01 00:00:00+00') TO ('2026-12-01 00:00:00+00');
CREATE TABLE outbox_events_y2026m12 PARTITION OF outbox_events
    FOR VALUES FROM ('2026-12-01 00:00:00+00') TO ('2027-01-01 00:00:00+00');
CREATE TABLE outbox_events_default PARTITION OF outbox_events DEFAULT;

-- Payouts (Section 5.1)
CREATE TABLE payouts (
    id              UUID PRIMARY KEY,
    merchant_id     UUID        NOT NULL REFERENCES merchants(id),
    amount          BIGINT      NOT NULL CHECK (amount > 0),
    currency        VARCHAR(3)  NOT NULL DEFAULT 'INR',
    status          VARCHAR(32) NOT NULL CHECK (status IN ('CREATED', 'COMPLETED', 'FAILED')),
    ledger_txn_id   UUID REFERENCES ledger_transactions(id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_payouts_merchant_created ON payouts (merchant_id, created_at DESC);

-- =============================================================================
-- Seed System Ledger Accounts (PSP_RECEIVABLE, PLATFORM_FEE_REVENUE + shards, BANK)
-- Deterministic UUIDs so ascending account_id lock order is well-defined.
-- =============================================================================
INSERT INTO accounts (id, merchant_id, type, currency, name) VALUES
    ('00000000-0000-7000-8000-000000000001', NULL, 'ASSET',    'INR', 'PSP_RECEIVABLE'),
    ('00000000-0000-7000-8000-000000000002', NULL, 'REVENUE',  'INR', 'PLATFORM_FEE_REVENUE'),
    ('00000000-0000-7000-8000-000000000003', NULL, 'EXTERNAL', 'INR', 'BANK');

-- 16 sharded fee revenue sub-accounts for the Hot Account A/B benchmark (Section 5.3)
INSERT INTO accounts (id, merchant_id, type, currency, name)
SELECT
    ('00000000-0000-7000-8000-0000000001' || LPAD(TO_HEX(i), 2, '0'))::uuid,
    NULL,
    'REVENUE',
    'INR',
    'PLATFORM_FEE_REVENUE_' || LPAD(i::text, 2, '0')
FROM generate_series(0, 15) AS s(i);

INSERT INTO account_balances (account_id, balance, version, updated_at)
SELECT id, 0, 0, NOW() FROM accounts;
