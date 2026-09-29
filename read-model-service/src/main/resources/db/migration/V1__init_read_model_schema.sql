-- =============================================================================
-- PayCore read-model-service CQRS Schema (Section 5.1 & FR11)
-- =============================================================================

CREATE TABLE IF NOT EXISTS payment_view (
    payment_id      UUID PRIMARY KEY,
    merchant_id     UUID        NOT NULL,
    status          VARCHAR(32) NOT NULL,
    amount          BIGINT      NOT NULL DEFAULT 0,
    captured_amount BIGINT      NOT NULL DEFAULT 0,
    refunded_amount BIGINT      NOT NULL DEFAULT 0,
    fee_amount      BIGINT      NOT NULL DEFAULT 0,
    currency        VARCHAR(3)  NOT NULL DEFAULT 'INR',
    psp_ref         VARCHAR(128),
    last_event_seq  BIGINT      NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_payment_view_merchant_cursor
    ON payment_view (merchant_id, payment_id DESC);

CREATE TABLE IF NOT EXISTS merchant_daily_summary (
    merchant_id     UUID        NOT NULL,
    day             DATE        NOT NULL,
    captured        BIGINT      NOT NULL DEFAULT 0,
    refunded        BIGINT      NOT NULL DEFAULT 0,
    fees            BIGINT      NOT NULL DEFAULT 0,
    payment_count   BIGINT      NOT NULL DEFAULT 0,
    PRIMARY KEY (merchant_id, day)
);

CREATE TABLE IF NOT EXISTS processed_events (
    consumer        VARCHAR(64) NOT NULL,
    event_id        UUID        NOT NULL,
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (consumer, event_id)
);

CREATE TABLE IF NOT EXISTS read_model_event_log (
    payment_id      UUID        NOT NULL,
    event_seq       BIGINT      NOT NULL,
    event_id        UUID        NOT NULL,
    raw_payload     BYTEA       NOT NULL,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (payment_id, event_seq)
);
