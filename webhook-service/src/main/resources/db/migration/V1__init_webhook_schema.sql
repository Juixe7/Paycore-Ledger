-- =============================================================================
-- PayCore webhook-service Schema (Section 5.1 & 5.4)
-- =============================================================================

CREATE TABLE IF NOT EXISTS merchant_webhook_endpoints (
    merchant_id     UUID PRIMARY KEY,
    webhook_url     TEXT        NOT NULL,
    webhook_secret  TEXT        NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS webhook_deliveries (
    id               UUID PRIMARY KEY,
    event_id         UUID        NOT NULL,
    merchant_id      UUID        NOT NULL,
    attempt          INTEGER     NOT NULL CHECK (attempt > 0),
    status           VARCHAR(32) NOT NULL CHECK (status IN ('DELIVERED', 'RETRY_SCHEDULED', 'DEAD_LETTERED', 'FAILED')),
    retry_topic      VARCHAR(64),
    next_attempt_at  TIMESTAMPTZ,
    last_status_code INTEGER,
    last_error       TEXT,
    target_url       TEXT        NOT NULL,
    payload_json     TEXT        NOT NULL,
    traceparent      TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_merchant ON webhook_deliveries (merchant_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_event_id ON webhook_deliveries (event_id, attempt DESC);
CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_due ON webhook_deliveries (status, next_attempt_at ASC)
    WHERE status = 'RETRY_SCHEDULED';

CREATE TABLE IF NOT EXISTS processed_events (
    consumer        VARCHAR(64) NOT NULL,
    event_id        UUID        NOT NULL,
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (consumer, event_id)
);

CREATE TABLE IF NOT EXISTS dlq_events (
    id              UUID PRIMARY KEY,
    event_id        VARCHAR(128),
    payment_id      VARCHAR(128),
    merchant_id     VARCHAR(128),
    raw_payload     BYTEA       NOT NULL,
    reason          TEXT        NOT NULL,
    replayed        BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
