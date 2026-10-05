-- ============================================================================
-- PayCore Settlement Reconciliation Schema (Section 5.1 & Section 7.10)
-- ============================================================================

CREATE TABLE IF NOT EXISTS recon_runs (
    id              UUID PRIMARY KEY,
    settlement_date DATE NOT NULL,
    matched         INT NOT NULL,
    discrepancies   INT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS recon_discrepancies (
    id           UUID PRIMARY KEY,
    run_id       UUID NOT NULL REFERENCES recon_runs(id),
    psp_ref      TEXT,
    payment_id   UUID,
    kind         TEXT NOT NULL CHECK (kind IN (
        'MISSING_IN_LEDGER',
        'MISSING_IN_PSP_FILE',
        'AMOUNT_MISMATCH',
        'STATUS_MISMATCH'
    )),
    expected_amt BIGINT,
    actual_amt   BIGINT,
    details      JSONB
);

CREATE INDEX IF NOT EXISTS idx_recon_discrepancies_run
    ON recon_discrepancies (run_id);
