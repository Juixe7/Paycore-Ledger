-- =============================================================================
-- V3: Add PARTIALLY_REFUNDED and REFUNDED lifecycle statuses to payments table
-- =============================================================================

ALTER TABLE payments DROP CONSTRAINT IF EXISTS payments_status_check;

ALTER TABLE payments ADD CONSTRAINT payments_status_check CHECK (status IN (
    'CREATED',
    'AUTHORIZING',
    'AUTHORIZED',
    'CAPTURED',
    'PARTIALLY_REFUNDED',
    'REFUNDED',
    'VOIDED',
    'FAILED'
));
