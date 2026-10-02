-- =============================================================================
-- V2: Ensure webhook_secret is never persisted in per-delivery audit rows
-- =============================================================================

ALTER TABLE webhook_deliveries DROP COLUMN IF EXISTS webhook_secret;
