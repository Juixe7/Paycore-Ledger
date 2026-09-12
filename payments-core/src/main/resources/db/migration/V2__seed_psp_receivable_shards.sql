-- =============================================================================
-- V2: Seed 16 Sharded PSP_RECEIVABLE System Accounts for Hot Account Sharding
-- =============================================================================
INSERT INTO accounts (id, merchant_id, type, currency, name)
SELECT
    ('00000000-0000-7000-8000-0000000002' || LPAD(TO_HEX(i), 2, '0'))::uuid,
    NULL,
    'ASSET',
    'INR',
    'PSP_RECEIVABLE_' || LPAD(i::text, 2, '0')
FROM generate_series(0, 15) AS s(i)
ON CONFLICT (id) DO NOTHING;

INSERT INTO account_balances (account_id, balance, version, updated_at)
SELECT id, 0, 0, NOW()
FROM accounts
WHERE name LIKE 'PSP_RECEIVABLE_%'
ON CONFLICT (account_id) DO NOTHING;
