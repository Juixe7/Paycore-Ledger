package com.paycore.payments.ledger;

import com.paycore.common.clock.ClockProvider;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Mathematical invariant checker for the double-entry ledger (Section 3, 5.1, 7).
 *
 * <p>Asserts:
 * <ol>
 *   <li>Global sum of debits == global sum of credits</li>
 *   <li>Every ledger_transaction has SUM(DEBIT) == SUM(CREDIT)</li>
 *   <li>No MERCHANT_PAYABLE account balance is negative</li>
 *   <li>Every account_balances row equals the exact sum of its ledger_entries (drift == 0)</li>
 *   <li>No payment is in an illegal state (refunded > captured or captured > amount)</li>
 * </ol>
 */
@Component
class LedgerInvariantChecker {

    private final JdbcClient jdbcClient;
    private final ClockProvider clockProvider;
    private final AtomicLong driftGaugePaise = new AtomicLong(0L);

    LedgerInvariantChecker(
            JdbcClient jdbcClient,
            ClockProvider clockProvider,
            @Autowired(required = false) MeterRegistry meterRegistry) {
        this.jdbcClient = jdbcClient;
        this.clockProvider = clockProvider;
        if (meterRegistry != null) {
            Gauge.builder("paycore_ledger_invariant_drift_paise", driftGaugePaise, AtomicLong::get)
                    .description("Absolute ledger invariant drift in paise (must always be 0)")
                    .register(meterRegistry);
        }
    }

    @Transactional(readOnly = true, isolation = Isolation.REPEATABLE_READ)
    InvariantReport checkInvariants() {
        record GlobalTotals(long debits, long credits) {}
        GlobalTotals totals = jdbcClient.sql("""
                SELECT
                    COALESCE(SUM(CASE WHEN direction = 'DEBIT'  THEN amount ELSE 0 END), 0) AS debits,
                    COALESCE(SUM(CASE WHEN direction = 'CREDIT' THEN amount ELSE 0 END), 0) AS credits
                FROM ledger_entries
                """)
                .query((rs, rowNum) -> new GlobalTotals(rs.getLong("debits"), rs.getLong("credits")))
                .single();

        long unbalancedTxnCount = jdbcClient.sql("""
                SELECT COUNT(*) FROM (
                    SELECT txn_id
                    FROM ledger_entries
                    GROUP BY txn_id
                    HAVING SUM(CASE WHEN direction = 'DEBIT' THEN amount ELSE 0 END)
                        <> SUM(CASE WHEN direction = 'CREDIT' THEN amount ELSE 0 END)
                ) u
                """)
                .query(Long.class)
                .single();

        long negativeMerchantPayableCount = jdbcClient.sql("""
                SELECT COUNT(*)
                FROM account_balances ab
                JOIN accounts a ON a.id = ab.account_id
                WHERE a.name = 'MERCHANT_PAYABLE'
                  AND ab.balance < 0
                """)
                .query(Long.class)
                .single();

        record DriftRow(long mismatchedAccounts, long totalAbsDrift) {}
        DriftRow drift = jdbcClient.sql("""
                WITH derived AS (
                    SELECT
                        a.id AS account_id,
                        COALESCE(SUM(
                            CASE
                                WHEN a.type = 'ASSET' AND le.direction = 'DEBIT'  THEN  le.amount
                                WHEN a.type = 'ASSET' AND le.direction = 'CREDIT' THEN -le.amount
                                WHEN a.type <> 'ASSET' AND le.direction = 'CREDIT' THEN  le.amount
                                WHEN a.type <> 'ASSET' AND le.direction = 'DEBIT'  THEN -le.amount
                                ELSE 0
                            END
                        ), 0) AS expected_balance
                    FROM accounts a
                    LEFT JOIN ledger_entries le ON le.account_id = a.id
                    GROUP BY a.id
                )
                SELECT
                    COUNT(*) FILTER (WHERE ab.balance <> d.expected_balance) AS mismatched_accounts,
                    COALESCE(SUM(ABS(ab.balance - d.expected_balance)), 0)   AS total_abs_drift
                FROM account_balances ab
                JOIN derived d ON d.account_id = ab.account_id
                """)
                .query((rs, rowNum) -> new DriftRow(
                        rs.getLong("mismatched_accounts"),
                        rs.getLong("total_abs_drift")))
                .single();

        long illegalPaymentCount = jdbcClient.sql("""
                WITH payment_ledger_refunds AS (
                    SELECT
                        p.id AS payment_id,
                        p.amount,
                        p.status,
                        p.captured_amount,
                        p.refunded_amount,
                        COALESCE(SUM(CASE WHEN lt.kind = 'REFUND' AND le.direction = 'DEBIT' THEN le.amount ELSE 0 END), 0) AS ledger_refunded,
                        COUNT(DISTINCT CASE WHEN lt.kind = 'CAPTURE' THEN lt.id END) AS capture_txn_count
                    FROM payments p
                    LEFT JOIN ledger_transactions lt ON lt.payment_id = p.id
                    LEFT JOIN ledger_entries le ON le.txn_id = lt.id
                    GROUP BY p.id, p.amount, p.status, p.captured_amount, p.refunded_amount
                )
                SELECT COUNT(*)
                FROM payment_ledger_refunds
                WHERE refunded_amount > captured_amount
                   OR ledger_refunded > captured_amount
                   OR refunded_amount <> ledger_refunded
                   OR captured_amount > amount
                   OR capture_txn_count > 1
                   OR (status IN ('CAPTURED', 'PARTIALLY_REFUNDED', 'REFUNDED') AND (captured_amount <= 0 OR capture_txn_count <> 1))
                   OR (status = 'VOIDED' AND captured_amount > 0)
                """)
                .query(Long.class)
                .single();

        long globalImbalance = Math.abs(totals.debits() - totals.credits());
        long totalDriftSignal = globalImbalance
                + unbalancedTxnCount
                + negativeMerchantPayableCount
                + drift.totalAbsDrift()
                + illegalPaymentCount;
        driftGaugePaise.set(totalDriftSignal);

        boolean healthy = totalDriftSignal == 0 && drift.mismatchedAccounts() == 0;

        return new InvariantReport(
                healthy ? "PASS" : "FAIL",
                healthy,
                totals.debits(),
                totals.credits(),
                globalImbalance,
                unbalancedTxnCount,
                negativeMerchantPayableCount,
                drift.mismatchedAccounts(),
                drift.totalAbsDrift(),
                illegalPaymentCount,
                clockProvider.now());
    }

    record InvariantReport(
            String status,
            boolean healthy,
            long globalDebits,
            long globalCredits,
            long globalImbalance,
            long unbalancedTransactions,
            long negativeMerchantAccounts,
            long mismatchedBalanceAccounts,
            long ledgerDrift,
            long illegalPayments,
            Instant checkedAt) {}
}
