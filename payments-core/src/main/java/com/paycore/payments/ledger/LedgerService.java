package com.paycore.payments.ledger;

import com.paycore.common.clock.ClockProvider;
import com.paycore.common.errors.PayCoreException;
import com.paycore.common.ids.UuidV7;
import com.paycore.common.money.CurrencyCode;
import com.paycore.common.money.Money;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Double-entry ledger posting engine (Section 5.1 & 5.3).
 *
 * <p>Architectural Rule (Section 5.7):
 * The {@code ledger} package exposes ONLY ONE public method to the rest of {@code payments-core}:
 * {@link #post(LedgerTransaction)}. Enforced by ArchUnit.
 */
@Service
public class LedgerService {

    static final UUID SYSTEM_PSP_RECEIVABLE_ID =
            UUID.fromString("00000000-0000-7000-8000-000000000001");
    static final UUID SYSTEM_PLATFORM_FEE_ID =
            UUID.fromString("00000000-0000-7000-8000-000000000002");
    static final UUID SYSTEM_BANK_ID =
            UUID.fromString("00000000-0000-7000-8000-000000000003");

    private static final int FEE_SHARD_COUNT = 16;
    private static final int MAX_SERIALIZATION_RETRIES = 3;

    private final JdbcClient jdbcClient;
    private final ClockProvider clockProvider;
    private volatile boolean shardedFeeAccounts;

    public LedgerService(
            JdbcClient jdbcClient,
            ClockProvider clockProvider,
            @Value("${paycore.ledger.sharded-fee-accounts:false}") boolean shardedFeeAccounts) {
        this.jdbcClient = jdbcClient;
        this.clockProvider = clockProvider;
        this.shardedFeeAccounts = shardedFeeAccounts;
    }

    /**
     * Posts a balanced double-entry transaction to the immutable ledger and updates account_balances
     * atomically inside the caller's database transaction.
     *
     * <p>Locks {@code account_balances} rows in ascending {@code account_id} order to prevent deadlocks,
     * and retries on SQLSTATE 40001 / 40P01 as a safety net.
     */
    @Transactional(propagation = Propagation.MANDATORY, isolation = Isolation.READ_COMMITTED)
    public UUID post(LedgerTransaction transaction) {
        int attempt = 0;
        while (true) {
            attempt++;
            try {
                return executePost(transaction);
            } catch (DataAccessException ex) {
                if (attempt < MAX_SERIALIZATION_RETRIES && isRetryableSerializationOrDeadlock(ex)) {
                    continue;
                }
                throw ex;
            }
        }
    }

    private UUID executePost(LedgerTransaction txn) {
        Instant now = clockProvider.now();
        Timestamp nowTs = Timestamp.from(now);
        UUID merchantPayableAccountId = ensureMerchantPayableAccount(txn.merchantId(), txn.grossAmount().currency(), nowTs);

        List<PostingLine> lines = buildPostingLines(txn, merchantPayableAccountId);

        // Verify double-entry mathematical invariant in application layer before hitting DB trigger
        long totalDebits = lines.stream()
                .filter(l -> l.direction() == EntryDirection.DEBIT)
                .mapToLong(PostingLine::amount)
                .sum();
        long totalCredits = lines.stream()
                .filter(l -> l.direction() == EntryDirection.CREDIT)
                .mapToLong(PostingLine::amount)
                .sum();
        if (totalDebits != totalCredits || totalDebits <= 0) {
            throw new IllegalStateException(
                    "Unbalanced ledger posting lines: debits=" + totalDebits + ", credits=" + totalCredits);
        }

        // Aggregate balance deltas per account and sort in strictly ascending account_id order (Section 5.3)
        Map<UUID, Long> deltaByAccount = new HashMap<>();
        for (PostingLine line : lines) {
            deltaByAccount.merge(line.accountId(), line.normalBalanceDelta(), Long::sum);
        }

        List<UUID> sortedAccountIds = deltaByAccount.keySet().stream()
                .sorted(Comparator.comparing(UUID::toString))
                .toList();

        // Lock account_balances in ascending account_id order to make deadlocks impossible
        for (UUID accountId : sortedAccountIds) {
            Long currentBalance = jdbcClient.sql("""
                    SELECT balance
                    FROM account_balances
                    WHERE account_id = :accountId
                    FOR UPDATE
                    """)
                    .param("accountId", accountId)
                    .query(Long.class)
                    .single();

            long delta = deltaByAccount.get(accountId);
            long newBalance = Math.addExact(currentBalance, delta);

            if (accountId.equals(merchantPayableAccountId) && newBalance < 0) {
                throw PayCoreException.unprocessable(
                        "INSUFFICIENT_MERCHANT_BALANCE",
                        "Insufficient Merchant Payable Balance",
                        "Operation would drive MERCHANT_PAYABLE negative (current=" + currentBalance
                                + ", delta=" + delta + ")");
            }
        }

        UUID txnId = UuidV7.generate(now.toEpochMilli());
        jdbcClient.sql("""
                INSERT INTO ledger_transactions (id, payment_id, kind, created_at, posted_by)
                VALUES (:id, :paymentId, :kind, :createdAt, :postedBy)
                """)
                .param("id", txnId)
                .param("paymentId", txn.paymentId())
                .param("kind", txn.kind().name())
                .param("createdAt", nowTs)
                .param("postedBy", txn.postedBy())
                .update();

        for (PostingLine line : lines) {
            UUID entryId = UuidV7.generate(now.toEpochMilli());
            jdbcClient.sql("""
                    INSERT INTO ledger_entries (id, txn_id, account_id, direction, amount, created_at)
                    VALUES (:id, :txnId, :accountId, :direction, :amount, :createdAt)
                    """)
                    .param("id", entryId)
                    .param("txnId", txnId)
                    .param("accountId", line.accountId())
                    .param("direction", line.direction().name())
                    .param("amount", line.amount())
                    .param("createdAt", nowTs)
                    .update();
        }

        for (UUID accountId : sortedAccountIds) {
            long delta = deltaByAccount.get(accountId);
            jdbcClient.sql("""
                    UPDATE account_balances
                    SET balance = balance + :delta,
                        version = version + 1,
                        updated_at = :updatedAt
                    WHERE account_id = :accountId
                    """)
                    .param("delta", delta)
                    .param("updatedAt", nowTs)
                    .param("accountId", accountId)
                    .update();
        }

        return txnId;
    }

    UUID ensureMerchantPayableAccount(UUID merchantId, CurrencyCode currency, Timestamp nowTs) {
        UUID candidateId = UuidV7.generate();
        jdbcClient.sql("""
                INSERT INTO accounts (id, merchant_id, type, currency, name, created_at)
                VALUES (:id, :merchantId, 'LIABILITY', :currency, 'MERCHANT_PAYABLE', :createdAt)
                ON CONFLICT (merchant_id, name, currency) DO NOTHING
                """)
                .param("id", candidateId)
                .param("merchantId", merchantId)
                .param("currency", currency.name())
                .param("createdAt", nowTs)
                .update();

        UUID accountId = jdbcClient.sql("""
                SELECT id FROM accounts
                WHERE merchant_id = :merchantId AND name = 'MERCHANT_PAYABLE' AND currency = :currency
                """)
                .param("merchantId", merchantId)
                .param("currency", currency.name())
                .query(UUID.class)
                .single();

        jdbcClient.sql("""
                INSERT INTO account_balances (account_id, balance, version, updated_at)
                VALUES (:accountId, 0, 0, :updatedAt)
                ON CONFLICT (account_id) DO NOTHING
                """)
                .param("accountId", accountId)
                .param("updatedAt", nowTs)
                .update();

        return accountId;
    }

    private List<PostingLine> buildPostingLines(LedgerTransaction txn, UUID merchantPayableId) {
        List<PostingLine> lines = new ArrayList<>(3);
        long gross = txn.grossAmount().amountMinor();
        long fee = txn.feeAmount().amountMinor();
        long net = txn.netMerchantAmount().amountMinor();

        switch (txn.kind()) {
            case CAPTURE -> {
                UUID pspAccountId = selectPspReceivableAccountId(txn.paymentId());
                // Debit PSP_RECEIVABLE (ASSET: DEBIT increases normal balance by +gross)
                lines.add(new PostingLine(pspAccountId, EntryDirection.DEBIT, gross, +gross));
                // Credit MERCHANT_PAYABLE (LIABILITY: CREDIT increases normal balance by +net)
                lines.add(new PostingLine(merchantPayableId, EntryDirection.CREDIT, net, +net));
                if (fee > 0) {
                    UUID feeAccountId = selectFeeAccountId(txn.paymentId());
                    // Credit PLATFORM_FEE_REVENUE (REVENUE: CREDIT increases normal balance by +fee)
                    lines.add(new PostingLine(feeAccountId, EntryDirection.CREDIT, fee, +fee));
                }
            }
            case REFUND -> {
                UUID pspAccountId = selectPspReceivableAccountId(txn.paymentId());
                // Refund policy (Page 9): Debit MERCHANT_PAYABLE, Credit PSP_RECEIVABLE (fee not returned)
                lines.add(new PostingLine(merchantPayableId, EntryDirection.DEBIT, gross, -gross));
                lines.add(new PostingLine(pspAccountId, EntryDirection.CREDIT, gross, -gross));
            }
            case PAYOUT -> {
                // Payout (Page 9): Debit MERCHANT_PAYABLE, Credit BANK
                lines.add(new PostingLine(merchantPayableId, EntryDirection.DEBIT, gross, -gross));
                lines.add(new PostingLine(SYSTEM_BANK_ID, EntryDirection.CREDIT, gross, +gross));
            }
            case REVERSAL -> {
                UUID pspAccountId = selectPspReceivableAccountId(txn.paymentId());
                lines.add(new PostingLine(merchantPayableId, EntryDirection.DEBIT, gross, -gross));
                lines.add(new PostingLine(pspAccountId, EntryDirection.CREDIT, gross, -gross));
            }
        }
        return lines;
    }

    private UUID selectFeeAccountId(UUID paymentId) {
        if (!shardedFeeAccounts) {
            return SYSTEM_PLATFORM_FEE_ID;
        }
        int shard = paymentId != null
                ? Math.floorMod(paymentId.hashCode(), FEE_SHARD_COUNT)
                : ThreadLocalRandom.current().nextInt(FEE_SHARD_COUNT);
        return UUID.fromString(String.format("00000000-0000-7000-8000-0000000001%02x", shard));
    }

    private UUID selectPspReceivableAccountId(UUID paymentId) {
        if (!shardedFeeAccounts) {
            return SYSTEM_PSP_RECEIVABLE_ID;
        }
        int shard = paymentId != null
                ? Math.floorMod(paymentId.hashCode(), FEE_SHARD_COUNT)
                : ThreadLocalRandom.current().nextInt(FEE_SHARD_COUNT);
        return UUID.fromString(String.format("00000000-0000-7000-8000-0000000002%02x", shard));
    }

    void setShardedFeeAccounts(boolean shardedFeeAccounts) {
        this.shardedFeeAccounts = shardedFeeAccounts;
    }

    boolean isShardedFeeAccounts() {
        return shardedFeeAccounts;
    }

    SystemBalancesView getSystemBalancesSummary() {
        long totalFeeRevenue = jdbcClient.sql("""
                SELECT COALESCE(SUM(ab.balance), 0)
                FROM account_balances ab
                JOIN accounts a ON a.id = ab.account_id
                WHERE a.name LIKE 'PLATFORM_FEE_REVENUE%'
                """)
                .query(Long.class)
                .single();
        long totalPspReceivable = jdbcClient.sql("""
                SELECT COALESCE(SUM(ab.balance), 0)
                FROM account_balances ab
                JOIN accounts a ON a.id = ab.account_id
                WHERE a.name LIKE 'PSP_RECEIVABLE%'
                """)
                .query(Long.class)
                .single();
        long activeFeeShards = jdbcClient.sql("""
                SELECT COUNT(*)
                FROM account_balances ab
                JOIN accounts a ON a.id = ab.account_id
                WHERE a.name LIKE 'PLATFORM_FEE_REVENUE_%' AND ab.balance > 0
                """)
                .query(Long.class)
                .single();
        return new SystemBalancesView(shardedFeeAccounts, totalFeeRevenue, totalPspReceivable, activeFeeShards);
    }

    record SystemBalancesView(
            boolean shardedModeEnabled,
            long totalPlatformFeeRevenuePaise,
            long totalPspReceivablePaise,
            long activeFeeShardsPopulated) {}

    private boolean isRetryableSerializationOrDeadlock(DataAccessException ex) {
        if (ex instanceof CannotAcquireLockException) {
            return true;
        }
        Throwable cause = ex.getMostSpecificCause();
        if (cause instanceof SQLException sqlEx) {
            String sqlState = sqlEx.getSQLState();
            return "40001".equals(sqlState) || "40P01".equals(sqlState);
        }
        return false;
    }

    // Package-private query methods used strictly by LedgerController within com.paycore.payments.ledger

    List<LedgerEntryView> findEntriesForPayment(UUID paymentId) {
        return jdbcClient.sql("""
                SELECT
                    le.id AS entry_id,
                    le.txn_id,
                    lt.kind AS txn_kind,
                    le.account_id,
                    a.name AS account_name,
                    a.type AS account_type,
                    le.direction,
                    le.amount,
                    le.created_at
                FROM ledger_entries le
                JOIN ledger_transactions lt ON lt.id = le.txn_id
                JOIN accounts a ON a.id = le.account_id
                WHERE lt.payment_id = :paymentId
                ORDER BY le.created_at ASC, le.id ASC
                """)
                .param("paymentId", paymentId)
                .query((rs, rowNum) -> new LedgerEntryView(
                        rs.getObject("entry_id", UUID.class),
                        rs.getObject("txn_id", UUID.class),
                        rs.getString("txn_kind"),
                        rs.getObject("account_id", UUID.class),
                        rs.getString("account_name"),
                        rs.getString("account_type"),
                        rs.getString("direction"),
                        rs.getLong("amount"),
                        rs.getTimestamp("created_at").toInstant()))
                .list();
    }

    MerchantBalanceView getMerchantBalance(UUID merchantId) {
        return jdbcClient.sql("""
                SELECT
                    a.id AS account_id,
                    a.currency,
                    ab.balance,
                    ab.version,
                    ab.updated_at
                FROM accounts a
                JOIN account_balances ab ON ab.account_id = a.id
                WHERE a.merchant_id = :merchantId
                  AND a.name = 'MERCHANT_PAYABLE'
                """)
                .param("merchantId", merchantId)
                .query((rs, rowNum) -> new MerchantBalanceView(
                        merchantId,
                        rs.getObject("account_id", UUID.class),
                        rs.getString("currency"),
                        rs.getLong("balance"),
                        rs.getLong("version"),
                        rs.getTimestamp("updated_at").toInstant()))
                .optional()
                .orElseGet(() -> new MerchantBalanceView(merchantId, null, "INR", 0L, 0L, clockProvider.now()));
    }

    private record PostingLine(UUID accountId, EntryDirection direction, long amount, long normalBalanceDelta) {}

    record LedgerEntryView(
            UUID entryId,
            UUID txnId,
            String txnKind,
            UUID accountId,
            String accountName,
            String accountType,
            String direction,
            long amount,
            Instant createdAt) {}

    record MerchantBalanceView(
            UUID merchantId,
            UUID accountId,
            String currency,
            long balance,
            long version,
            Instant updatedAt) {}
}
