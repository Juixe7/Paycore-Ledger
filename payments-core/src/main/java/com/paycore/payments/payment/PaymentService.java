package com.paycore.payments.payment;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.paycore.common.clock.ClockProvider;
import com.paycore.common.errors.PayCoreException;
import com.paycore.common.ids.UuidV7;
import com.paycore.common.money.CurrencyCode;
import com.paycore.common.money.Money;
import com.paycore.payments.ledger.LedgerService;
import com.paycore.payments.ledger.LedgerTransaction;
import com.paycore.payments.outbox.OutboxWriter;
import com.paycore.payments.psp.PspClient;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Core Payment Lifecycle Service (Section 4.3, 5.1, 5.3).
 *
 * <p>Implements:
 * <ul>
 *   <li>Create payment intent</li>
 *   <li>Two-transaction Authorize across external PSP call (never holding a DB connection during network I/O)</li>
 *   <li>Unknown-outcome (PSP timeout) background resolver</li>
 *   <li>Atomic Capture (state transition + double-entry ledger post + outbox event in one commit)</li>
 *   <li>Void and Full/Partial Refunds serialized via {@code SELECT ... FOR UPDATE}</li>
 *   <li>Settlement Payouts</li>
 * </ul>
 */
@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);
    private static final TypeReference<Map<String, String>> MAP_TYPE = new TypeReference<>() {};

    private final JdbcClient jdbcClient;
    private final LedgerService ledgerService;
    private final OutboxWriter outboxWriter;
    private final PspClient pspClient;
    private final ObjectMapper objectMapper;
    private final ClockProvider clockProvider;
    private final TransactionTemplate requiresNewTx;

    private volatile boolean naiveUnlockedRefundMode;
    private volatile Consumer<String> crashHook = stage -> {};

    public PaymentService(
            JdbcClient jdbcClient,
            LedgerService ledgerService,
            OutboxWriter outboxWriter,
            PspClient pspClient,
            ObjectMapper objectMapper,
            ClockProvider clockProvider,
            PlatformTransactionManager txManager,
            @Value("${paycore.payment.naive-unlocked-refund:false}") boolean naiveUnlockedRefundMode) {
        this.jdbcClient = jdbcClient;
        this.ledgerService = ledgerService;
        this.outboxWriter = outboxWriter;
        this.pspClient = pspClient;
        this.objectMapper = objectMapper;
        this.clockProvider = clockProvider;
        this.requiresNewTx = new TransactionTemplate(txManager);
        this.requiresNewTx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        this.naiveUnlockedRefundMode = naiveUnlockedRefundMode;
    }

    /**
     * Initializes a new payment intent with {@code CREATED} state in minor currency units (paise).
     *
     * @param merchantId authenticated merchant identifier owning this payment
     * @param request creation parameters including positive amount in minor units, currency, and optional metadata
     * @return persisted {@link Payment} aggregate record in {@code CREATED} state
     * @throws PayCoreException if amount is non-positive (422 INVALID_AMOUNT) or currency is unsupported
     */
    @Transactional(isolation = Isolation.READ_COMMITTED)
    public Payment createPayment(UUID merchantId, CreatePaymentRequest request) {
        if (request.amount() <= 0) {
            throw PayCoreException.unprocessable("INVALID_AMOUNT", "Invalid Payment Amount", "Amount must be > 0 in minor units (paise)");
        }
        CurrencyCode currency = CurrencyCode.fromString(request.currency() != null ? request.currency() : "INR");
        Instant now = clockProvider.now();
        Timestamp nowTs = Timestamp.from(now);
        UUID paymentId = UuidV7.generate(now.toEpochMilli());
        Map<String, String> metadata = request.metadata() != null ? request.metadata() : Collections.emptyMap();
        String metadataJson = toJson(metadata);

        jdbcClient.sql("""
                INSERT INTO payments (
                    id, merchant_id, amount, currency, status,
                    captured_amount, refunded_amount, psp_ref, metadata,
                    version, event_seq, created_at, updated_at
                ) VALUES (
                    :id, :merchantId, :amount, :currency, 'CREATED',
                    0, 0, NULL, CAST(:metadata AS jsonb),
                    1, 0, :createdAt, :updatedAt
                )
                """)
                .param("id", paymentId)
                .param("merchantId", merchantId)
                .param("amount", request.amount())
                .param("currency", currency.name())
                .param("metadata", metadataJson)
                .param("createdAt", nowTs)
                .param("updatedAt", nowTs)
                .update();

        return getPayment(merchantId, paymentId);
    }

    /**
     * Orchestrates a Two-Phase idempotent authorization against an external PSP.
     *
     * <p>Execution phases:
     * <ol>
     *   <li><b>Tx 1 (Local):</b> Compare-and-set {@code CREATED -> AUTHORIZING} and commit immediately,
     *       releasing the JDBC connection back to HikariCP before network I/O.</li>
     *   <li><b>External I/O (Virtual Thread):</b> Calls the external PSP with deterministic reference
     *       {@code psp_ref = payment_id + ":" + attempt_no}. Resilience4j CircuitBreaker and Bulkhead enforce fault isolation.</li>
     *   <li><b>Tx 2 (Local):</b> On {@code AUTHORIZED}, updates state and inserts outbox {@code payment.authorized}.
     *       On {@code DECLINED}, marks {@code FAILED} and emits {@code payment.failed}.
     *       On timeout or connection drop, payment remains {@code AUTHORIZING} for background resolution.</li>
     * </ol>
     *
     * @param merchantId authenticated merchant identifier owning the payment
     * @param paymentId UUIDv7 identifier of the payment to authorize
     * @return updated {@link Payment} aggregate
     * @throws PayCoreException if payment is in an illegal transition state (409 INVALID_STATE_TRANSITION)
     */
    public Payment authorizePayment(UUID merchantId, UUID paymentId) {
        // Step 1: Tx 1 - Claim payment in CREATED -> AUTHORIZING and commit before network I/O
        AuthClaim claim = requiresNewTx.execute(status -> {
            Payment current = getPayment(merchantId, paymentId);
            if (current.status() == PaymentStatus.AUTHORIZED) {
                return new AuthClaim(current, 0, current.pspRef(), true);
            }
            if (current.status() == PaymentStatus.AUTHORIZING) {
                // If already AUTHORIZING (e.g. expired idempotency takeover), reuse the deterministic pspRef
                int existingAttempt = countAttempts(paymentId) + 1;
                String ref = current.pspRef() != null ? current.pspRef() : (paymentId + ":" + existingAttempt);
                return new AuthClaim(current, existingAttempt, ref, false);
            }
            current.status().requireTransitionTo(paymentId, PaymentStatus.AUTHORIZING, "authorize");

            int attemptNo = countAttempts(paymentId) + 1;
            String pspRef = paymentId + ":" + attemptNo;
            Instant now = clockProvider.now();

            int updated = jdbcClient.sql("""
                    UPDATE payments
                    SET status = 'AUTHORIZING',
                        psp_ref = :pspRef,
                        version = version + 1,
                        updated_at = :updatedAt
                    WHERE id = :id
                      AND merchant_id = :merchantId
                      AND status = 'CREATED'
                    """)
                    .param("pspRef", pspRef)
                    .param("updatedAt", Timestamp.from(now))
                    .param("id", paymentId)
                    .param("merchantId", merchantId)
                    .update();

            if (updated == 0) {
                Payment reloaded = getPayment(merchantId, paymentId);
                if (reloaded.status() == PaymentStatus.AUTHORIZED) {
                    return new AuthClaim(reloaded, attemptNo, reloaded.pspRef(), true);
                }
                throw PayCoreException.invalidStateTransition(
                        paymentId.toString(), reloaded.status().name(), "authorize");
            }

            return new AuthClaim(getPayment(merchantId, paymentId), attemptNo, pspRef, false);
        });

        if (claim.alreadyAuthorized()) {
            return claim.payment();
        }

        crashHook.accept("AFTER_TX1_BEFORE_PSP");

        // Step 2: Network call to PSP outside any DB transaction!
        PspClient.PspAuthResult pspResult = pspClient.authorize(new PspClient.PspAuthRequest(
                paymentId,
                merchantId,
                claim.attemptNo(),
                claim.pspRef(),
                claim.payment().amount(),
                claim.payment().currency()));

        crashHook.accept("AFTER_PSP_BEFORE_TX2");

        // Step 3: Tx 2 - Finalize or leave AUTHORIZING on unknown timeout
        return requiresNewTx.execute(status -> {
            Instant now = clockProvider.now();
            Timestamp nowTs = Timestamp.from(now);

            String attemptOutcome = switch (pspResult.outcome()) {
                case AUTHORIZED -> "AUTHORIZED";
                case DECLINED -> "DECLINED";
                case UNKNOWN_TIMEOUT -> "TIMEOUT";
            };

            insertAttemptRow(paymentId, claim.attemptNo(), claim.pspRef(), attemptOutcome, pspResult.latencyMs(), nowTs);

            if (pspResult.outcome() == PspClient.PspOutcome.UNKNOWN_TIMEOUT) {
                // Unknown outcome (Section 4.3): payment stays AUTHORIZING. Resolver job queries PSP by psp_ref.
                return getPayment(merchantId, paymentId);
            }

            if (pspResult.outcome() == PspClient.PspOutcome.AUTHORIZED) {
                int updated = jdbcClient.sql("""
                        UPDATE payments
                        SET status = 'AUTHORIZED',
                            version = version + 1,
                            event_seq = event_seq + 1,
                            updated_at = :updatedAt
                        WHERE id = :id
                          AND merchant_id = :merchantId
                          AND status = 'AUTHORIZING'
                        """)
                        .param("updatedAt", nowTs)
                        .param("id", paymentId)
                        .param("merchantId", merchantId)
                        .update();

                Payment after = getPayment(merchantId, paymentId);
                if (updated == 1) {
                    outboxWriter.writePaymentAuthorized(
                            paymentId,
                            merchantId,
                            after.eventSeq(),
                            after.amount(),
                            after.currency(),
                            after.pspRef());
                }
                return after;
            } else {
                int updated = jdbcClient.sql("""
                        UPDATE payments
                        SET status = 'FAILED',
                            version = version + 1,
                            event_seq = event_seq + 1,
                            updated_at = :updatedAt
                        WHERE id = :id
                          AND merchant_id = :merchantId
                          AND status = 'AUTHORIZING'
                        """)
                        .param("updatedAt", nowTs)
                        .param("id", paymentId)
                        .param("merchantId", merchantId)
                        .update();

                Payment after = getPayment(merchantId, paymentId);
                if (updated == 1) {
                    outboxWriter.writePaymentFailed(
                            paymentId,
                            merchantId,
                            after.eventSeq(),
                            after.amount(),
                            after.currency(),
                            after.pspRef(),
                            pspResult.detail());
                }
                return after;
            }
        });
    }

    /**
     * Executes atomic settlement capture (Tx 3), posting balanced 3-leg entries to the double-entry ledger.
     *
     * <p>Acquires an exclusive row-level lock ({@code SELECT ... FOR UPDATE}) on the payment,
     * verifies {@code AUTHORIZED} status, executes PSP capture I/O, invokes {@link com.paycore.payments.ledger.LedgerService#post},
     * advances state to {@code CAPTURED}, and writes the outbox event in the same ACID commit.
     *
     * @param merchantId authenticated merchant identifier
     * @param paymentId UUIDv7 payment identifier to capture
     * @param merchantFeeBps platform commission in basis points (e.g. 200 = 2.00%)
     * @return updated {@link Payment} aggregate in {@code CAPTURED} state
     * @throws PayCoreException if payment is not in {@code AUTHORIZED} state (409 INVALID_STATE_TRANSITION)
     */
    @Transactional(isolation = Isolation.READ_COMMITTED)
    public Payment capturePayment(UUID merchantId, UUID paymentId, int merchantFeeBps) {
        Payment locked = lockPaymentRow(merchantId, paymentId);
        locked.status().requireTransitionTo(paymentId, PaymentStatus.CAPTURED, "capture");

        Money grossAmount = Money.ofPositive(locked.amount(), CurrencyCode.fromString(locked.currency()));
        Money feeAmount = grossAmount.calculateFeeBps(merchantFeeBps);

        pspClient.capture(new PspClient.PspCaptureRequest(
                paymentId, merchantId, locked.pspRef(), locked.amount(), locked.currency()));

        UUID ledgerTxnId = ledgerService.post(
                LedgerTransaction.forCapture(paymentId, merchantId, grossAmount, feeAmount, "payments-core:capture"));

        crashHook.accept("MID_TX3_AFTER_LEDGER");

        Instant now = clockProvider.now();
        long nextEventSeq = locked.eventSeq() + 1;

        jdbcClient.sql("""
                UPDATE payments
                SET status = 'CAPTURED',
                    captured_amount = amount,
                    version = version + 1,
                    event_seq = :eventSeq,
                    updated_at = :updatedAt
                WHERE id = :id
                  AND merchant_id = :merchantId
                """)
                .param("eventSeq", nextEventSeq)
                .param("updatedAt", Timestamp.from(now))
                .param("id", paymentId)
                .param("merchantId", merchantId)
                .update();

        outboxWriter.writePaymentCaptured(
                paymentId,
                merchantId,
                nextEventSeq,
                grossAmount.amountMinor(),
                feeAmount.amountMinor(),
                grossAmount.subtract(feeAmount).amountMinor(),
                locked.currency(),
                locked.pspRef(),
                ledgerTxnId);

        return getPayment(merchantId, paymentId);
    }

    /**
     * Voids an authorized payment hold before capture occurs.
     *
     * @param merchantId authenticated merchant identifier
     * @param paymentId UUIDv7 payment identifier to void
     * @return updated {@link Payment} aggregate in {@code VOIDED} state
     * @throws PayCoreException if payment is not in {@code AUTHORIZED} state (409 INVALID_STATE_TRANSITION)
     */
    @Transactional(isolation = Isolation.READ_COMMITTED)
    public Payment voidPayment(UUID merchantId, UUID paymentId) {
        Payment locked = lockPaymentRow(merchantId, paymentId);
        locked.status().requireTransitionTo(paymentId, PaymentStatus.VOIDED, "void");

        Instant now = clockProvider.now();
        jdbcClient.sql("""
                UPDATE payments
                SET status = 'VOIDED',
                    version = version + 1,
                    updated_at = :updatedAt
                WHERE id = :id
                  AND merchant_id = :merchantId
                """)
                .param("updatedAt", Timestamp.from(now))
                .param("id", paymentId)
                .param("merchantId", merchantId)
                .update();

        return getPayment(merchantId, paymentId);
    }

    /**
     * Executes atomic partial or full refund protected by pessimistic row locking ({@code SELECT ... FOR UPDATE}).
     *
     * <p>Enforces invariant: {@code refunded_amount + request.amount() <= captured_amount}.
     * Transitions state to {@code PARTIALLY_REFUNDED} or {@code REFUNDED} using a database CASE statement,
     * posts reversing ledger entries ({@code DEBIT MERCHANT_PAYABLE}, {@code CREDIT PSP_RECEIVABLE}),
     * and emits {@code refund.processed} via transactional outbox.
     *
     * @param merchantId authenticated merchant identifier
     * @param paymentId UUIDv7 payment identifier to refund
     * @param request refund request containing positive minor-unit amount and optional reason
     * @return {@link RefundResponse} containing cumulative refund state and remaining refundable headroom
     * @throws PayCoreException if amount is invalid, status is not refundable, or request exceeds captured total (422 OVER_REFUND_REJECTED)
     */
    @Transactional(isolation = Isolation.READ_COMMITTED)
    public RefundResponse refundPayment(UUID merchantId, UUID paymentId, RefundRequest request) {
        if (request.amount() <= 0) {
            throw PayCoreException.unprocessable("INVALID_REFUND_AMOUNT", "Invalid Refund Amount", "Refund amount must be > 0");
        }

        Payment payment = naiveUnlockedRefundMode
                ? getPayment(merchantId, paymentId)
                : lockPaymentRow(merchantId, paymentId);

        if (payment.status() != PaymentStatus.CAPTURED
                && payment.status() != PaymentStatus.PARTIALLY_REFUNDED
                && payment.status() != PaymentStatus.REFUNDED) {
            throw PayCoreException.invalidStateTransition(paymentId.toString(), payment.status().name(), "refund");
        }

        long newRefundedTotal = Math.addExact(payment.refundedAmount(), request.amount());
        if (newRefundedTotal > payment.capturedAmount()) {
            throw PayCoreException.unprocessable(
                    "REFUND_EXCEEDS_CAPTURED_AMOUNT",
                    "Refund Exceeds Captured Amount",
                    "Requested refund " + request.amount() + " plus existing refunded "
                            + payment.refundedAmount() + " exceeds captured amount " + payment.capturedAmount());
        }

        PaymentStatus nextStatus = (newRefundedTotal >= payment.capturedAmount())
                ? PaymentStatus.REFUNDED
                : PaymentStatus.PARTIALLY_REFUNDED;
        payment.status().requireTransitionTo(paymentId, nextStatus, "refund");

        if (naiveUnlockedRefundMode) {
            // Deliberately widen race window in naive mode so 100 concurrent threads interleave check-then-update
            try {
                Thread.sleep(5);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }

        pspClient.refund(new PspClient.PspRefundRequest(
                paymentId, merchantId, payment.pspRef(), request.amount(), payment.currency()));

        Money refundMoney = Money.ofPositive(request.amount(), CurrencyCode.fromString(payment.currency()));
        UUID ledgerTxnId = ledgerService.post(
                LedgerTransaction.forRefund(paymentId, merchantId, refundMoney, "payments-core:refund"));

        Instant now = clockProvider.now();
        UUID refundId = UuidV7.generate(now.toEpochMilli());

        if (naiveUnlockedRefundMode) {
            // Naive check-then-set (classic lost-update bug when ORM/application writes computed value without a lock)
            jdbcClient.sql("""
                    UPDATE payments
                    SET status = :nextStatus,
                        refunded_amount = :newRefundedTotal,
                        version = version + 1,
                        event_seq = event_seq + 1,
                        updated_at = :updatedAt
                    WHERE id = :id
                      AND merchant_id = :merchantId
                    """)
                    .param("nextStatus", nextStatus.name())
                    .param("newRefundedTotal", newRefundedTotal)
                    .param("updatedAt", Timestamp.from(now))
                    .param("id", paymentId)
                    .param("merchantId", merchantId)
                    .update();
        } else {
            jdbcClient.sql("""
                    UPDATE payments
                    SET status = CASE WHEN refunded_amount + :refundAmount >= captured_amount THEN 'REFUNDED' ELSE 'PARTIALLY_REFUNDED' END,
                        refunded_amount = refunded_amount + :refundAmount,
                        version = version + 1,
                        event_seq = event_seq + 1,
                        updated_at = :updatedAt
                    WHERE id = :id
                      AND merchant_id = :merchantId
                    """)
                    .param("refundAmount", request.amount())
                    .param("updatedAt", Timestamp.from(now))
                    .param("id", paymentId)
                    .param("merchantId", merchantId)
                    .update();
        }

        Payment updated = getPayment(merchantId, paymentId);
        outboxWriter.writeRefundProcessed(
                paymentId,
                merchantId,
                updated.eventSeq(),
                refundId,
                request.amount(),
                updated.refundedAmount(),
                updated.currency(),
                ledgerTxnId);

        return new RefundResponse(
                refundId,
                paymentId,
                merchantId,
                request.amount(),
                updated.refundedAmount(),
                updated.capturedAmount(),
                updated.currency(),
                ledgerTxnId,
                now);
    }

    /**
     * Executes merchant withdrawal (payout), posting double-entry entries from {@code MERCHANT_PAYABLE} to {@code BANK}.
     *
     * <p>Decrements the merchant's liability balance, creates a completed {@link PayoutResponse},
     * and inserts {@code payout.created} outbox event in the same atomic transaction.
     *
     * @param merchantId authenticated merchant identifier withdrawing funds
     * @param request payout parameters containing positive minor-unit amount and currency
     * @return {@link PayoutResponse} confirming settlement reference and linked ledger transaction ID
     * @throws PayCoreException if amount is non-positive (422 INVALID_PAYOUT_AMOUNT) or balance is insufficient
     */
    @Transactional(isolation = Isolation.READ_COMMITTED)
    public PayoutResponse createPayout(UUID merchantId, PayoutRequest request) {
        if (request.amount() <= 0) {
            throw PayCoreException.unprocessable("INVALID_PAYOUT_AMOUNT", "Invalid Payout Amount", "Payout amount must be > 0");
        }
        String currencyStr = request.currency() != null ? request.currency() : "INR";
        CurrencyCode currency = CurrencyCode.fromString(currencyStr);
        Money payoutMoney = Money.ofPositive(request.amount(), currency);

        UUID ledgerTxnId = ledgerService.post(
                LedgerTransaction.forPayout(merchantId, payoutMoney, "payments-core:payout"));

        Instant now = clockProvider.now();
        UUID payoutId = UuidV7.generate(now.toEpochMilli());

        jdbcClient.sql("""
                INSERT INTO payouts (id, merchant_id, amount, currency, status, ledger_txn_id, created_at)
                VALUES (:id, :merchantId, :amount, :currency, 'COMPLETED', :ledgerTxnId, :createdAt)
                """)
                .param("id", payoutId)
                .param("merchantId", merchantId)
                .param("amount", request.amount())
                .param("currency", currency.name())
                .param("ledgerTxnId", ledgerTxnId)
                .param("createdAt", Timestamp.from(now))
                .update();

        outboxWriter.writePayoutCreated(payoutId, merchantId, 1L, request.amount(), currency.name(), ledgerTxnId);

        return new PayoutResponse(payoutId, merchantId, request.amount(), currency.name(), "COMPLETED", ledgerTxnId, now);
    }

    /**
     * Periodic background reconciliation for in-flight authorizations stuck due to network drops or timeouts.
     *
     * <p>Queries the external PSP status by {@code psp_ref} for payments remaining in {@code AUTHORIZING}
     * beyond the threshold window, converging state deterministically to {@code AUTHORIZED} or {@code FAILED}.
     *
     * @return count of payments successfully resolved in this sweep
     */
    @Scheduled(fixedDelayString = "${paycore.resolver.interval-ms:5000}")
    public int resolveStuckAuthorizingPayments() {
        return resolveStuckAuthorizingPayments(Duration.ofSeconds(2));
    }

    /**
     * Resolves in-flight {@code AUTHORIZING} payments older than the specified duration threshold.
     *
     * @param threshold minimum elapsed duration since {@code updated_at} before a payment is eligible for resolution
     * @return count of payments converged to terminal or authorized state
     */
    public int resolveStuckAuthorizingPayments(Duration threshold) {
        Instant cutoff = clockProvider.now().minus(threshold);
        List<Payment> stuckPayments = jdbcClient.sql("""
                SELECT id, merchant_id, amount, currency, status, captured_amount, refunded_amount,
                       psp_ref, metadata::text AS metadata_json, version, event_seq, created_at, updated_at
                FROM payments
                WHERE status = 'AUTHORIZING'
                  AND updated_at <= :cutoff
                ORDER BY updated_at ASC
                LIMIT 100
                """)
                .param("cutoff", Timestamp.from(cutoff))
                .query(this::mapPaymentRow)
                .list();

        int resolvedCount = 0;
        for (Payment stuck : stuckPayments) {
            if (stuck.pspRef() == null || stuck.pspRef().isBlank()) {
                continue;
            }
            PspClient.PspStatusLookupResult lookup = pspClient.lookupByPspRef(stuck.pspRef());
            if (lookup.state() == PspClient.PspLookupState.LOOKUP_ERROR) {
                continue;
            }

            Boolean converged = requiresNewTx.execute(status -> {
                Instant now = clockProvider.now();
                Timestamp nowTs = Timestamp.from(now);
                if (lookup.state() == PspClient.PspLookupState.AUTHORIZED) {
                    int updated = jdbcClient.sql("""
                            UPDATE payments
                            SET status = 'AUTHORIZED',
                                version = version + 1,
                                event_seq = event_seq + 1,
                                updated_at = :updatedAt
                            WHERE id = :id
                              AND status = 'AUTHORIZING'
                            """)
                            .param("updatedAt", nowTs)
                            .param("id", stuck.id())
                            .update();
                    if (updated == 1) {
                        Payment after = getPayment(stuck.merchantId(), stuck.id());
                        outboxWriter.writePaymentAuthorized(
                                after.id(),
                                after.merchantId(),
                                after.eventSeq(),
                                after.amount(),
                                after.currency(),
                                after.pspRef());
                        return true;
                    }
                } else {
                    // DECLINED or NOT_FOUND at PSP after timeout threshold -> finalize as FAILED
                    int updated = jdbcClient.sql("""
                            UPDATE payments
                            SET status = 'FAILED',
                                version = version + 1,
                                event_seq = event_seq + 1,
                                updated_at = :updatedAt
                            WHERE id = :id
                              AND status = 'AUTHORIZING'
                            """)
                            .param("updatedAt", nowTs)
                            .param("id", stuck.id())
                            .update();
                    if (updated == 1) {
                        Payment after = getPayment(stuck.merchantId(), stuck.id());
                        outboxWriter.writePaymentFailed(
                                after.id(),
                                after.merchantId(),
                                after.eventSeq(),
                                after.amount(),
                                after.currency(),
                                after.pspRef(),
                                "RESOLVED_" + lookup.state().name());
                        return true;
                    }
                }
                return false;
            });

            if (Boolean.TRUE.equals(converged)) {
                resolvedCount++;
                log.info("Resolved stuck AUTHORIZING payment {} via PSP lookup ({})", stuck.id(), lookup.state());
            }
        }
        return resolvedCount;
    }

    @Transactional(readOnly = true)
    public Payment getPayment(UUID merchantId, UUID paymentId) {
        return jdbcClient.sql("""
                SELECT id, merchant_id, amount, currency, status, captured_amount, refunded_amount,
                       psp_ref, metadata::text AS metadata_json, version, event_seq, created_at, updated_at
                FROM payments
                WHERE id = :id AND merchant_id = :merchantId
                """)
                .param("id", paymentId)
                .param("merchantId", merchantId)
                .query(this::mapPaymentRow)
                .optional()
                .orElseThrow(() -> PayCoreException.notFound("Payment", paymentId));
    }

    private Payment lockPaymentRow(UUID merchantId, UUID paymentId) {
        return jdbcClient.sql("""
                SELECT id, merchant_id, amount, currency, status, captured_amount, refunded_amount,
                       psp_ref, metadata::text AS metadata_json, version, event_seq, created_at, updated_at
                FROM payments
                WHERE id = :id AND merchant_id = :merchantId
                FOR UPDATE
                """)
                .param("id", paymentId)
                .param("merchantId", merchantId)
                .query(this::mapPaymentRow)
                .optional()
                .orElseThrow(() -> PayCoreException.notFound("Payment", paymentId));
    }

    private int countAttempts(UUID paymentId) {
        Integer count = jdbcClient.sql("SELECT COUNT(*) FROM payment_attempts WHERE payment_id = :paymentId")
                .param("paymentId", paymentId)
                .query(Integer.class)
                .single();
        return count != null ? count : 0;
    }

    private void insertAttemptRow(
            UUID paymentId, int attemptNo, String pspRef, String outcome, long latencyMs, Timestamp nowTs) {
        jdbcClient.sql("""
                INSERT INTO payment_attempts (id, payment_id, attempt_no, psp_ref, outcome, latency_ms, created_at)
                VALUES (:id, :paymentId, :attemptNo, :pspRef, :outcome, :latencyMs, :createdAt)
                ON CONFLICT (payment_id, attempt_no) DO UPDATE
                SET outcome = EXCLUDED.outcome,
                    latency_ms = EXCLUDED.latency_ms
                """)
                .param("id", UuidV7.generate(nowTs.getTime()))
                .param("paymentId", paymentId)
                .param("attemptNo", attemptNo)
                .param("pspRef", pspRef)
                .param("outcome", outcome)
                .param("latencyMs", latencyMs)
                .param("createdAt", nowTs)
                .update();
    }

    private Payment mapPaymentRow(ResultSet rs, int rowNum) throws SQLException {
        return new Payment(
                rs.getObject("id", UUID.class),
                rs.getObject("merchant_id", UUID.class),
                rs.getLong("amount"),
                rs.getString("currency"),
                PaymentStatus.valueOf(rs.getString("status")),
                rs.getLong("captured_amount"),
                rs.getLong("refunded_amount"),
                rs.getString("psp_ref"),
                fromJson(rs.getString("metadata_json")),
                rs.getLong("version"),
                rs.getLong("event_seq"),
                rs.getTimestamp("created_at").toInstant(),
                rs.getTimestamp("updated_at").toInstant());
    }

    private String toJson(Map<String, String> map) {
        try {
            return objectMapper.writeValueAsString(map != null ? map : Collections.emptyMap());
        } catch (Exception e) {
            return "{}";
        }
    }

    private Map<String, String> fromJson(String json) {
        if (json == null || json.isBlank()) {
            return Collections.emptyMap();
        }
        try {
            return objectMapper.readValue(json, MAP_TYPE);
        } catch (Exception e) {
            return Collections.emptyMap();
        }
    }

    public void setNaiveUnlockedRefundMode(boolean enabled) {
        this.naiveUnlockedRefundMode = enabled;
    }

    public boolean isNaiveUnlockedRefundMode() {
        return naiveUnlockedRefundMode;
    }

    public void setCrashHook(Consumer<String> crashHook) {
        this.crashHook = crashHook != null ? crashHook : stage -> {};
    }

    private record AuthClaim(Payment payment, int attemptNo, String pspRef, boolean alreadyAuthorized) {}

    public record CreatePaymentRequest(long amount, String currency, Map<String, String> metadata) {}

    public record RefundRequest(long amount, String reason) {}

    public record RefundResponse(
            UUID refundId,
            UUID paymentId,
            UUID merchantId,
            long refundAmount,
            long totalRefundedAmount,
            long capturedAmount,
            String currency,
            UUID ledgerTxnId,
            Instant createdAt) {}

    public record PayoutRequest(long amount, String currency) {}

    public record PayoutResponse(
            UUID payoutId,
            UUID merchantId,
            long amount,
            String currency,
            String status,
            UUID ledgerTxnId,
            Instant createdAt) {}
}
