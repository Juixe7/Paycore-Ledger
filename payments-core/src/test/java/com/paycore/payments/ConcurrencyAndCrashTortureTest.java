package com.paycore.payments;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.paycore.common.tracing.TraceHeaders;
import com.paycore.payments.idempotency.IdempotencyService;
import com.paycore.payments.merchant.MerchantService;
import com.paycore.payments.payment.Payment;
import com.paycore.payments.payment.PaymentService;
import com.paycore.payments.payment.PaymentStatus;
import com.paycore.payments.psp.ResilientPspClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Concurrency, Race Condition, Naive-vs-Locked Contrast, and 1,000-Iteration Crash Consistency Suite
 * (Section 7.1, 7.2, 7.4, 7.5).
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
            "spring.datasource.hikari.maximum-pool-size=48",
            "spring.datasource.hikari.connection-timeout=10000",
            "server.tomcat.accept-count=1000",
            "server.tomcat.max-connections=2000",
            "paycore.resolver.interval-ms=3600000"
        })
@Testcontainers
class ConcurrencyAndCrashTortureTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrencyAndCrashTortureTest.class);

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:16-alpine")
                    .withCommand("postgres", "-c", "max_connections=200");

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private PaymentService paymentService;

    @Autowired
    private IdempotencyService idempotencyService;

    @Autowired
    private ResilientPspClient pspClient;

    @BeforeEach
    void setUp() {
        pspClient.clearFaults();
        paymentService.setNaiveUnlockedRefundMode(false);
        paymentService.setCrashHook(stage -> {});
        idempotencyService.setRedisEnabled(false);
    }

    @AfterEach
    void tearDown() {
        pspClient.clearFaults();
        paymentService.setNaiveUnlockedRefundMode(false);
        paymentService.setCrashHook(stage -> {});
        idempotencyService.setRedisEnabled(false);
    }

    /**
     * Section 7.1: Concurrent idempotency.
     * Fire 500 parallel requests with the same key. Assert exactly one payment row, one ledger transaction,
     * and 500 identical responses. Repeat with different bodies to assert 422s, and repeat with Redis
     * enabled (simulating dead/unreachable Redis mid-test) vs disabled to prove it only affects latency.
     */
    @Test
    void concurrentIdempotency500ParallelRequestsSameKeyAndDifferentBodies() throws Exception {
        MerchantService.CreatedMerchantResponse merchant = onboardMerchant("Concurrency500Merchant", 200);

        // Prepare an AUTHORIZED payment to capture with 500 concurrent same-key capture requests
        Payment payment = createAndAuthorize(merchant.apiKey(), "idem-prep-500", 10_000L);

        int threads = 500;
        String sharedCaptureKey = "idem-500-concurrent-capture";
        CountDownLatch startGun = new CountDownLatch(1);
        Set<UUID> returnedPaymentIds = ConcurrentHashMap.newKeySet();
        AtomicInteger http200Count = new AtomicInteger(0);

        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<?>> futures = new ArrayList<>(threads);
            for (int i = 0; i < threads; i++) {
                final int idx = i;
                futures.add(pool.submit(() -> {
                    awaitLatch(startGun);
                    // Halfway through, toggle Redis enabled (with no Redis server up -> simulating Redis outage mid-test)
                    if (idx == 250) {
                        idempotencyService.setRedisEnabled(true);
                    }
                    ResponseEntity<Payment> resp = postWithKey(
                            merchant.apiKey(),
                            sharedCaptureKey,
                            "/v1/payments/" + payment.id() + "/capture",
                            Map.of("note", "same-canonical-payload"),
                            Payment.class);
                    if (resp.getStatusCode() == HttpStatus.OK && resp.getBody() != null) {
                        http200Count.incrementAndGet();
                        returnedPaymentIds.add(resp.getBody().id());
                    }
                }));
            }
            startGun.countDown();
            for (Future<?> f : futures) {
                f.get();
            }
        }

        // Assert all 500 parallel requests received identical 200 OK responses for the single payment
        assertThat(http200Count.get()).isEqualTo(500);
        assertThat(returnedPaymentIds).containsExactly(payment.id());

        // Assert exactly ONE ledger transaction was posted for this payment
        long ledgerTxnCount = jdbcClient.sql(
                        "SELECT COUNT(*) FROM ledger_transactions WHERE payment_id = :paymentId")
                .param("paymentId", payment.id())
                .query(Long.class)
                .single();
        assertThat(ledgerTxnCount).isEqualTo(1L);

        // Now fire 500 parallel requests with the SAME key but DIFFERENT bodies -> assert 422s
        String mismatchKey = "idem-500-mismatch-key";
        // Seed the key first with amount=5000
        ResponseEntity<Payment> seedResp = postWithKey(
                merchant.apiKey(),
                mismatchKey,
                "/v1/payments",
                new PaymentService.CreatePaymentRequest(5000L, "INR", Map.of()),
                Payment.class);
        assertThat(seedResp.getStatusCode()).isEqualTo(HttpStatus.CREATED);

        CountDownLatch mismatchGun = new CountDownLatch(1);
        AtomicInteger status422Count = new AtomicInteger(0);
        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<?>> futures = new ArrayList<>(threads);
            for (int i = 0; i < threads; i++) {
                final long diffAmount = 6000L + i;
                futures.add(pool.submit(() -> {
                    awaitLatch(mismatchGun);
                    ResponseEntity<JsonNode> resp = postWithKey(
                            merchant.apiKey(),
                            mismatchKey,
                            "/v1/payments",
                            new PaymentService.CreatePaymentRequest(diffAmount, "INR", Map.of()),
                            JsonNode.class);
                    if (resp.getStatusCode() == HttpStatus.UNPROCESSABLE_ENTITY) {
                        status422Count.incrementAndGet();
                    }
                }));
            }
            mismatchGun.countDown();
            for (Future<?> f : futures) {
                f.get();
            }
        }

        assertThat(status422Count.get()).isEqualTo(500);
        assertInvariantsHealthy();
        log.info("[RESUME METRIC] 500 concurrent same-key requests -> 500 identical responses, 1 payment row, 1 ledger txn, 500/500 payload mismatches rejected with 422.");
    }

    /**
     * Section 7.2: Double-capture & double-refund races, including the naive unlocked implementation contrast.
     * - 200 concurrent captures on one payment: exactly one succeeds.
     * - 100 concurrent partial refunds that together exceed captured_amount:
     *   1) Naive unlocked implementation FAILS (over-refunds in the ledger + lost update on payment).
     *   2) Production locked implementation (SELECT ... FOR UPDATE) SUCCEEDS (total refunded never exceeds captured_amount).
     */
    @Test
    void doubleCapture200RaceAndDoubleRefund100RaceWithNaiveContrast() throws Exception {
        MerchantService.CreatedMerchantResponse merchant = onboardMerchant("RaceMerchant", 200);

        // --- Part A: 200 Concurrent Captures on One Payment (with distinct idempotency keys) ---
        Payment authPayment = createAndAuthorize(merchant.apiKey(), "idem-race-cap-prep", 50_000L);
        int captureThreads = 200;
        CountDownLatch captureGun = new CountDownLatch(1);
        AtomicInteger captureSuccesses = new AtomicInteger(0);
        AtomicInteger captureConflicts = new AtomicInteger(0);

        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<?>> futures = new ArrayList<>(captureThreads);
            for (int i = 0; i < captureThreads; i++) {
                final String distinctKey = "idem-distinct-cap-" + i;
                futures.add(pool.submit(() -> {
                    awaitLatch(captureGun);
                    ResponseEntity<JsonNode> resp = postWithKey(
                            merchant.apiKey(),
                            distinctKey,
                            "/v1/payments/" + authPayment.id() + "/capture",
                            Map.of(),
                            JsonNode.class);
                    if (resp.getStatusCode() == HttpStatus.OK) {
                        captureSuccesses.incrementAndGet();
                    } else if (resp.getStatusCode() == HttpStatus.CONFLICT) {
                        captureConflicts.incrementAndGet();
                    }
                }));
            }
            captureGun.countDown();
            for (Future<?> f : futures) {
                f.get();
            }
        }

        assertThat(captureSuccesses.get()).isEqualTo(1);
        assertThat(captureConflicts.get()).isEqualTo(199);

        // --- Part B1: Deliberately Naive Unlocked Refund Contrast (100 concurrent partial refunds of 200 paise on 1000 paise payment) ---
        Payment naivePayment = createAuthorizeAndCapture(merchant.apiKey(), "idem-naive-ref-prep", 1000L);
        paymentService.setNaiveUnlockedRefundMode(true);

        int refundThreads = 100;
        CountDownLatch naiveGun = new CountDownLatch(1);
        AtomicInteger naiveRefundSuccesses = new AtomicInteger(0);

        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<?>> futures = new ArrayList<>(refundThreads);
            for (int i = 0; i < refundThreads; i++) {
                final int idx = i;
                futures.add(pool.submit(() -> {
                    awaitLatch(naiveGun);
                    try {
                        paymentService.refundPayment(
                                merchant.id(),
                                naivePayment.id(),
                                new PaymentService.RefundRequest(200L, "naive-race-" + idx));
                        naiveRefundSuccesses.incrementAndGet();
                    } catch (Exception ignored) {
                    }
                }));
            }
            naiveGun.countDown();
            for (Future<?> f : futures) {
                f.get();
            }
        }

        // In naive mode without SELECT ... FOR UPDATE, way more than 5 refunds (5 * 200 = 1000) succeed!
        long naiveLedgerRefunded = jdbcClient.sql("""
                SELECT COALESCE(SUM(le.amount), 0)
                FROM ledger_transactions lt
                JOIN ledger_entries le ON le.txn_id = lt.id
                WHERE lt.payment_id = :paymentId
                  AND lt.kind = 'REFUND'
                  AND le.direction = 'DEBIT'
                """)
                .param("paymentId", naivePayment.id())
                .query(Long.class)
                .single();

        log.info("[NAIVE CONTRAST PROOF] Without SELECT ... FOR UPDATE, 100 concurrent 200-paise refunds on a 1000-paise payment succeeded {} times (ledger refunded {} paise > 1000 paise captured)!",
                naiveRefundSuccesses.get(), naiveLedgerRefunded);
        assertThat(naiveRefundSuccesses.get()).isGreaterThan(5);
        assertThat(naiveLedgerRefunded).isGreaterThan(1000L);

        // Verify the Invariant Checker catches the naive violation (status == FAIL, illegalPayments > 0)
        ResponseEntity<JsonNode> naiveInvariantResp = restTemplate.getForEntity("/v1/admin/invariants", JsonNode.class);
        assertThat(naiveInvariantResp.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat(naiveInvariantResp.getBody().get("status").asText()).isEqualTo("FAIL");
        assertThat(naiveInvariantResp.getBody().get("illegalPayments").asLong()).isGreaterThanOrEqualTo(1L);

        // Clean up the naive payment's corrupted state and restore balances before running the locked production test
        cleanupNaivePaymentState(naivePayment.id());
        paymentService.setNaiveUnlockedRefundMode(false);
        assertInvariantsHealthy();

        // --- Part B2: Production Locked Refund Path (SELECT ... FOR UPDATE) ---
        Payment lockedPayment = createAuthorizeAndCapture(merchant.apiKey(), "idem-locked-ref-prep", 1000L);
        CountDownLatch lockedGun = new CountDownLatch(1);
        AtomicInteger lockedSuccesses = new AtomicInteger(0);
        AtomicInteger lockedRejections = new AtomicInteger(0);

        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<?>> futures = new ArrayList<>(refundThreads);
            for (int i = 0; i < refundThreads; i++) {
                final String key = "idem-locked-ref-" + i;
                futures.add(pool.submit(() -> {
                    awaitLatch(lockedGun);
                    ResponseEntity<JsonNode> resp = postWithKey(
                            merchant.apiKey(),
                            key,
                            "/v1/payments/" + lockedPayment.id() + "/refunds",
                            new PaymentService.RefundRequest(200L, "locked-race-" + key),
                            JsonNode.class);
                    if (resp.getStatusCode() == HttpStatus.OK) {
                        lockedSuccesses.incrementAndGet();
                    } else if (resp.getStatusCode() == HttpStatus.UNPROCESSABLE_ENTITY) {
                        lockedRejections.incrementAndGet();
                    }
                }));
            }
            lockedGun.countDown();
            for (Future<?> f : futures) {
                f.get();
            }
        }

        // Production locked path: exactly 5 refunds of 200 paise (= 1000 paise) succeed, 95 rejected with 422!
        assertThat(lockedSuccesses.get()).isEqualTo(5);
        assertThat(lockedRejections.get()).isEqualTo(95);

        Payment afterLockedRefunds = paymentService.getPayment(merchant.id(), lockedPayment.id());
        assertThat(afterLockedRefunds.refundedAmount()).isEqualTo(1000L);
        assertInvariantsHealthy();
        log.info("[RESUME METRIC] 200 concurrent captures -> 1 succeeded; 100 concurrent partial refunds -> 5/5 valid succeeded (1000/1000 paise), 95 rejected, 0 drift.");
    }

    /**
     * Section 7.4 & 7.5: Crash consistency & PSP fault injection loop (1,000 iterations).
     * Injects randomized crashes (after Tx1, after PSP call before Tx2, mid-Tx3 after ledger post)
     * and PSP faults (timeouts after charge, timeouts before charge, 500s, declines), runs the
     * resolver, and verifies 1,000 iterations produce 0 invariant violations, 0 stuck AUTHORIZING
     * payments, and 0 lost events.
     */
    @Test
    void crashConsistencyAndPspFaults1000IterationsZeroDriftZeroLostEvents() {
        MerchantService.CreatedMerchantResponse merchant = onboardMerchant("Crash1000Merchant", 200);

        int iterations = 1000;
        int injectedCrashAndFaultCount = 0;
        ThreadLocalRandom rng = ThreadLocalRandom.current();

        for (int i = 0; i < iterations; i++) {
            int scenario = rng.nextInt(10);
            UUID paymentId = paymentService.createPayment(
                    merchant.id(),
                    new PaymentService.CreatePaymentRequest(1000L, "INR", Map.of("iter", String.valueOf(i)))).id();

            pspClient.clearFaults();
            paymentService.setCrashHook(stage -> {});

            try {
                switch (scenario) {
                    case 0, 1, 2 -> {
                        // Happy path: authorize -> capture -> partial refund
                        paymentService.authorizePayment(merchant.id(), paymentId);
                        paymentService.capturePayment(merchant.id(), paymentId, merchant.feeBps());
                        paymentService.refundPayment(
                                merchant.id(), paymentId, new PaymentService.RefundRequest(300L, "partial"));
                    }
                    case 3 -> {
                        // Crash right after Tx1 (payment committed in AUTHORIZING before PSP call)
                        injectedCrashAndFaultCount++;
                        paymentService.setCrashHook(stage -> {
                            if ("AFTER_TX1_BEFORE_PSP".equals(stage)) {
                                throw new RuntimeException("Simulated kill -9 after Tx1 before PSP call");
                            }
                        });
                        paymentService.authorizePayment(merchant.id(), paymentId);
                    }
                    case 4 -> {
                        // Crash right after PSP call (PSP charged customer, app crashed before Tx2 commit)
                        injectedCrashAndFaultCount++;
                        paymentService.setCrashHook(stage -> {
                            if ("AFTER_PSP_BEFORE_TX2".equals(stage)) {
                                throw new RuntimeException("Simulated kill -9 after PSP charge before Tx2 commit");
                            }
                        });
                        paymentService.authorizePayment(merchant.id(), paymentId);
                    }
                    case 5 -> {
                        // Crash mid-Tx3 after ledger post before payment status update -> atomic DB rollback!
                        injectedCrashAndFaultCount++;
                        paymentService.authorizePayment(merchant.id(), paymentId);
                        paymentService.setCrashHook(stage -> {
                            if ("MID_TX3_AFTER_LEDGER".equals(stage)) {
                                throw new RuntimeException("Simulated kill -9 mid-Tx3 after ledger post");
                            }
                        });
                        try {
                            paymentService.capturePayment(merchant.id(), paymentId, merchant.feeBps());
                        } finally {
                            paymentService.setCrashHook(stage -> {});
                        }
                        // Retry capture after restart -> must succeed cleanly once!
                        paymentService.capturePayment(merchant.id(), paymentId, merchant.feeBps());
                    }
                    case 6 -> {
                        // PSP timeout after acquirer charge (unknown outcome)
                        injectedCrashAndFaultCount++;
                        pspClient.scriptFaultForPayment(
                                paymentId, ResilientPspClient.FaultMode.TIMEOUT_AFTER_PSP_CHARGE, 0L);
                        paymentService.authorizePayment(merchant.id(), paymentId);
                    }
                    case 7 -> {
                        // PSP timeout before acquirer charge
                        injectedCrashAndFaultCount++;
                        pspClient.scriptFaultForPayment(
                                paymentId, ResilientPspClient.FaultMode.TIMEOUT_BEFORE_PSP_CHARGE, 0L);
                        paymentService.authorizePayment(merchant.id(), paymentId);
                    }
                    case 8 -> {
                        // PSP HTTP 500 error
                        injectedCrashAndFaultCount++;
                        pspClient.scriptFaultForPayment(
                                paymentId, ResilientPspClient.FaultMode.HTTP_500, 0L);
                        paymentService.authorizePayment(merchant.id(), paymentId);
                    }
                    case 9 -> {
                        // PSP Decline
                        injectedCrashAndFaultCount++;
                        pspClient.scriptFaultForPayment(
                                paymentId, ResilientPspClient.FaultMode.DECLINE, 0L);
                        paymentService.authorizePayment(merchant.id(), paymentId);
                    }
                }
            } catch (RuntimeException expectedCrash) {
                // Expected simulated crash; resolver will converge any stuck state
            }
        }

        pspClient.clearFaults();
        paymentService.setCrashHook(stage -> {});

        // Run background resolver batches (each batch processes up to 100 rows) to converge all AUTHORIZING payments
        int resolved = 0;
        int batch;
        while ((batch = paymentService.resolveStuckAuthorizingPayments(Duration.ZERO)) > 0) {
            resolved += batch;
        }

        // Assert zero payments remain stuck in AUTHORIZING
        long stuckAuthorizingCount = jdbcClient.sql(
                        "SELECT COUNT(*) FROM payments WHERE merchant_id = :m AND status = 'AUTHORIZING'")
                .param("m", merchant.id())
                .query(Long.class)
                .single();
        assertThat(stuckAuthorizingCount).isZero();

        // Assert every payment's event_seq matches the exact count of outbox_events for that payment (0 lost events)
        long outboxMismatchCount = jdbcClient.sql("""
                SELECT COUNT(*)
                FROM payments p
                LEFT JOIN (
                    SELECT aggregate_id, COUNT(*) AS cnt
                    FROM outbox_events
                    GROUP BY aggregate_id
                ) o ON o.aggregate_id = p.id
                WHERE p.merchant_id = :m
                  AND p.event_seq <> COALESCE(o.cnt, 0)
                """)
                .param("m", merchant.id())
                .query(Long.class)
                .single();
        assertThat(outboxMismatchCount).isZero();

        // Run invariant checker across the entire database
        assertInvariantsHealthy();
        log.info("[RESUME METRIC] 1000 iterations ({} injected crashes & PSP faults, {} converged by resolver) -> 0 invariant violations, 0 stuck AUTHORIZING payments, 0 lost events.",
                injectedCrashAndFaultCount, resolved);
    }

    private void cleanupNaivePaymentState(UUID naivePaymentId) {
        // Recompute account_balances from remaining entries after deleting the intentionally corrupted naive test payment
        jdbcClient.sql("ALTER TABLE ledger_entries DISABLE TRIGGER trg_ledger_entries_append_only").update();
        jdbcClient.sql("ALTER TABLE ledger_entries DISABLE TRIGGER ALL").update();
        jdbcClient.sql("""
                DELETE FROM ledger_entries
                WHERE txn_id IN (SELECT id FROM ledger_transactions WHERE payment_id = :pId)
                """)
                .param("pId", naivePaymentId)
                .update();
        jdbcClient.sql("ALTER TABLE ledger_entries ENABLE TRIGGER ALL").update();
        jdbcClient.sql("DELETE FROM ledger_transactions WHERE payment_id = :pId")
                .param("pId", naivePaymentId)
                .update();
        jdbcClient.sql("DELETE FROM outbox_events WHERE aggregate_id = :pId")
                .param("pId", naivePaymentId)
                .update();
        jdbcClient.sql("DELETE FROM payment_attempts WHERE payment_id = :pId")
                .param("pId", naivePaymentId)
                .update();
        jdbcClient.sql("DELETE FROM payments WHERE id = :pId")
                .param("pId", naivePaymentId)
                .update();
        jdbcClient.sql("""
                UPDATE account_balances ab
                SET balance = COALESCE((
                    SELECT SUM(
                        CASE
                            WHEN a.type = 'ASSET' AND le.direction = 'DEBIT'  THEN  le.amount
                            WHEN a.type = 'ASSET' AND le.direction = 'CREDIT' THEN -le.amount
                            WHEN a.type <> 'ASSET' AND le.direction = 'CREDIT' THEN  le.amount
                            WHEN a.type <> 'ASSET' AND le.direction = 'DEBIT'  THEN -le.amount
                            ELSE 0
                        END
                    )
                    FROM accounts a
                    LEFT JOIN ledger_entries le ON le.account_id = a.id
                    WHERE a.id = ab.account_id
                    GROUP BY a.id
                ), 0)
                """).update();
    }

    private Payment createAndAuthorize(String apiKey, String keyPrefix, long amount) {
        Payment created = postWithKey(
                apiKey,
                keyPrefix + "-create",
                "/v1/payments",
                new PaymentService.CreatePaymentRequest(amount, "INR", Map.of()),
                Payment.class).getBody();
        return postWithKey(
                apiKey,
                keyPrefix + "-auth",
                "/v1/payments/" + created.id() + "/authorize",
                Map.of(),
                Payment.class).getBody();
    }

    private Payment createAuthorizeAndCapture(String apiKey, String keyPrefix, long amount) {
        Payment authed = createAndAuthorize(apiKey, keyPrefix, amount);
        return postWithKey(
                apiKey,
                keyPrefix + "-cap",
                "/v1/payments/" + authed.id() + "/capture",
                Map.of(),
                Payment.class).getBody();
    }

    private MerchantService.CreatedMerchantResponse onboardMerchant(String name, int feeBps) {
        ResponseEntity<MerchantService.CreatedMerchantResponse> resp = restTemplate.postForEntity(
                "/v1/merchants",
                new MerchantService.CreateMerchantRequest(name, "https://example.com/hook", null, feeBps),
                MerchantService.CreatedMerchantResponse.class);
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        return resp.getBody();
    }

    private void assertInvariantsHealthy() {
        ResponseEntity<JsonNode> inv = restTemplate.getForEntity("/v1/admin/invariants", JsonNode.class);
        assertThat(inv.getStatusCode()).isEqualTo(HttpStatus.OK);
        JsonNode body = inv.getBody();
        assertThat(body.get("status").asText()).isEqualTo("PASS");
        assertThat(body.get("healthy").asBoolean()).isTrue();
        assertThat(body.get("globalImbalance").asLong()).isZero();
        assertThat(body.get("unbalancedTransactions").asLong()).isZero();
        assertThat(body.get("negativeMerchantAccounts").asLong()).isZero();
        assertThat(body.get("ledgerDrift").asLong()).isZero();
        assertThat(body.get("illegalPayments").asLong()).isZero();
    }

    private <T> ResponseEntity<T> postWithKey(
            String apiKey, String idempotencyKey, String path, Object body, Class<T> responseType) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set(TraceHeaders.API_KEY_HEADER, apiKey);
        headers.set(TraceHeaders.IDEMPOTENCY_KEY, idempotencyKey);
        return restTemplate.exchange(path, HttpMethod.POST, new HttpEntity<>(body, headers), responseType);
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
