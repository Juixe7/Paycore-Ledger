package com.paycore.payments;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.paycore.common.events.v1.PaymentEvent;
import com.paycore.common.ids.UuidV7;
import com.paycore.common.tracing.TraceHeaders;
import com.paycore.payments.merchant.MerchantService;
import com.paycore.payments.payment.Payment;
import com.paycore.payments.payment.PaymentService;
import com.paycore.payments.payment.PaymentStatus;
import com.paycore.payments.psp.ResilientPspClient;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"paycore.resolver.interval-ms=3600000"})
@Testcontainers
class PaymentsCoreIntegrationTest {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:16-alpine");

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private TransactionTemplate transactionTemplate;

    @Autowired
    private PaymentService paymentService;

    @Autowired
    private ResilientPspClient pspClient;

    @Autowired
    private ObjectMapper objectMapper;

    @BeforeEach
    void resetFaults() {
        pspClient.clearFaults();
    }

    @Test
    void merchantOnboardingHashesApiKeyAndBlocksSsrfWebhookUrls() {
        // 1. SSRF protection blocks loopback & metadata URLs at registration
        ResponseEntity<JsonNode> ssrfLoopback = restTemplate.postForEntity(
                "/v1/merchants",
                new MerchantService.CreateMerchantRequest("EvilLoopback", "http://127.0.0.1:8080/hook", null, 200),
                JsonNode.class);
        assertThat(ssrfLoopback.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
        assertThat(ssrfLoopback.getBody().get("errorCode").asText()).isEqualTo("SSRF_BLOCKED_WEBHOOK_URL");

        ResponseEntity<JsonNode> ssrfMetadata = restTemplate.postForEntity(
                "/v1/merchants",
                new MerchantService.CreateMerchantRequest("EvilMetadata", "http://169.254.169.254/latest/meta-data", null, 200),
                JsonNode.class);
        assertThat(ssrfMetadata.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);

        // 2. Valid onboarding stores only SHA-256 hash of API key
        ResponseEntity<MerchantService.CreatedMerchantResponse> created = restTemplate.postForEntity(
                "/v1/merchants",
                new MerchantService.CreateMerchantRequest("Acme Payments India", "https://example.com/webhooks", null, 200),
                MerchantService.CreatedMerchantResponse.class);
        assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        MerchantService.CreatedMerchantResponse merchant = created.getBody();
        assertThat(merchant).isNotNull();
        assertThat(merchant.apiKey()).startsWith("pc_live_");

        String storedHash = jdbcClient.sql("SELECT api_key_hash FROM merchants WHERE id = :id")
                .param("id", merchant.id())
                .query(String.class)
                .single();
        assertThat(storedHash).isNotEqualTo(merchant.apiKey());
        assertThat(storedHash).hasSize(64);

        // 3. Unauthenticated request returns 401 problem+json
        ResponseEntity<JsonNode> unauth = restTemplate.getForEntity("/v1/merchants/me/balance", JsonNode.class);
        assertThat(unauth.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(unauth.getBody().get("errorCode").asText()).isEqualTo("UNAUTHORIZED_MERCHANT");
    }

    @Test
    void fullMoneyPathCreateAuthorizeCapturePartialRefundsPayoutAndLedgerInvariants() throws Exception {
        MerchantService.CreatedMerchantResponse merchant = onboardMerchant("RazorStore", 200); // 2% fee

        // 1. Create payment intent of 1000 paise
        ResponseEntity<Payment> createResp = postWithKey(
                merchant.apiKey(),
                "idem-create-1",
                "/v1/payments",
                new PaymentService.CreatePaymentRequest(1000L, "INR", Map.of("order_id", "ord_101")),
                Payment.class);
        assertThat(createResp.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        Payment created = createResp.getBody();
        assertThat(created.status()).isEqualTo(PaymentStatus.CREATED);
        assertThat(created.amount()).isEqualTo(1000L);

        // 2. Authorize payment (Tx1 CREATED->AUTHORIZING, PSP call, Tx2 AUTHORIZING->AUTHORIZED)
        ResponseEntity<Payment> authResp = postWithKey(
                merchant.apiKey(),
                "idem-auth-1",
                "/v1/payments/" + created.id() + "/authorize",
                Map.of(),
                Payment.class);
        assertThat(authResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(authResp.getBody().status()).isEqualTo(PaymentStatus.AUTHORIZED);
        assertThat(authResp.getBody().pspRef()).isEqualTo(created.id() + ":1");

        // 3. Capture payment (1000 paise -> 980 merchant payable, 20 platform fee)
        ResponseEntity<Payment> captureResp = postWithKey(
                merchant.apiKey(),
                "idem-cap-1",
                "/v1/payments/" + created.id() + "/capture",
                Map.of(),
                Payment.class);
        assertThat(captureResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(captureResp.getBody().status()).isEqualTo(PaymentStatus.CAPTURED);
        assertThat(captureResp.getBody().capturedAmount()).isEqualTo(1000L);

        // 4. Verify merchant balance == 980 paise
        assertThat(fetchMerchantBalance(merchant.apiKey())).isEqualTo(980L);

        // 5. Partial refund #1 of 400 paise (fee not returned policy -> merchant payable 980 - 400 = 580)
        ResponseEntity<PaymentService.RefundResponse> refund1 = postWithKey(
                merchant.apiKey(),
                "idem-ref-1",
                "/v1/payments/" + created.id() + "/refunds",
                new PaymentService.RefundRequest(400L, "customer_return_item_1"),
                PaymentService.RefundResponse.class);
        assertThat(refund1.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(refund1.getBody().totalRefundedAmount()).isEqualTo(400L);
        assertThat(fetchMerchantBalance(merchant.apiKey())).isEqualTo(580L);

        // 6. Partial refund #2 of 300 paise (total refunded = 700, merchant payable = 280)
        ResponseEntity<PaymentService.RefundResponse> refund2 = postWithKey(
                merchant.apiKey(),
                "idem-ref-2",
                "/v1/payments/" + created.id() + "/refunds",
                new PaymentService.RefundRequest(300L, "customer_return_item_2"),
                PaymentService.RefundResponse.class);
        assertThat(refund2.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(refund2.getBody().totalRefundedAmount()).isEqualTo(700L);
        assertThat(fetchMerchantBalance(merchant.apiKey())).isEqualTo(280L);

        ResponseEntity<Payment> afterPartialRefunds = getWithKey(
                merchant.apiKey(), "/v1/payments/" + created.id(), Payment.class);
        assertThat(afterPartialRefunds.getBody().status()).isEqualTo(PaymentStatus.PARTIALLY_REFUNDED);

        // 7. Partial refund #3 of 400 paise (700 + 400 > 1000 captured) must fail with 422
        ResponseEntity<JsonNode> overRefund = postWithKey(
                merchant.apiKey(),
                "idem-ref-3",
                "/v1/payments/" + created.id() + "/refunds",
                new PaymentService.RefundRequest(400L, "over_refund"),
                JsonNode.class);
        assertThat(overRefund.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
        assertThat(overRefund.getBody().get("errorCode").asText()).isEqualTo("REFUND_EXCEEDS_CAPTURED_AMOUNT");

        // 8. Payout of 200 paise (merchant payable 280 - 200 = 80)
        ResponseEntity<PaymentService.PayoutResponse> payoutResp = postWithKey(
                merchant.apiKey(),
                "idem-payout-1",
                "/v1/payouts",
                new PaymentService.PayoutRequest(200L, "INR"),
                PaymentService.PayoutResponse.class);
        assertThat(payoutResp.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(fetchMerchantBalance(merchant.apiKey())).isEqualTo(80L);

        // 9. Verify ledger entries for payment (3 capture entries + 2 refund1 entries + 2 refund2 entries = 7 entries)
        ResponseEntity<JsonNode> ledgerEntriesResp = getWithKey(
                merchant.apiKey(), "/v1/payments/" + created.id() + "/ledger", JsonNode.class);
        assertThat(ledgerEntriesResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(ledgerEntriesResp.getBody().size()).isEqualTo(7);

        // 10. Verify outbox_events contain valid Protobuf PaymentEvent payloads with gapless event_seq 1..4
        List<byte[]> payloads = jdbcClient.sql("""
                SELECT payload FROM outbox_events
                WHERE aggregate_id = :paymentId
                ORDER BY event_seq ASC
                """)
                .param("paymentId", created.id())
                .query((rs, rowNum) -> rs.getBytes("payload"))
                .list();
        assertThat(payloads).hasSize(4);
        PaymentEvent ev1 = PaymentEvent.parseFrom(payloads.get(0));
        PaymentEvent ev2 = PaymentEvent.parseFrom(payloads.get(1));
        PaymentEvent ev3 = PaymentEvent.parseFrom(payloads.get(2));
        PaymentEvent ev4 = PaymentEvent.parseFrom(payloads.get(3));
        assertThat(ev1.getEventSeq()).isEqualTo(1L);
        assertThat(ev1.getEventType()).isEqualTo("payment.authorized");
        assertThat(ev2.getEventSeq()).isEqualTo(2L);
        assertThat(ev2.getEventType()).isEqualTo("payment.captured");
        assertThat(ev3.getEventSeq()).isEqualTo(3L);
        assertThat(ev3.getEventType()).isEqualTo("refund.processed");
        assertThat(ev4.getEventSeq()).isEqualTo(4L);
        assertThat(ev4.getEventType()).isEqualTo("refund.processed");

        // 11. Run Invariant Checker (/v1/admin/invariants) -> must report PASS and 0 drift
        assertInvariantsHealthy();
    }

    @Test
    void idempotencyReplaysSameBodyAndRejectsDifferentBodyWith422() {
        MerchantService.CreatedMerchantResponse merchant = onboardMerchant("IdemMerchant", 200);

        // First call creates payment
        ResponseEntity<Payment> first = postWithKey(
                merchant.apiKey(),
                "idem-key-abc",
                "/v1/payments",
                Map.of("amount", 2500, "currency", "INR"),
                Payment.class);
        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.CREATED);

        // Replay with same key and reordered JSON keys -> must return identical 201 Payment
        ResponseEntity<Payment> replay = postWithKey(
                merchant.apiKey(),
                "idem-key-abc",
                "/v1/payments",
                Map.of("currency", "INR", "amount", 2500),
                Payment.class);
        assertThat(replay.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(replay.getBody().id()).isEqualTo(first.getBody().id());

        // Replay with same key and different amount -> must return 422 Unprocessable Entity
        ResponseEntity<JsonNode> mismatch = postWithKey(
                merchant.apiKey(),
                "idem-key-abc",
                "/v1/payments",
                Map.of("amount", 9999, "currency", "INR"),
                JsonNode.class);
        assertThat(mismatch.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
        assertThat(mismatch.getBody().get("errorCode").asText()).isEqualTo("IDEMPOTENCY_KEY_PAYLOAD_MISMATCH");
    }

    @Test
    void unknownOutcomePspTimeoutLeavesPaymentAuthorizingUntilResolverConvergesIt() {
        MerchantService.CreatedMerchantResponse merchant = onboardMerchant("TimeoutMerchant", 200);

        Payment created = postWithKey(
                merchant.apiKey(),
                "idem-timeout-create",
                "/v1/payments",
                new PaymentService.CreatePaymentRequest(5000L, "INR", Map.of()),
                Payment.class).getBody();

        // Script PSP to charge the card internally and then time out before responding
        pspClient.scriptFaultForPayment(
                created.id(), ResilientPspClient.FaultMode.TIMEOUT_AFTER_PSP_CHARGE, 0L);

        ResponseEntity<Payment> authResp = postWithKey(
                merchant.apiKey(),
                "idem-timeout-auth",
                "/v1/payments/" + created.id() + "/authorize",
                Map.of(),
                Payment.class);

        // Payment must stay AUTHORIZING (unknown outcome) — never guess, never double-charge!
        assertThat(authResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(authResp.getBody().status()).isEqualTo(PaymentStatus.AUTHORIZING);

        // Now run the stuck-payment resolver job -> queries PSP by psp_ref and converges to AUTHORIZED
        int resolved = paymentService.resolveStuckAuthorizingPayments(Duration.ZERO);
        assertThat(resolved).isGreaterThanOrEqualTo(1);

        ResponseEntity<Payment> afterResolve = getWithKey(
                merchant.apiKey(), "/v1/payments/" + created.id(), Payment.class);
        assertThat(afterResolve.getBody().status()).isEqualTo(PaymentStatus.AUTHORIZED);

        assertInvariantsHealthy();
    }

    @Test
    void databaseTriggersEnforceAppendOnlyAndBalancedLedgerTransactions() {
        // 1. Verify append-only trigger rejects UPDATE and DELETE on ledger_entries
        assertThatThrownBy(() -> jdbcClient.sql("UPDATE ledger_entries SET amount = amount + 1").update())
                .isInstanceOf(DataAccessException.class)
                .hasMessageContaining("append-only");

        assertThatThrownBy(() -> jdbcClient.sql("DELETE FROM ledger_entries").update())
                .isInstanceOf(DataAccessException.class)
                .hasMessageContaining("append-only");

        // 2. Verify deferred constraint trigger rejects unbalanced ledger transaction at commit time
        assertThatThrownBy(() -> transactionTemplate.executeWithoutResult(status -> {
            UUID badTxnId = UuidV7.generate();
            jdbcClient.sql("""
                    INSERT INTO ledger_transactions (id, payment_id, kind, posted_by)
                    VALUES (:id, NULL, 'CAPTURE', 'tamper-test')
                    """)
                    .param("id", badTxnId)
                    .update();

            // Insert only 1 DEBIT entry (100) and 1 unequal CREDIT entry (50)
            jdbcClient.sql("""
                    INSERT INTO ledger_entries (id, txn_id, account_id, direction, amount)
                    VALUES (:id1, :txnId, '00000000-0000-7000-8000-000000000001', 'DEBIT', 100),
                           (:id2, :txnId, '00000000-0000-7000-8000-000000000002', 'CREDIT', 50)
                    """)
                    .param("id1", UuidV7.generate())
                    .param("id2", UuidV7.generate())
                    .param("txnId", badTxnId)
                    .update();
        }))
                .isInstanceOf(DataAccessException.class)
                .hasMessageContaining("Unbalanced ledger transaction");

        assertInvariantsHealthy();
    }

    private MerchantService.CreatedMerchantResponse onboardMerchant(String name, int feeBps) {
        ResponseEntity<MerchantService.CreatedMerchantResponse> resp = restTemplate.postForEntity(
                "/v1/merchants",
                new MerchantService.CreateMerchantRequest(name, "https://example.com/hook", null, feeBps),
                MerchantService.CreatedMerchantResponse.class);
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        return resp.getBody();
    }

    private long fetchMerchantBalance(String apiKey) {
        ResponseEntity<JsonNode> resp = getWithKey(apiKey, "/v1/merchants/me/balance", JsonNode.class);
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.OK);
        return resp.getBody().get("balance").asLong();
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
        if (idempotencyKey != null) {
            headers.set(TraceHeaders.IDEMPOTENCY_KEY, idempotencyKey);
        }
        return restTemplate.exchange(path, HttpMethod.POST, new HttpEntity<>(body, headers), responseType);
    }

    private <T> ResponseEntity<T> getWithKey(String apiKey, String path, Class<T> responseType) {
        HttpHeaders headers = new HttpHeaders();
        headers.set(TraceHeaders.API_KEY_HEADER, apiKey);
        return restTemplate.exchange(path, HttpMethod.GET, new HttpEntity<>(headers), responseType);
    }
}
