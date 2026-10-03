package com.paycore.webhook;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.Timestamp;
import com.paycore.common.events.v1.PaymentCaptured;
import com.paycore.common.events.v1.PaymentEvent;
import com.paycore.common.ids.UuidV7;
import com.paycore.common.tracing.TraceHeaders;
import com.paycore.webhook.WebhookDeliveryService.DeliveryOutcome;
import com.paycore.webhook.WebhookDeliveryService.DeliveryStatus;
import com.paycore.webhook.WebhookDeliveryService.DlqEventRow;
import com.paycore.webhook.WebhookDeliveryService.WebhookDeliveryRow;
import com.sun.net.httpserver.HttpServer;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class WebhookServiceIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("paycore_webhook_test")
            .withUsername("paycore")
            .withPassword("paycore");

    private static HttpServer mockMerchantServer;
    private static int mockMerchantPort;
    private static final AtomicInteger merchantResponseStatus = new AtomicInteger(200);
    private static final AtomicBoolean failSeventyPercentMode = new AtomicBoolean(false);
    private static final java.util.concurrent.ConcurrentHashMap<String, AtomicInteger> perEventAttempts =
            new java.util.concurrent.ConcurrentHashMap<>();
    private static final List<ReceivedWebhook> receivedWebhooks = new CopyOnWriteArrayList<>();

    record ReceivedWebhook(String signatureHeader, String timestampHeader, String eventIdHeader, String body) {}

    @BeforeAll
    static void startMockMerchantServer() throws Exception {
        mockMerchantServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        mockMerchantPort = mockMerchantServer.getAddress().getPort();
        mockMerchantServer.createContext("/webhook", exchange -> {
            byte[] bodyBytes = exchange.getRequestBody().readAllBytes();
            String body = new String(bodyBytes, StandardCharsets.UTF_8);
            String sig = exchange.getRequestHeaders().getFirst(TraceHeaders.WEBHOOK_SIGNATURE);
            String ts = exchange.getRequestHeaders().getFirst(TraceHeaders.WEBHOOK_TIMESTAMP);
            String eventId = exchange.getRequestHeaders().getFirst(TraceHeaders.WEBHOOK_EVENT_ID);

            int attemptForEvent = perEventAttempts
                    .computeIfAbsent(eventId != null ? eventId : "unknown", k -> new AtomicInteger(0))
                    .incrementAndGet();
            int statusToReturn = merchantResponseStatus.get();
            if (failSeventyPercentMode.get()) {
                // Fail first 3 attempts (75% failure rate) and succeed on 4th attempt
                statusToReturn = (attemptForEvent <= 3) ? 503 : 200;
            }

            if (statusToReturn == 200) {
                receivedWebhooks.add(new ReceivedWebhook(sig, ts, eventId, body));
            }

            byte[] resp = ("{\"received\":" + (statusToReturn == 200) + "}").getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(statusToReturn, resp.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(resp);
            }
        });
        mockMerchantServer.start();
    }

    @AfterAll
    static void stopMockMerchantServer() {
        if (mockMerchantServer != null) {
            mockMerchantServer.stop(0);
        }
    }

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("paycore.webhook.kafka-listener-enabled", () -> "false");
        registry.add("paycore.webhook.allow-local-targets", () -> "true");
    }

    @Autowired
    private WebhookDeliveryService deliveryService;

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private TestRestTemplate restTemplate;

    private UUID merchantId;
    private static final String WEBHOOK_SECRET = "whsec_paycore_test_secret_998877";

    @BeforeEach
    void setUp() {
        receivedWebhooks.clear();
        perEventAttempts.clear();
        merchantResponseStatus.set(200);
        failSeventyPercentMode.set(false);
        deliveryService.setAllowLocalInTest(true);

        jdbcClient.sql("TRUNCATE TABLE webhook_deliveries, dlq_events, processed_events, merchant_webhook_endpoints")
                .update();

        merchantId = UuidV7.generate();
        deliveryService.registerMerchantEndpoint(
                merchantId, "http://127.0.0.1:" + mockMerchantPort + "/webhook", WEBHOOK_SECRET);
    }

    @Test
    @DisplayName("1. HMAC-SHA256 signing & processed_events consumer deduplication")
    void hmacSignatureVerificationAndDeduplication() {
        UUID eventId = UuidV7.generate();
        UUID paymentId = UuidV7.generate();
        byte[] rawEvent = buildCapturedEvent(eventId, paymentId, merchantId, 2L, 15_000L, 450L);

        // Consume identical event 5 times (simulating at-least-once Redpanda redelivery)
        DeliveryOutcome first = deliveryService.consumeRawEvent(rawEvent);
        assertThat(first.status()).isEqualTo(DeliveryStatus.DELIVERED);

        for (int i = 0; i < 4; i++) {
            DeliveryOutcome dup = deliveryService.consumeRawEvent(rawEvent);
            assertThat(dup.status()).isEqualTo(DeliveryStatus.DUPLICATE_IGNORED);
        }

        List<WebhookDeliveryRow> rows = deliveryService.listDeliveries(merchantId);
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).status()).isEqualTo("DELIVERED");
        assertThat(receivedWebhooks).hasSize(1);

        ReceivedWebhook hook = receivedWebhooks.get(0);
        assertThat(hook.eventIdHeader()).isEqualTo(eventId.toString());
        assertThat(WebhookSigner.verify(WEBHOOK_SECRET, hook.timestampHeader(), hook.body(), hook.signatureHeader()))
                .isTrue();

        // Tampered payload must fail HMAC verification
        assertThat(WebhookSigner.verify(
                        WEBHOOK_SECRET, hook.timestampHeader(), hook.body() + "tamper", hook.signatureHeader()))
                .isFalse();
    }

    @Test
    @DisplayName("2. 70%-failing merchant endpoint recovers via multi-tier retry topics (retry-1m -> 5m -> 30m -> 2h -> 6h)")
    void seventyPercentFailingEndpointRecoversViaRetries() {
        failSeventyPercentMode.set(true);

        // Enqueue 10 distinct payment events
        for (int i = 1; i <= 10; i++) {
            byte[] rawEvent = buildCapturedEvent(
                    UuidV7.generate(), UuidV7.generate(), merchantId, 2L, 1000L * i, 29L * i);
            deliveryService.consumeRawEvent(rawEvent);
        }

        // Advance virtual retry clock across retry tiers until all 10 succeed
        Instant futureAsOf = Instant.now();
        for (int round = 1; round <= 5; round++) {
            futureAsOf = futureAsOf.plus(Duration.ofHours(7));
            deliveryService.processDueRetries(futureAsOf);
        }

        assertThat(receivedWebhooks).hasSize(10);
        assertThat(deliveryService.getConsumerLagStats().processedEventsTotal()).isEqualTo(10L);
        assertThat(deliveryService.getConsumerLagStats().pendingRetries()).isEqualTo(0L);
    }

    @Test
    @DisplayName("3. Poison message & Exhausted Retries -> DLQ -> Manual Admin Replay succeeds")
    void poisonMessageAndExhaustedRetriesRouteToDlqAndReplayCleanly() {
        // Part A: Malformed poison message goes straight to DLQ without blocking partition
        DeliveryOutcome poisonOutcome = deliveryService.consumeRawEvent(new byte[] {0x01, 0x02, (byte) 0xFF});
        assertThat(poisonOutcome.status()).isEqualTo(DeliveryStatus.DEAD_LETTERED);
        assertThat(poisonOutcome.dlqId()).isNotNull();

        // Part B: Valid event against dead merchant endpoint exhausts all 5 retry tiers -> DEAD_LETTERED on attempt 6
        merchantResponseStatus.set(500);
        UUID eventId = UuidV7.generate();
        UUID paymentId = UuidV7.generate();
        DeliveryOutcome initial = deliveryService.consumeRawEvent(
                buildCapturedEvent(eventId, paymentId, merchantId, 2L, 50_000L, 1_450L));
        assertThat(initial.status()).isEqualTo(DeliveryStatus.RETRY_SCHEDULED);
        assertThat(initial.attempt()).isEqualTo(1);

        Instant future = Instant.now();
        for (int tier = 1; tier <= WebhookDeliveryService.RETRY_TOPICS.size(); tier++) {
            future = future.plus(Duration.ofHours(8));
            int processed = deliveryService.processDueRetries(future);
            assertThat(processed).isEqualTo(1);
        }

        List<DlqEventRow> dlqEvents = deliveryService.listDlqEvents();
        assertThat(dlqEvents).hasSize(2); // 1 poison + 1 exhausted retries

        // Merchant fixes their webhook server (returns 200 OK now)
        merchantResponseStatus.set(200);

        // Operator triggers POST /v1/admin/webhooks/{id}/replay on the latest delivery row
        UUID latestDeliveryId = deliveryService.listDeliveries(merchantId).get(0).id();
        ResponseEntity<DeliveryOutcome> replayResp = restTemplate.postForEntity(
                "/v1/admin/webhooks/" + latestDeliveryId + "/replay", null, DeliveryOutcome.class);
        assertThat(replayResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(replayResp.getBody()).isNotNull();
        assertThat(replayResp.getBody().status()).isEqualTo(DeliveryStatus.DELIVERED);
        assertThat(receivedWebhooks).hasSize(1);
    }

    @Test
    @DisplayName("4. SSRF protection blocks delivery to loopback/private IP when test override is disabled")
    void ssrfProtectionBlocksPrivateIpAtDeliveryTime() {
        deliveryService.setAllowLocalInTest(false); // Enable strict production SSRF enforcement at delivery time

        UUID eventId = UuidV7.generate();
        UUID paymentId = UuidV7.generate();
        DeliveryOutcome outcome = deliveryService.consumeRawEvent(
                buildCapturedEvent(eventId, paymentId, merchantId, 2L, 9_000L, 261L));

        assertThat(outcome.status()).isEqualTo(DeliveryStatus.RETRY_SCHEDULED);
        WebhookDeliveryRow row = deliveryService.listDeliveries(merchantId).get(0);
        assertThat(row.attempt()).isEqualTo(1);
        assertThat(row.lastError()).contains("private, loopback, or link-local");
        assertThat(receivedWebhooks).isEmpty();
    }

    private byte[] buildCapturedEvent(
            UUID eventId, UUID paymentId, UUID merchantId, long seq, long capturedAmt, long feeAmt) {
        Instant now = Instant.now();
        return PaymentEvent.newBuilder()
                .setEventId(eventId.toString())
                .setPaymentId(paymentId.toString())
                .setMerchantId(merchantId.toString())
                .setEventSeq(seq)
                .setEventType("payment.captured")
                .setOccurredAt(Timestamp.newBuilder()
                        .setSeconds(now.getEpochSecond())
                        .setNanos(now.getNano())
                        .build())
                .setCaptured(PaymentCaptured.newBuilder()
                        .setCapturedAmount(capturedAmt)
                        .setFeeAmount(feeAmt)
                        .setCurrency("INR")
                        .setLedgerTxnId(UuidV7.generate().toString())
                        .build())
                .build()
                .toByteArray();
    }
}
