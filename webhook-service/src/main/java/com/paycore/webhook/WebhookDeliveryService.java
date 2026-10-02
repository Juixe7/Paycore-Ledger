package com.paycore.webhook;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import com.paycore.common.clock.ClockProvider;
import com.paycore.common.errors.PayCoreException;
import com.paycore.common.events.v1.PaymentEvent;
import com.paycore.common.ids.UuidV7;
import com.paycore.common.tracing.TraceHeaders;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;

/**
 * Webhook Delivery Engine with HMAC-SHA256 signing, SSRF protection, idempotent {@code processed_events}
 * deduplication, delay-aware retry topics (1m, 5m, 30m, 2h, 6h) with jitter, and DLQ + replay (Section 5.4 & 7.8).
 */
@Service
public class WebhookDeliveryService {

    public static final String CONSUMER_NAME = "webhook-service";
    public static final List<String> RETRY_TOPICS = List.of(
            "payments.events.retry-1m",
            "payments.events.retry-5m",
            "payments.events.retry-30m",
            "payments.events.retry-2h",
            "payments.events.retry-6h");
    public static final List<Duration> RETRY_DELAYS = List.of(
            Duration.ofMinutes(1),
            Duration.ofMinutes(5),
            Duration.ofMinutes(30),
            Duration.ofHours(2),
            Duration.ofHours(6));
    public static final String DLQ_TOPIC = "payments.events.dlq";

    private static final Logger log = LoggerFactory.getLogger(WebhookDeliveryService.class);

    private final JdbcClient jdbcClient;
    private final ObjectMapper objectMapper;
    private final ClockProvider clockProvider;
    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final RestTemplate restTemplate;
    private final boolean kafkaEnabled;
    private volatile boolean allowLocalInTest;

    public WebhookDeliveryService(
            JdbcClient jdbcClient,
            ObjectMapper objectMapper,
            ClockProvider clockProvider,
            @Autowired(required = false) KafkaTemplate<String, byte[]> kafkaTemplate,
            @Value("${paycore.webhook.kafka-listener-enabled:false}") boolean kafkaEnabled,
            @Value("${paycore.webhook.allow-local-targets:false}") boolean allowLocalInTest,
            @Value("${paycore.webhook.http-timeout-ms:2000}") int timeoutMs) {
        this.jdbcClient = jdbcClient;
        this.objectMapper = objectMapper;
        this.clockProvider = clockProvider;
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaEnabled = kafkaEnabled;
        this.allowLocalInTest = allowLocalInTest;

        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(timeoutMs);
        factory.setReadTimeout(timeoutMs);
        this.restTemplate = new RestTemplate(factory);
    }

    @Transactional
    public void registerMerchantEndpoint(UUID merchantId, String webhookUrl, String webhookSecret) {
        WebhookSsrfValidator.validateAtDeliveryTime(webhookUrl, allowLocalInTest);
        jdbcClient.sql("""
                INSERT INTO merchant_webhook_endpoints (merchant_id, webhook_url, webhook_secret, updated_at)
                VALUES (:merchantId, :webhookUrl, :webhookSecret, :updatedAt)
                ON CONFLICT (merchant_id) DO UPDATE
                SET webhook_url = EXCLUDED.webhook_url,
                    webhook_secret = EXCLUDED.webhook_secret,
                    updated_at = EXCLUDED.updated_at
                """)
                .param("merchantId", merchantId)
                .param("webhookUrl", webhookUrl)
                .param("webhookSecret", webhookSecret)
                .param("updatedAt", Timestamp.from(clockProvider.now()))
                .update();
    }

    @KafkaListener(
            topics = "payments.events",
            groupId = "webhook-service-group",
            autoStartup = "${paycore.webhook.kafka-listener-enabled:false}")
    public void onKafkaPaymentEvent(byte[] rawPayload) {
        consumeRawEvent(rawPayload);
    }

    /**
     * Processes a raw Protobuf message from the event bus.
     * Poison messages are routed directly to the DLQ so they never block a partition (Section 6.2 #4 & 7.8).
     */
    @Transactional
    public DeliveryOutcome consumeRawEvent(byte[] rawPayload) {
        PaymentEvent event;
        try {
            event = PaymentEvent.parseFrom(rawPayload);
            if (event.getEventId().isBlank() || event.getMerchantId().isBlank()) {
                throw new InvalidProtocolBufferException("Missing required event_id or merchant_id in PaymentEvent");
            }
            UUID.fromString(event.getEventId());
            UUID.fromString(event.getMerchantId());
        } catch (Exception poisonEx) {
            UUID dlqId = recordDlqEvent(null, null, null, rawPayload, "POISON_MESSAGE: " + poisonEx.getMessage());
            log.warn("Routed poison message to DLQ (dlqId={}): {}", dlqId, poisonEx.getMessage());
            return new DeliveryOutcome(DeliveryStatus.DEAD_LETTERED, null, dlqId, 0);
        }

        UUID eventId = UUID.fromString(event.getEventId());
        UUID merchantId = UUID.fromString(event.getMerchantId());

        // Acquire per-event transaction-scoped advisory lock to eliminate TOCTOU race across concurrent consumers
        jdbcClient.sql("SELECT pg_advisory_xact_lock(hashtext(:lockKey))")
                .param("lockKey", CONSUMER_NAME + ":" + eventId)
                .query((rs, rowNum) -> 1)
                .optional();

        // Atomic dedupe check in processed_events & in-flight deliveries (Section 5.4 & 6.2 #3)
        int existingAttempts = countAttemptsForEvent(eventId);
        if (isAlreadyProcessed(eventId) || existingAttempts > 0) {
            return new DeliveryOutcome(DeliveryStatus.DUPLICATE_IGNORED, null, null, 0);
        }

        Optional<EndpointConfig> endpointOpt = findEndpoint(merchantId);
        if (endpointOpt.isEmpty()) {
            markProcessed(eventId);
            return new DeliveryOutcome(DeliveryStatus.DELIVERED, null, null, 1);
        }

        EndpointConfig endpoint = endpointOpt.get();
        String payloadJson = buildWebhookJsonPayload(event);
        String traceparent = event.getTraceContextMap().getOrDefault(
                TraceHeaders.TRACEPARENT, TraceHeaders.currentTraceparentOrGenerate());

        int currentAttempt = existingAttempts + 1;

        return attemptDelivery(
                eventId,
                event.getPaymentId(),
                merchantId,
                currentAttempt,
                endpoint.webhookUrl(),
                endpoint.webhookSecret(),
                payloadJson,
                traceparent,
                rawPayload);
    }

    /**
     * Executes all due scheduled retries from {@code webhook_deliveries} (delay-aware retry consumer).
     */
    @Transactional
    public int processDueRetries(Instant asOf) {
        List<WebhookDeliveryRow> dueRows = jdbcClient.sql("""
                SELECT id, event_id, merchant_id, attempt, status, retry_topic, next_attempt_at,
                       last_status_code, last_error, target_url, payload_json, traceparent, created_at
                FROM webhook_deliveries
                WHERE status = 'RETRY_SCHEDULED'
                  AND next_attempt_at <= :asOf
                ORDER BY next_attempt_at ASC, id ASC
                FOR UPDATE SKIP LOCKED
                """)
                .param("asOf", Timestamp.from(asOf))
                .query(this::mapDeliveryRow)
                .list();

        int processed = 0;
        for (WebhookDeliveryRow row : dueRows) {
            if (isAlreadyProcessed(row.eventId())) {
                continue;
            }
            // Mark previous scheduled row as superseded so only the latest attempt is active
            jdbcClient.sql("UPDATE webhook_deliveries SET status = 'FAILED' WHERE id = :id")
                    .param("id", row.id())
                    .update();

            Optional<EndpointConfig> endpointOpt = findEndpoint(row.merchantId());
            if (endpointOpt.isEmpty()) {
                continue;
            }

            attemptDelivery(
                    row.eventId(),
                    "",
                    row.merchantId(),
                    row.attempt() + 1,
                    row.targetUrl(),
                    endpointOpt.get().webhookSecret(),
                    row.payloadJson(),
                    row.traceparent(),
                    row.payloadJson().getBytes(java.nio.charset.StandardCharsets.UTF_8));
            processed++;
        }
        return processed;
    }

    @Transactional
    public DeliveryOutcome replayDelivery(UUID deliveryId) {
        WebhookDeliveryRow row = jdbcClient.sql("""
                SELECT id, event_id, merchant_id, attempt, status, retry_topic, next_attempt_at,
                       last_status_code, last_error, target_url, payload_json, traceparent, created_at
                FROM webhook_deliveries
                WHERE id = :id
                """)
                .param("id", deliveryId)
                .query(this::mapDeliveryRow)
                .optional()
                .orElseThrow(() -> PayCoreException.notFound("WebhookDelivery", deliveryId));

        EndpointConfig endpoint = findEndpoint(row.merchantId())
                .orElseThrow(() -> PayCoreException.notFound("MerchantWebhookEndpoint", row.merchantId()));

        int nextAttempt = countAttemptsForEvent(row.eventId()) + 1;
        return attemptDelivery(
                row.eventId(),
                "",
                row.merchantId(),
                nextAttempt,
                row.targetUrl(),
                endpoint.webhookSecret(),
                row.payloadJson(),
                row.traceparent(),
                row.payloadJson().getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    @Transactional
    public DlqEventRow replayDlqEvent(UUID dlqId) {
        DlqEventRow dlq = jdbcClient.sql("""
                SELECT id, event_id, payment_id, merchant_id, raw_payload, reason, replayed, created_at
                FROM dlq_events
                WHERE id = :id
                FOR UPDATE
                """)
                .param("id", dlqId)
                .query(this::mapDlqRow)
                .optional()
                .orElseThrow(() -> PayCoreException.notFound("DlqEvent", dlqId));

        jdbcClient.sql("UPDATE dlq_events SET replayed = TRUE WHERE id = :id")
                .param("id", dlqId)
                .update();

        // If it's a valid PaymentEvent that exhausted retries, re-attempt delivery
        try {
            PaymentEvent event = PaymentEvent.parseFrom(dlq.rawPayload());
            if (!event.getEventId().isBlank() && !event.getMerchantId().isBlank()) {
                UUID eventId = UUID.fromString(event.getEventId());
                UUID merchantId = UUID.fromString(event.getMerchantId());
                Optional<EndpointConfig> ep = findEndpoint(merchantId);
                if (ep.isPresent()) {
                    String payloadJson = buildWebhookJsonPayload(event);
                    String traceparent = event.getTraceContextMap().getOrDefault(
                            TraceHeaders.TRACEPARENT, TraceHeaders.currentTraceparentOrGenerate());
                    attemptDelivery(
                            eventId,
                            event.getPaymentId(),
                            merchantId,
                            1, // Reset retry tier on explicit admin DLQ replay
                            ep.get().webhookUrl(),
                            ep.get().webhookSecret(),
                            payloadJson,
                            traceparent,
                            dlq.rawPayload());
                }
            }
        } catch (Exception ignored) {
            // Poison payload marked replayed/inspected
        }

        return jdbcClient.sql("""
                SELECT id, event_id, payment_id, merchant_id, raw_payload, reason, replayed, created_at
                FROM dlq_events WHERE id = :id
                """)
                .param("id", dlqId)
                .query(this::mapDlqRow)
                .single();
    }

    private DeliveryOutcome attemptDelivery(
            UUID eventId,
            String paymentId,
            UUID merchantId,
            int attempt,
            String targetUrl,
            String webhookSecret,
            String payloadJson,
            String traceparent,
            byte[] rawBytesForDlq) {

        Instant now = clockProvider.now();
        UUID deliveryId = UuidV7.generate(now.toEpochMilli());
        String timestampSec = String.valueOf(now.getEpochSecond());
        String signature = WebhookSigner.sign(webhookSecret, timestampSec, payloadJson);

        Integer statusCode = null;
        String errorMessage = null;
        boolean delivered = false;

        try {
            WebhookSsrfValidator.validateAtDeliveryTime(targetUrl, allowLocalInTest);

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.set(TraceHeaders.WEBHOOK_SIGNATURE, signature);
            headers.set(TraceHeaders.WEBHOOK_TIMESTAMP, timestampSec);
            headers.set(TraceHeaders.WEBHOOK_EVENT_ID, eventId.toString());
            if (traceparent != null && !traceparent.isBlank()) {
                headers.set(TraceHeaders.TRACEPARENT, traceparent);
            }

            ResponseEntity<String> resp = restTemplate.exchange(
                    targetUrl, HttpMethod.POST, new HttpEntity<>(payloadJson, headers), String.class);
            statusCode = resp.getStatusCode().value();
            if (resp.getStatusCode().is2xxSuccessful()) {
                delivered = true;
            } else {
                errorMessage = "HTTP " + statusCode;
            }
        } catch (HttpStatusCodeException httpEx) {
            statusCode = httpEx.getStatusCode().value();
            errorMessage = "HTTP " + statusCode + ": " + httpEx.getResponseBodyAsString();
        } catch (Exception ex) {
            errorMessage = ex.getMessage();
        }

        if (delivered) {
            insertDeliveryRow(
                    deliveryId,
                    eventId,
                    merchantId,
                    attempt,
                    "DELIVERED",
                    null,
                    null,
                    statusCode,
                    null,
                    targetUrl,
                    payloadJson,
                    traceparent,
                    now);
            markProcessed(eventId);
            return new DeliveryOutcome(DeliveryStatus.DELIVERED, deliveryId, null, attempt);
        }

        // Failed attempt: route to retry topic (attempts 1..5 -> retry-1m, 5m, 30m, 2h, 6h) or DLQ on attempt > 5
        if (attempt <= RETRY_TOPICS.size()) {
            String retryTopic = RETRY_TOPICS.get(attempt - 1);
            Duration baseDelay = RETRY_DELAYS.get(attempt - 1);
            long jitterMs = ThreadLocalRandom.current().nextLong(0, 1000);
            Instant nextAttemptAt = now.plus(baseDelay).plusMillis(jitterMs);

            insertDeliveryRow(
                    deliveryId,
                    eventId,
                    merchantId,
                    attempt,
                    "RETRY_SCHEDULED",
                    retryTopic,
                    nextAttemptAt,
                    statusCode,
                    errorMessage,
                    targetUrl,
                    payloadJson,
                    traceparent,
                    now);

            publishToKafkaTopicSafely(retryTopic, paymentId, rawBytesForDlq);
            return new DeliveryOutcome(DeliveryStatus.RETRY_SCHEDULED, deliveryId, null, attempt);
        } else {
            insertDeliveryRow(
                    deliveryId,
                    eventId,
                    merchantId,
                    attempt,
                    "DEAD_LETTERED",
                    DLQ_TOPIC,
                    null,
                    statusCode,
                    errorMessage,
                    targetUrl,
                    payloadJson,
                    traceparent,
                    now);
            UUID dlqId = recordDlqEvent(
                    eventId.toString(),
                    paymentId,
                    merchantId.toString(),
                    rawBytesForDlq,
                    "MAX_RETRIES_EXCEEDED: " + errorMessage);
            publishToKafkaTopicSafely(DLQ_TOPIC, paymentId, rawBytesForDlq);
            return new DeliveryOutcome(DeliveryStatus.DEAD_LETTERED, deliveryId, dlqId, attempt);
        }
    }

    private void insertDeliveryRow(
            UUID id,
            UUID eventId,
            UUID merchantId,
            int attempt,
            String status,
            String retryTopic,
            Instant nextAttemptAt,
            Integer lastStatusCode,
            String lastError,
            String targetUrl,
            String payloadJson,
            String traceparent,
            Instant createdAt) {
        jdbcClient.sql("""
                INSERT INTO webhook_deliveries (
                    id, event_id, merchant_id, attempt, status, retry_topic, next_attempt_at,
                    last_status_code, last_error, target_url, payload_json, traceparent, created_at
                ) VALUES (
                    :id, :eventId, :merchantId, :attempt, :status, :retryTopic, :nextAttemptAt,
                    :lastStatusCode, :lastError, :targetUrl, :payloadJson, :traceparent, :createdAt
                )
                """)
                .param("id", id)
                .param("eventId", eventId)
                .param("merchantId", merchantId)
                .param("attempt", attempt)
                .param("status", status)
                .param("retryTopic", retryTopic)
                .param("nextAttemptAt", nextAttemptAt != null ? Timestamp.from(nextAttemptAt) : null)
                .param("lastStatusCode", lastStatusCode)
                .param("lastError", lastError)
                .param("targetUrl", targetUrl)
                .param("payloadJson", payloadJson)
                .param("traceparent", traceparent)
                .param("createdAt", Timestamp.from(createdAt))
                .update();
    }

    private UUID recordDlqEvent(
            String eventId, String paymentId, String merchantId, byte[] rawPayload, String reason) {
        UUID dlqId = UuidV7.generate(clockProvider.now().toEpochMilli());
        jdbcClient.sql("""
                INSERT INTO dlq_events (id, event_id, payment_id, merchant_id, raw_payload, reason, replayed, created_at)
                VALUES (:id, :eventId, :paymentId, :merchantId, :rawPayload, :reason, FALSE, :createdAt)
                """)
                .param("id", dlqId)
                .param("eventId", eventId)
                .param("paymentId", paymentId)
                .param("merchantId", merchantId)
                .param("rawPayload", rawPayload)
                .param("reason", reason)
                .param("createdAt", Timestamp.from(clockProvider.now()))
                .update();
        return dlqId;
    }

    private void publishToKafkaTopicSafely(String topic, String key, byte[] payload) {
        if (!kafkaEnabled || kafkaTemplate == null) {
            return;
        }
        try {
            kafkaTemplate.send(topic, key != null ? key : "", payload);
        } catch (Exception e) {
            log.debug("Kafka publish to {} skipped: {}", topic, e.getMessage());
        }
    }

    private boolean isAlreadyProcessed(UUID eventId) {
        Integer cnt = jdbcClient.sql("""
                SELECT COUNT(*) FROM processed_events
                WHERE consumer = :consumer AND event_id = :eventId
                """)
                .param("consumer", CONSUMER_NAME)
                .param("eventId", eventId)
                .query(Integer.class)
                .single();
        return cnt != null && cnt > 0;
    }

    private void markProcessed(UUID eventId) {
        jdbcClient.sql("""
                INSERT INTO processed_events (consumer, event_id, processed_at)
                VALUES (:consumer, :eventId, :processedAt)
                ON CONFLICT (consumer, event_id) DO NOTHING
                """)
                .param("consumer", CONSUMER_NAME)
                .param("eventId", eventId)
                .param("processedAt", Timestamp.from(clockProvider.now()))
                .update();
    }

    private int countAttemptsForEvent(UUID eventId) {
        Integer cnt = jdbcClient.sql("SELECT COUNT(*) FROM webhook_deliveries WHERE event_id = :eventId")
                .param("eventId", eventId)
                .query(Integer.class)
                .single();
        return cnt != null ? cnt : 0;
    }

    private Optional<EndpointConfig> findEndpoint(UUID merchantId) {
        return jdbcClient.sql("""
                SELECT webhook_url, webhook_secret
                FROM merchant_webhook_endpoints
                WHERE merchant_id = :merchantId
                """)
                .param("merchantId", merchantId)
                .query((rs, rowNum) -> new EndpointConfig(
                        rs.getString("webhook_url"), rs.getString("webhook_secret")))
                .optional();
    }

    private String buildWebhookJsonPayload(PaymentEvent event) {
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("event_id", event.getEventId());
        root.put("event_type", event.getEventType());
        root.put("schema_version", event.getSchemaVersion());
        root.put("payment_id", event.getPaymentId());
        root.put("merchant_id", event.getMerchantId());
        root.put("event_seq", event.getEventSeq());
        root.put("occurred_at", Instant.ofEpochSecond(
                event.getOccurredAt().getSeconds(), event.getOccurredAt().getNanos()).toString());

        Map<String, Object> data = new LinkedHashMap<>();
        switch (event.getPayloadCase()) {
            case AUTHORIZED -> {
                data.put("amount", event.getAuthorized().getAmount());
                data.put("currency", event.getAuthorized().getCurrency());
                data.put("psp_ref", event.getAuthorized().getPspRef());
            }
            case CAPTURED -> {
                data.put("captured_amount", event.getCaptured().getCapturedAmount());
                data.put("fee_amount", event.getCaptured().getFeeAmount());
                data.put("net_merchant_amount", event.getCaptured().getNetMerchantAmount());
                data.put("currency", event.getCaptured().getCurrency());
                data.put("psp_ref", event.getCaptured().getPspRef());
                data.put("ledger_txn_id", event.getCaptured().getLedgerTxnId());
            }
            case REFUNDED -> {
                data.put("refund_id", event.getRefunded().getRefundId());
                data.put("refund_amount", event.getRefunded().getRefundAmount());
                data.put("total_refunded_amount", event.getRefunded().getTotalRefundedAmount());
                data.put("currency", event.getRefunded().getCurrency());
                data.put("ledger_txn_id", event.getRefunded().getLedgerTxnId());
            }
            case PAYOUT -> {
                data.put("payout_id", event.getPayout().getPayoutId());
                data.put("amount", event.getPayout().getAmount());
                data.put("currency", event.getPayout().getCurrency());
                data.put("ledger_txn_id", event.getPayout().getLedgerTxnId());
            }
            case FAILED -> {
                data.put("amount", event.getFailed().getAmount());
                data.put("currency", event.getFailed().getCurrency());
                data.put("psp_ref", event.getFailed().getPspRef());
                data.put("reason", event.getFailed().getReason());
            }
            default -> {}
        }
        root.put("data", data);

        try {
            return objectMapper.writeValueAsString(root);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize webhook JSON", e);
        }
    }

    @Transactional(readOnly = true)
    public List<WebhookDeliveryRow> listDeliveries(UUID merchantId) {
        if (merchantId != null) {
            return jdbcClient.sql("""
                    SELECT id, event_id, merchant_id, attempt, status, retry_topic, next_attempt_at,
                           last_status_code, last_error, target_url, payload_json, traceparent, created_at
                    FROM webhook_deliveries
                    WHERE merchant_id = :merchantId
                    ORDER BY created_at DESC, attempt DESC
                    """)
                    .param("merchantId", merchantId)
                    .query(this::mapDeliveryRow)
                    .list();
        }
        return jdbcClient.sql("""
                SELECT id, event_id, merchant_id, attempt, status, retry_topic, next_attempt_at,
                       last_status_code, last_error, target_url, payload_json, traceparent, created_at
                FROM webhook_deliveries
                ORDER BY created_at DESC, attempt DESC
                LIMIT 200
                """)
                .query(this::mapDeliveryRow)
                .list();
    }

    @Transactional(readOnly = true)
    public List<DlqEventRow> listDlqEvents() {
        return jdbcClient.sql("""
                SELECT id, event_id, payment_id, merchant_id, raw_payload, reason, replayed, created_at
                FROM dlq_events
                ORDER BY created_at DESC
                """)
                .query(this::mapDlqRow)
                .list();
    }

    @Transactional(readOnly = true)
    public ConsumerLagView getConsumerLagStats() {
        long pendingRetries = jdbcClient.sql(
                        "SELECT COUNT(*) FROM webhook_deliveries WHERE status = 'RETRY_SCHEDULED'")
                .query(Long.class)
                .single();
        long dlqDepth = jdbcClient.sql(
                        "SELECT COUNT(*) FROM dlq_events WHERE replayed = FALSE")
                .query(Long.class)
                .single();
        long processedTotal = jdbcClient.sql(
                        "SELECT COUNT(*) FROM processed_events WHERE consumer = :c")
                .param("c", CONSUMER_NAME)
                .query(Long.class)
                .single();
        return new ConsumerLagView(CONSUMER_NAME, pendingRetries, dlqDepth, processedTotal, clockProvider.now());
    }

    public void setAllowLocalInTest(boolean allowLocalInTest) {
        this.allowLocalInTest = allowLocalInTest;
    }

    private WebhookDeliveryRow mapDeliveryRow(ResultSet rs, int rowNum) throws SQLException {
        Timestamp nextTs = rs.getTimestamp("next_attempt_at");
        return new WebhookDeliveryRow(
                rs.getObject("id", UUID.class),
                rs.getObject("event_id", UUID.class),
                rs.getObject("merchant_id", UUID.class),
                rs.getInt("attempt"),
                rs.getString("status"),
                rs.getString("retry_topic"),
                nextTs != null ? nextTs.toInstant() : null,
                rs.getObject("last_status_code", Integer.class),
                rs.getString("last_error"),
                rs.getString("target_url"),
                rs.getString("payload_json"),
                rs.getString("traceparent"),
                rs.getTimestamp("created_at").toInstant());
    }

    private DlqEventRow mapDlqRow(ResultSet rs, int rowNum) throws SQLException {
        return new DlqEventRow(
                rs.getObject("id", UUID.class),
                rs.getString("event_id"),
                rs.getString("payment_id"),
                rs.getString("merchant_id"),
                rs.getBytes("raw_payload"),
                rs.getString("reason"),
                rs.getBoolean("replayed"),
                rs.getTimestamp("created_at").toInstant());
    }

    private record EndpointConfig(String webhookUrl, String webhookSecret) {}

    public enum DeliveryStatus {
        DELIVERED,
        DUPLICATE_IGNORED,
        RETRY_SCHEDULED,
        DEAD_LETTERED
    }

    public record DeliveryOutcome(
            DeliveryStatus status, UUID deliveryId, UUID dlqId, int attempt) {}

    public record WebhookDeliveryRow(
            UUID id,
            UUID eventId,
            UUID merchantId,
            int attempt,
            String status,
            String retryTopic,
            Instant nextAttemptAt,
            Integer lastStatusCode,
            String lastError,
            String targetUrl,
            String payloadJson,
            String traceparent,
            Instant createdAt) {}

    public record DlqEventRow(
            UUID id,
            String eventId,
            String paymentId,
            String merchantId,
            byte[] rawPayload,
            String reason,
            boolean replayed,
            Instant createdAt) {}

    public record ConsumerLagView(
            String consumer,
            long pendingRetries,
            long dlqDepth,
            long processedEventsTotal,
            Instant checkedAt) {}
}
