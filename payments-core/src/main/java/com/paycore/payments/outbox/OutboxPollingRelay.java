package com.paycore.payments.outbox;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.paycore.common.clock.ClockProvider;
import com.paycore.common.tracing.TraceHeaders;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Outbox Relay v1: Polling with {@code FOR UPDATE SKIP LOCKED} (Section 4.2 #5, 5.1, 6.2, 7.7).
 *
 * <p>Claims unpublished rows from {@code outbox_events} using {@code FOR UPDATE SKIP LOCKED}
 * so multiple {@code payments-core} replicas never contend on the same batch, publishes
 * Protobuf {@code PaymentEvent} bytes to {@code payments.events} keyed by {@code payment_id},
 * propagates W3C {@code traceparent} in Kafka headers, and stamps {@code published_at}.
 */
@Component
public class OutboxPollingRelay {

    public static final String PAYMENTS_EVENTS_TOPIC = "payments.events";
    private static final Logger log = LoggerFactory.getLogger(OutboxPollingRelay.class);
    private static final TypeReference<Map<String, String>> MAP_TYPE = new TypeReference<>() {};

    private final JdbcClient jdbcClient;
    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final ClockProvider clockProvider;
    private final boolean relayEnabled;
    private final int batchSize;

    private volatile Consumer<Integer> crashMidBatchHook = index -> {};
    private volatile Consumer<OutboxRow> customPublishSink;

    public OutboxPollingRelay(
            JdbcClient jdbcClient,
            @Autowired(required = false) KafkaTemplate<String, byte[]> kafkaTemplate,
            ObjectMapper objectMapper,
            ClockProvider clockProvider,
            @Value("${paycore.outbox.relay-enabled:false}") boolean relayEnabled,
            @Value("${paycore.outbox.batch-size:100}") int batchSize) {
        this.jdbcClient = jdbcClient;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.clockProvider = clockProvider;
        this.relayEnabled = relayEnabled;
        this.batchSize = batchSize;
    }

    @Scheduled(fixedDelayString = "${paycore.outbox.poll-interval-ms:250}")
    public void scheduledRelayTick() {
        if (!relayEnabled) {
            return;
        }
        try {
            relayNextBatch();
        } catch (Exception e) {
            log.warn("Outbox polling relay tick encountered error (will retry next tick): {}", e.getMessage());
        }
    }

    /**
     * Claims up to {@code batchSize} unpublished outbox rows with {@code FOR UPDATE SKIP LOCKED},
     * publishes each to Redpanda/Kafka keyed by {@code aggregate_id} ({@code payment_id}),
     * and marks {@code published_at}.
     */
    @Transactional(isolation = Isolation.READ_COMMITTED)
    public int relayNextBatch() {
        List<OutboxRow> rows = jdbcClient.sql("""
                SELECT id, aggregate_id, event_seq, event_type, schema_version,
                       payload, trace_context::text AS trace_json, created_at
                FROM outbox_events
                WHERE published_at IS NULL
                ORDER BY created_at ASC, id ASC
                LIMIT :batchSize
                FOR UPDATE SKIP LOCKED
                """)
                .param("batchSize", batchSize)
                .query((rs, rowNum) -> new OutboxRow(
                        rs.getObject("id", UUID.class),
                        rs.getObject("aggregate_id", UUID.class),
                        rs.getLong("event_seq"),
                        rs.getString("event_type"),
                        rs.getInt("schema_version"),
                        rs.getBytes("payload"),
                        parseTraceContext(rs.getString("trace_json")),
                        rs.getTimestamp("created_at").toInstant()))
                .list();

        if (rows.isEmpty()) {
            return 0;
        }

        int publishedCount = 0;
        Timestamp nowTs = Timestamp.from(clockProvider.now());

        for (int i = 0; i < rows.size(); i++) {
            OutboxRow row = rows.get(i);
            publishToBus(row);
            crashMidBatchHook.accept(i);

            jdbcClient.sql("""
                    UPDATE outbox_events
                    SET published_at = :publishedAt
                    WHERE id = :id AND created_at = :createdAt
                    """)
                    .param("publishedAt", nowTs)
                    .param("id", row.id())
                    .param("createdAt", Timestamp.from(row.createdAt()))
                    .update();

            publishedCount++;
        }

        return publishedCount;
    }

    private void publishToBus(OutboxRow row) {
        Consumer<OutboxRow> sink = this.customPublishSink;
        if (sink != null) {
            sink.accept(row);
            return;
        }

        if (kafkaTemplate == null) {
            throw new IllegalStateException("KafkaTemplate is not configured and no customPublishSink is set");
        }

        ProducerRecord<String, byte[]> record = new ProducerRecord<>(
                PAYMENTS_EVENTS_TOPIC,
                null,
                row.aggregateId().toString(), // Keyed by payment_id so per-payment events stay strictly ordered
                row.payload());

        record.headers().add(new RecordHeader("event_id", row.id().toString().getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader("event_type", row.eventType().getBytes(StandardCharsets.UTF_8)));
        String traceparent = row.traceContext().get(TraceHeaders.TRACEPARENT);
        if (traceparent != null) {
            record.headers().add(
                    new RecordHeader(TraceHeaders.TRACEPARENT, traceparent.getBytes(StandardCharsets.UTF_8)));
        }

        try {
            kafkaTemplate.send(record).get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Failed to publish outbox event " + row.id() + " to Kafka", e);
        }
    }

    private Map<String, String> parseTraceContext(String json) {
        if (json == null || json.isBlank()) {
            return Collections.emptyMap();
        }
        try {
            return objectMapper.readValue(json, MAP_TYPE);
        } catch (Exception e) {
            return Collections.emptyMap();
        }
    }

    public void setCrashMidBatchHook(Consumer<Integer> crashMidBatchHook) {
        this.crashMidBatchHook = crashMidBatchHook != null ? crashMidBatchHook : idx -> {};
    }

    public void setCustomPublishSink(Consumer<OutboxRow> customPublishSink) {
        this.customPublishSink = customPublishSink;
    }

    public record OutboxRow(
            UUID id,
            UUID aggregateId,
            long eventSeq,
            String eventType,
            int schemaVersion,
            byte[] payload,
            Map<String, String> traceContext,
            Instant createdAt) {}
}
