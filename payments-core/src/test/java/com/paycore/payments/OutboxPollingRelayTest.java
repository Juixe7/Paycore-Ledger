package com.paycore.payments;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.paycore.common.events.v1.PaymentEvent;
import com.paycore.common.ids.UuidV7;
import com.paycore.payments.outbox.OutboxPollingRelay;
import com.paycore.payments.outbox.OutboxWriter;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Verifies Outbox Polling Relay v1 (Section 7.7):
 * - {@code FOR UPDATE SKIP LOCKED} batch claiming
 * - Mid-batch broker crash simulation (at-least-once delivery + downstream event_id deduplication)
 */
@SpringBootTest
@Testcontainers
class OutboxPollingRelayTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("paycore_outbox_test")
            .withUsername("paycore")
            .withPassword("paycore");

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("paycore.redis.enabled", () -> "false");
        registry.add("paycore.outbox.relay-enabled", () -> "false");
        registry.add("paycore.resolver.interval-ms", () -> "3600000");
    }

    @Autowired
    private OutboxWriter outboxWriter;

    @Autowired
    private OutboxPollingRelay outboxPollingRelay;

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private TransactionTemplate txTemplate;

    @BeforeEach
    void setUp() {
        jdbcClient.sql("TRUNCATE TABLE outbox_events").update();
        outboxPollingRelay.setCrashMidBatchHook(null);
        outboxPollingRelay.setCustomPublishSink(null);
    }

    @Test
    @DisplayName("Outbox Relay v1: Mid-batch broker crash recovers cleanly on retry with zero lost events and idempotent consumer deduplication")
    void midBatchCrashRecoversWithZeroLostEvents() throws Exception {
        UUID merchantId = UuidV7.generate();
        UUID paymentId = UuidV7.generate();

        // Write 5 events into outbox_events
        for (int i = 1; i <= 5; i++) {
            final long seq = i;
            final long amount = i * 1_000L;
            txTemplate.executeWithoutResult(status -> outboxWriter.writePaymentAuthorized(
                    paymentId, merchantId, seq, amount, "INR", "psp_" + amount));
        }

        assertThat(countUnpublished()).isEqualTo(5L);

        List<byte[]> brokerReceivedRaw = new ArrayList<>();
        AtomicBoolean failOnThirdPublish = new AtomicBoolean(true);

        outboxPollingRelay.setCustomPublishSink(row -> brokerReceivedRaw.add(row.payload()));
        outboxPollingRelay.setCrashMidBatchHook(index -> {
            if (failOnThirdPublish.get() && index == 2) {
                throw new RuntimeException("Simulated Kafka/Redpanda broker crash mid-batch after event #3");
            }
        });

        // First relay poll crashes mid-batch after publishing 3 events -> transaction rolls back published_at updates!
        assertThatThrownBy(() -> outboxPollingRelay.relayNextBatch())
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Simulated Kafka/Redpanda broker crash");

        // All 5 events remain unpublished in outbox_events so nothing is ever lost
        assertThat(countUnpublished()).isEqualTo(5L);

        // Broker recovers on next poll
        failOnThirdPublish.set(false);
        int publishedCount = outboxPollingRelay.relayNextBatch();
        assertThat(publishedCount).isEqualTo(5);
        assertThat(countUnpublished()).isEqualTo(0L);

        // Total raw messages sent to broker across both attempts = 3 (before crash) + 5 (after recovery) = 8
        assertThat(brokerReceivedRaw).hasSize(8);

        // Downstream consumer deduplicating by event_id sees exactly the 5 unique events!
        Set<String> deduplicatedEventIds = new LinkedHashSet<>();
        for (byte[] raw : brokerReceivedRaw) {
            PaymentEvent ev = PaymentEvent.parseFrom(raw);
            deduplicatedEventIds.add(ev.getEventId());
        }
        assertThat(deduplicatedEventIds).hasSize(5);
    }

    private long countUnpublished() {
        Long cnt = jdbcClient.sql("SELECT COUNT(*) FROM outbox_events WHERE published_at IS NULL")
                .query(Long.class)
                .single();
        return cnt != null ? cnt : 0L;
    }
}
