package com.paycore.payments.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Custom Prometheus/Micrometer metrics & SLO instrumentation (Section 8).
 *
 * <ul>
 *   <li>{@code paycore_outbox_unpublished_rows} (gauge)</li>
 *   <li>{@code paycore_outbox_oldest_unpublished_age_seconds} (gauge — alert if > 30s)</li>
 *   <li>{@code paycore_idempotency_decisions_total{result,source}} (counter)</li>
 *   <li>{@code paycore_payment_transitions_total{from,to}} (counter)</li>
 *   <li>{@code paycore_psp_call_duration_seconds{operation,outcome}} (timer)</li>
 * </ul>
 */
@Component
public class PayCoreMetrics {

    private final MeterRegistry registry;
    private final JdbcClient jdbcClient;

    private final AtomicLong outboxUnpublishedRows = new AtomicLong(0L);
    private final AtomicLong outboxOldestUnpublishedAgeSeconds = new AtomicLong(0L);

    public PayCoreMetrics(MeterRegistry registry, JdbcClient jdbcClient) {
        this.registry = registry;
        this.jdbcClient = jdbcClient;

        Gauge.builder("paycore_outbox_unpublished_rows", outboxUnpublishedRows, AtomicLong::get)
                .description("Number of unpublished rows currently in outbox_events")
                .register(registry);

        Gauge.builder(
                        "paycore_outbox_oldest_unpublished_age_seconds",
                        outboxOldestUnpublishedAgeSeconds,
                        AtomicLong::get)
                .description("Age in seconds of the oldest unpublished row in outbox_events")
                .register(registry);
    }

    @Scheduled(fixedDelayString = "${paycore.metrics.refresh-interval-ms:5000}")
    public void refreshOutboxGauges() {
        try {
            Long unpublished = jdbcClient.sql("SELECT COUNT(*) FROM outbox_events WHERE published_at IS NULL")
                    .query(Long.class)
                    .single();
            outboxUnpublishedRows.set(unpublished != null ? unpublished : 0L);

            Long oldestAgeSec = jdbcClient.sql("""
                    SELECT COALESCE(EXTRACT(EPOCH FROM (now() - MIN(created_at)))::bigint, 0)
                    FROM outbox_events
                    WHERE published_at IS NULL
                    """)
                    .query(Long.class)
                    .single();
            outboxOldestUnpublishedAgeSeconds.set(oldestAgeSec != null ? Math.max(0L, oldestAgeSec) : 0L);
        } catch (Exception ignored) {
            // Ignore transient DB startup/shutdown errors during background gauge refresh
        }
    }

    public void recordIdempotencyDecision(String result, String source) {
        Counter.builder("paycore_idempotency_decisions_total")
                .tag("result", result)
                .tag("source", source)
                .register(registry)
                .increment();
    }

    public void recordPaymentTransition(String fromStatus, String toStatus) {
        Counter.builder("paycore_payment_transitions_total")
                .tag("from", fromStatus)
                .tag("to", toStatus)
                .register(registry)
                .increment();
    }

    public void recordPspCall(String operation, String outcome, Duration duration) {
        Timer.builder("paycore_psp_call_duration_seconds")
                .tag("operation", operation)
                .tag("outcome", outcome)
                .register(registry)
                .record(duration);
    }
}
