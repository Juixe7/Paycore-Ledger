package com.paycore.payments.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Timestamp;
import com.paycore.common.clock.ClockProvider;
import com.paycore.common.events.v1.PaymentAuthorized;
import com.paycore.common.events.v1.PaymentCaptured;
import com.paycore.common.events.v1.PaymentEvent;
import com.paycore.common.events.v1.PaymentFailed;
import com.paycore.common.events.v1.PayoutCreated;
import com.paycore.common.events.v1.RefundProcessed;
import com.paycore.common.ids.UuidV7;
import com.paycore.common.tracing.TraceHeaders;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Transactional Outbox Writer (Section 4.2 #5 & Section 5.1).
 *
 * <p>Commits versioned Protobuf {@link PaymentEvent} rows into {@code outbox_events}
 * in the exact same PostgreSQL transaction as the payment state transition and ledger postings.
 */
@Component
public class OutboxWriter {

    private final JdbcClient jdbcClient;
    private final ObjectMapper objectMapper;
    private final ClockProvider clockProvider;

    public OutboxWriter(JdbcClient jdbcClient, ObjectMapper objectMapper, ClockProvider clockProvider) {
        this.jdbcClient = jdbcClient;
        this.objectMapper = objectMapper;
        this.clockProvider = clockProvider;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public UUID writePaymentAuthorized(
            UUID paymentId, UUID merchantId, long eventSeq, long amount, String currency, String pspRef) {
        PaymentEvent.Builder builder = baseEvent(paymentId, merchantId, eventSeq, "payment.authorized")
                .setAuthorized(PaymentAuthorized.newBuilder()
                        .setAmount(amount)
                        .setCurrency(currency)
                        .setPspRef(pspRef != null ? pspRef : "")
                        .build());
        return insertOutboxRow(paymentId, eventSeq, "payment.authorized", builder.build());
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public UUID writePaymentFailed(
            UUID paymentId, UUID merchantId, long eventSeq, long amount, String currency, String pspRef, String reason) {
        PaymentEvent.Builder builder = baseEvent(paymentId, merchantId, eventSeq, "payment.failed")
                .setFailed(PaymentFailed.newBuilder()
                        .setAmount(amount)
                        .setCurrency(currency)
                        .setPspRef(pspRef != null ? pspRef : "")
                        .setReason(reason != null ? reason : "DECLINED")
                        .build());
        return insertOutboxRow(paymentId, eventSeq, "payment.failed", builder.build());
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public UUID writePaymentCaptured(
            UUID paymentId,
            UUID merchantId,
            long eventSeq,
            long capturedAmount,
            long feeAmount,
            long netMerchantAmount,
            String currency,
            String pspRef,
            UUID ledgerTxnId) {
        PaymentEvent.Builder builder = baseEvent(paymentId, merchantId, eventSeq, "payment.captured")
                .setCaptured(PaymentCaptured.newBuilder()
                        .setCapturedAmount(capturedAmount)
                        .setFeeAmount(feeAmount)
                        .setNetMerchantAmount(netMerchantAmount)
                        .setCurrency(currency)
                        .setPspRef(pspRef != null ? pspRef : "")
                        .setLedgerTxnId(ledgerTxnId.toString())
                        .build());
        return insertOutboxRow(paymentId, eventSeq, "payment.captured", builder.build());
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public UUID writeRefundProcessed(
            UUID paymentId,
            UUID merchantId,
            long eventSeq,
            UUID refundId,
            long refundAmount,
            long totalRefundedAmount,
            String currency,
            UUID ledgerTxnId) {
        PaymentEvent.Builder builder = baseEvent(paymentId, merchantId, eventSeq, "refund.processed")
                .setRefunded(RefundProcessed.newBuilder()
                        .setRefundId(refundId.toString())
                        .setRefundAmount(refundAmount)
                        .setTotalRefundedAmount(totalRefundedAmount)
                        .setCurrency(currency)
                        .setLedgerTxnId(ledgerTxnId.toString())
                        .build());
        return insertOutboxRow(paymentId, eventSeq, "refund.processed", builder.build());
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public UUID writePayoutCreated(
            UUID payoutId,
            UUID merchantId,
            long eventSeq,
            long amount,
            String currency,
            UUID ledgerTxnId) {
        PaymentEvent.Builder builder = baseEvent(payoutId, merchantId, eventSeq, "payout.created")
                .setPayout(PayoutCreated.newBuilder()
                        .setPayoutId(payoutId.toString())
                        .setAmount(amount)
                        .setCurrency(currency)
                        .setLedgerTxnId(ledgerTxnId.toString())
                        .build());
        return insertOutboxRow(payoutId, eventSeq, "payout.created", builder.build());
    }

    private PaymentEvent.Builder baseEvent(UUID aggregateId, UUID merchantId, long eventSeq, String eventType) {
        Instant now = clockProvider.now();
        UUID eventId = UuidV7.generate(now.toEpochMilli());
        String traceparent = TraceHeaders.currentTraceparentOrGenerate();
        Map<String, String> traceMap = new LinkedHashMap<>(TraceHeaders.currentContext());
        traceMap.put(TraceHeaders.TRACEPARENT, traceparent);

        return PaymentEvent.newBuilder()
                .setEventId(eventId.toString())
                .setEventType(eventType)
                .setSchemaVersion(1)
                .setPaymentId(aggregateId.toString())
                .setMerchantId(merchantId.toString())
                .setEventSeq(eventSeq)
                .setOccurredAt(Timestamp.newBuilder()
                        .setSeconds(now.getEpochSecond())
                        .setNanos(now.getNano())
                        .build())
                .putAllTraceContext(traceMap);
    }

    private UUID insertOutboxRow(UUID aggregateId, long eventSeq, String eventType, PaymentEvent event) {
        UUID eventId = UUID.fromString(event.getEventId());
        Instant now = Instant.ofEpochSecond(
                event.getOccurredAt().getSeconds(), event.getOccurredAt().getNanos());
        String traceJson;
        try {
            traceJson = objectMapper.writeValueAsString(event.getTraceContextMap());
        } catch (JsonProcessingException e) {
            traceJson = "{}";
        }

        jdbcClient.sql("""
                INSERT INTO outbox_events (
                    id, aggregate_id, event_seq, event_type, schema_version,
                    payload, trace_context, created_at, published_at
                ) VALUES (
                    :id, :aggregateId, :eventSeq, :eventType, :schemaVersion,
                    :payload, CAST(:traceContext AS jsonb), :createdAt, NULL
                )
                """)
                .param("id", eventId)
                .param("aggregateId", aggregateId)
                .param("eventSeq", eventSeq)
                .param("eventType", eventType)
                .param("schemaVersion", event.getSchemaVersion())
                .param("payload", event.toByteArray())
                .param("traceContext", traceJson)
                .param("createdAt", java.sql.Timestamp.from(now))
                .update();

        return eventId;
    }
}
