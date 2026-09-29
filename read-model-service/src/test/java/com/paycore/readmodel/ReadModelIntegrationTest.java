package com.paycore.readmodel;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.Timestamp;
import com.paycore.common.events.v1.PaymentAuthorized;
import com.paycore.common.events.v1.PaymentCaptured;
import com.paycore.common.events.v1.PaymentEvent;
import com.paycore.common.events.v1.RefundProcessed;
import com.paycore.common.ids.UuidV7;
import com.paycore.readmodel.ReadModelProjectorService.CursorPage;
import com.paycore.readmodel.ReadModelProjectorService.MerchantSummaryResponse;
import com.paycore.readmodel.ReadModelProjectorService.PaymentViewRow;
import com.paycore.readmodel.ReadModelProjectorService.RebuildReport;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
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
class ReadModelIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("paycore_readmodel_test")
            .withUsername("paycore")
            .withPassword("paycore");

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("paycore.readmodel.kafka-listener-enabled", () -> "false");
    }

    @Autowired
    private ReadModelProjectorService projectorService;

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private TestRestTemplate restTemplate;

    private UUID merchantId;

    @BeforeEach
    void setUp() {
        jdbcClient.sql("TRUNCATE TABLE payment_view, merchant_daily_summary, processed_events, read_model_event_log")
                .update();
        merchantId = UuidV7.generate();
    }

    @Test
    @DisplayName("1. Out-of-order & duplicate event_seq projection buffers gaps and drains strictly in order")
    void outOfOrderAndDuplicateEventsProjectedStrictlyInSequenceOrder() {
        UUID paymentId = UuidV7.generate();
        Instant now = Instant.parse("2026-10-11T10:00:00Z");

        byte[] seq1Authorized = buildAuthorizedEvent(UuidV7.generate(), paymentId, merchantId, 1L, 10_000L, "psp_ref_1", now);
        byte[] seq2Captured = buildCapturedEvent(UuidV7.generate(), paymentId, merchantId, 2L, 10_000L, 290L, now.plusSeconds(5));
        byte[] seq3Refund1 = buildRefundedEvent(UuidV7.generate(), paymentId, merchantId, 3L, 2_500L, 2_500L, now.plusSeconds(10));
        byte[] seq4Refund2 = buildRefundedEvent(UuidV7.generate(), paymentId, merchantId, 4L, 1_500L, 4_000L, now.plusSeconds(20));

        // 1. Deliver seq 3 first (out-of-order gap: missing seq 1 and seq 2)
        projectorService.ingestEvent(seq3Refund1);
        assertThat(projectorService.listPayments(merchantId, null, 10).items()).isEmpty();

        // 2. Deliver seq 2 next (still missing seq 1)
        projectorService.ingestEvent(seq2Captured);
        // Also deliver duplicate seq 2
        projectorService.ingestEvent(seq2Captured);
        assertThat(projectorService.listPayments(merchantId, null, 10).items()).isEmpty();

        // 3. Deliver seq 1 -> gap is filled! Must automatically drain seq 1 -> seq 2 -> seq 3!
        projectorService.ingestEvent(seq1Authorized);

        List<PaymentViewRow> rowsAfterDrain = projectorService.listPayments(merchantId, null, 10).items();
        assertThat(rowsAfterDrain).hasSize(1);
        PaymentViewRow view = rowsAfterDrain.get(0);
        assertThat(view.status()).isEqualTo("CAPTURED");
        assertThat(view.amount()).isEqualTo(10_000L);
        assertThat(view.capturedAmount()).isEqualTo(10_000L);
        assertThat(view.feeAmount()).isEqualTo(290L);
        assertThat(view.refundedAmount()).isEqualTo(2_500L);
        assertThat(view.lastEventSeq()).isEqualTo(3L);

        // 4. Deliver seq 4 and duplicate seq 1 + seq 3
        projectorService.ingestEvent(seq4Refund2);
        projectorService.ingestEvent(seq1Authorized);
        projectorService.ingestEvent(seq3Refund1);

        MerchantSummaryResponse summary = projectorService.getMerchantSummary(merchantId);
        assertThat(summary.totalCaptured()).isEqualTo(10_000L);
        assertThat(summary.totalFees()).isEqualTo(290L);
        assertThat(summary.totalRefunded()).isEqualTo(4_000L);
        assertThat(summary.netMerchantAmount()).isEqualTo(10_000L - 290L - 4_000L);
        assertThat(summary.paymentCount()).isEqualTo(1L);
    }

    @Test
    @DisplayName("2. Cursor pagination (UUIDv7) + Rebuild-from-Scratch matches live projection 100%")
    void cursorPaginationAndRebuildFromOffsetZero() {
        Instant baseTime = Instant.parse("2026-10-11T12:00:00Z");
        for (int i = 1; i <= 15; i++) {
            UUID paymentId = UuidV7.generate();
            long amt = i * 1_000L;
            long fee = (amt * 290L) / 10_000L;
            projectorService.ingestEvent(buildAuthorizedEvent(
                    UuidV7.generate(), paymentId, merchantId, 1L, amt, "psp_" + i, baseTime.plusSeconds(i)));
            projectorService.ingestEvent(buildCapturedEvent(
                    UuidV7.generate(), paymentId, merchantId, 2L, amt, fee, baseTime.plusSeconds(i + 1)));
            if (i % 3 == 0) {
                projectorService.ingestEvent(buildRefundedEvent(
                        UuidV7.generate(), paymentId, merchantId, 3L, 200L, 200L, baseTime.plusSeconds(i + 2)));
            }
        }

        // Paginate in pages of 5 via REST controller
        List<UUID> collectedIds = new ArrayList<>();
        UUID cursor = null;
        int pages = 0;
        while (true) {
            String url = "/v1/payments?limit=5" + (cursor != null ? "&cursor=" + cursor : "");
            HttpHeaders headers = new HttpHeaders();
            headers.set("X-Merchant-Id", merchantId.toString());
            ResponseEntity<CursorPage<PaymentViewRow>> pageResp = restTemplate.exchange(
                    url,
                    HttpMethod.GET,
                    new HttpEntity<>(headers),
                    new ParameterizedTypeReference<>() {});
            assertThat(pageResp.getStatusCode()).isEqualTo(HttpStatus.OK);
            CursorPage<PaymentViewRow> page = pageResp.getBody();
            assertThat(page).isNotNull();
            page.items().forEach(r -> collectedIds.add(r.paymentId()));
            pages++;
            if (!page.hasMore()) {
                break;
            }
            cursor = page.nextCursor();
        }

        assertThat(pages).isEqualTo(3);
        assertThat(collectedIds).hasSize(15).doesNotHaveDuplicates();

        // Capture live summary before rebuild
        MerchantSummaryResponse beforeRebuild = projectorService.getMerchantSummary(merchantId);

        // Trigger POST /v1/admin/read-model/rebuild (truncates projection tables and replays from offset 0)
        ResponseEntity<RebuildReport> rebuildResp =
                restTemplate.postForEntity("/v1/admin/read-model/rebuild", null, RebuildReport.class);
        assertThat(rebuildResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        RebuildReport report = rebuildResp.getBody();
        assertThat(report).isNotNull();
        assertThat(report.paymentsProjected()).isEqualTo(15L);
        assertThat(report.eventsProcessed()).isEqualTo(35L); // 15 auth + 15 capture + 5 refunds

        // Verify post-rebuild summary matches pre-rebuild summary paise-for-paise
        MerchantSummaryResponse afterRebuild = projectorService.getMerchantSummary(merchantId);
        assertThat(afterRebuild).isEqualTo(beforeRebuild);
    }

    private byte[] buildAuthorizedEvent(
            UUID eventId, UUID paymentId, UUID merchantId, long seq, long amount, String pspRef, Instant at) {
        return PaymentEvent.newBuilder()
                .setEventId(eventId.toString())
                .setPaymentId(paymentId.toString())
                .setMerchantId(merchantId.toString())
                .setEventSeq(seq)
                .setEventType("payment.authorized")
                .setOccurredAt(Timestamp.newBuilder().setSeconds(at.getEpochSecond()).setNanos(at.getNano()).build())
                .setAuthorized(PaymentAuthorized.newBuilder()
                        .setAmount(amount)
                        .setCurrency("INR")
                        .setPspRef(pspRef)
                        .build())
                .build()
                .toByteArray();
    }

    private byte[] buildCapturedEvent(
            UUID eventId, UUID paymentId, UUID merchantId, long seq, long capturedAmt, long feeAmt, Instant at) {
        return PaymentEvent.newBuilder()
                .setEventId(eventId.toString())
                .setPaymentId(paymentId.toString())
                .setMerchantId(merchantId.toString())
                .setEventSeq(seq)
                .setEventType("payment.captured")
                .setOccurredAt(Timestamp.newBuilder().setSeconds(at.getEpochSecond()).setNanos(at.getNano()).build())
                .setCaptured(PaymentCaptured.newBuilder()
                        .setCapturedAmount(capturedAmt)
                        .setFeeAmount(feeAmt)
                        .setCurrency("INR")
                        .setLedgerTxnId(UuidV7.generate().toString())
                        .build())
                .build()
                .toByteArray();
    }

    private byte[] buildRefundedEvent(
            UUID eventId, UUID paymentId, UUID merchantId, long seq, long refundAmt, long totalRefunded, Instant at) {
        return PaymentEvent.newBuilder()
                .setEventId(eventId.toString())
                .setPaymentId(paymentId.toString())
                .setMerchantId(merchantId.toString())
                .setEventSeq(seq)
                .setEventType("refund.processed")
                .setOccurredAt(Timestamp.newBuilder().setSeconds(at.getEpochSecond()).setNanos(at.getNano()).build())
                .setRefunded(RefundProcessed.newBuilder()
                        .setRefundId(UuidV7.generate().toString())
                        .setRefundAmount(refundAmt)
                        .setTotalRefundedAmount(totalRefunded)
                        .setCurrency("INR")
                        .setLedgerTxnId(UuidV7.generate().toString())
                        .build())
                .build()
                .toByteArray();
    }
}
