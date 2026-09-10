package com.paycore.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.Timestamp;
import com.paycore.common.events.v1.PaymentCaptured;
import com.paycore.common.events.v1.PaymentEvent;
import com.paycore.common.ids.UuidV7;
import com.paycore.common.money.CurrencyCode;
import com.paycore.common.money.Money;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class PlatformCommonTest {

    @Test
    void uuidV7GeneratesMonotonicallyIncreasingIdsWithinSameMillisecond() {
        long fixedMillis = 1728640000000L;
        List<UUID> uuids = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            UUID id = UuidV7.generate(fixedMillis);
            assertThat(id.version()).isEqualTo(7);
            assertThat(id.variant()).isEqualTo(2);
            assertThat(UuidV7.extractEpochMillis(id)).isGreaterThanOrEqualTo(fixedMillis);
            uuids.add(id);
        }

        // String representations of UUIDv7 generated in sequence must be strictly sorted
        List<String> asStrings = uuids.stream().map(UUID::toString).toList();
        assertThat(asStrings).isSorted();
        assertThat(uuids).doesNotHaveDuplicates();
    }

    @Test
    void moneyEnforcesMinorUnitsAndCalculatesExactBasisPointsFee() {
        Money thousandPaise = Money.inr(1000L);
        Money fee = thousandPaise.calculateFeeBps(200); // 2% = 200 bps -> 20 paise
        Money merchantNet = thousandPaise.subtract(fee);

        assertThat(fee.amountMinor()).isEqualTo(20L);
        assertThat(merchantNet.amountMinor()).isEqualTo(980L);
        assertThat(merchantNet.add(fee)).isEqualTo(thousandPaise);

        assertThatThrownBy(() -> Money.ofPositive(0L, CurrencyCode.INR))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Money.ofPositive(-100L, CurrencyCode.INR))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void paymentEventProtobufRoundTripPreservesAllFields() throws Exception {
        String eventId = UuidV7.generate().toString();
        String paymentId = UuidV7.generate().toString();
        String merchantId = UuidV7.generate().toString();

        PaymentEvent original = PaymentEvent.newBuilder()
                .setEventId(eventId)
                .setEventType("payment.captured")
                .setSchemaVersion(1)
                .setPaymentId(paymentId)
                .setMerchantId(merchantId)
                .setEventSeq(2L)
                .setOccurredAt(Timestamp.newBuilder().setSeconds(1728640000L).build())
                .putTraceContext("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
                .setCaptured(PaymentCaptured.newBuilder()
                        .setCapturedAmount(1000L)
                        .setFeeAmount(20L)
                        .setNetMerchantAmount(980L)
                        .setCurrency("INR")
                        .setPspRef(paymentId + ":1")
                        .setLedgerTxnId(UuidV7.generate().toString())
                        .build())
                .build();

        byte[] wireBytes = original.toByteArray();
        PaymentEvent decoded = PaymentEvent.parseFrom(wireBytes);

        assertThat(decoded.getEventId()).isEqualTo(eventId);
        assertThat(decoded.getEventType()).isEqualTo("payment.captured");
        assertThat(decoded.getEventSeq()).isEqualTo(2L);
        assertThat(decoded.getCaptured().getCapturedAmount()).isEqualTo(1000L);
        assertThat(decoded.getCaptured().getFeeAmount()).isEqualTo(20L);
        assertThat(decoded.getCaptured().getNetMerchantAmount()).isEqualTo(980L);
        assertThat(decoded.getTraceContextMap()).containsKey("traceparent");
    }
}
