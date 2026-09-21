package com.paycore.payments.payment;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record Payment(
        UUID id,
        UUID merchantId,
        long amount,
        String currency,
        PaymentStatus status,
        long capturedAmount,
        long refundedAmount,
        String pspRef,
        Map<String, String> metadata,
        long version,
        long eventSeq,
        Instant createdAt,
        Instant updatedAt) {}
