package com.paycore.payments.payment;

import com.paycore.common.errors.PayCoreException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Strict payment lifecycle state machine (Section 2 FR2 & Section 4.3).
 */
public enum PaymentStatus {
    CREATED,
    AUTHORIZING,
    AUTHORIZED,
    CAPTURED,
    PARTIALLY_REFUNDED,
    REFUNDED,
    VOIDED,
    FAILED;

    private static final Map<PaymentStatus, Set<PaymentStatus>> ALLOWED_TRANSITIONS = Map.of(
            CREATED, EnumSet.of(AUTHORIZING),
            AUTHORIZING, EnumSet.of(AUTHORIZED, FAILED),
            AUTHORIZED, EnumSet.of(CAPTURED, VOIDED),
            CAPTURED, EnumSet.of(PARTIALLY_REFUNDED, REFUNDED),
            PARTIALLY_REFUNDED, EnumSet.of(PARTIALLY_REFUNDED, REFUNDED),
            REFUNDED, EnumSet.noneOf(PaymentStatus.class),
            VOIDED, EnumSet.noneOf(PaymentStatus.class),
            FAILED, EnumSet.noneOf(PaymentStatus.class)
    );

    public boolean canTransitionTo(PaymentStatus target) {
        return ALLOWED_TRANSITIONS.getOrDefault(this, Set.of()).contains(target);
    }

    public void requireTransitionTo(UUID paymentId, PaymentStatus target, String operation) {
        if (!canTransitionTo(target)) {
            throw PayCoreException.invalidStateTransition(paymentId.toString(), this.name(), operation);
        }
    }
}
