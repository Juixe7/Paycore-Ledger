package com.paycore.payments.psp;

import java.util.UUID;

/**
 * Acquirer / PSP client contract (Section 4.3 & FR8).
 *
 * <p>Uses a deterministic {@code pspRef = paymentId + ":" + attemptNo} so retries and
 * unknown-outcome resolution queries are strictly idempotent at the PSP.
 */
public interface PspClient {

    PspAuthResult authorize(PspAuthRequest request);

    default PspCaptureResult capture(PspCaptureRequest request) {
        return new PspCaptureResult(PspOutcome.AUTHORIZED, request.pspRef(), 0L, "OK");
    }

    default PspRefundResult refund(PspRefundRequest request) {
        return new PspRefundResult(PspOutcome.AUTHORIZED, request.pspRef(), 0L, "OK");
    }

    PspStatusLookupResult lookupByPspRef(String pspRef);

    record PspAuthRequest(
            UUID paymentId,
            UUID merchantId,
            int attemptNo,
            String pspRef,
            long amountMinor,
            String currency) {}

    record PspCaptureRequest(
            UUID paymentId,
            UUID merchantId,
            String pspRef,
            long amountMinor,
            String currency) {}

    record PspRefundRequest(
            UUID paymentId,
            UUID merchantId,
            String pspRef,
            long refundAmountMinor,
            String currency) {}

    enum PspOutcome {
        AUTHORIZED,
        DECLINED,
        UNKNOWN_TIMEOUT
    }

    record PspAuthResult(
            PspOutcome outcome,
            String pspRef,
            long latencyMs,
            String detail) {}

    record PspCaptureResult(
            PspOutcome outcome,
            String pspRef,
            long latencyMs,
            String detail) {}

    record PspRefundResult(
            PspOutcome outcome,
            String pspRef,
            long latencyMs,
            String detail) {}

    enum PspLookupState {
        AUTHORIZED,
        DECLINED,
        NOT_FOUND,
        LOOKUP_ERROR
    }

    record PspStatusLookupResult(
            PspLookupState state,
            String pspRef,
            long amountMinor) {}
}
