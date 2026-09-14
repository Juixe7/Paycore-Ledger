package com.paycore.payments.ledger;

import com.paycore.common.money.Money;
import java.util.Objects;
import java.util.UUID;

/**
 * Immutable command representing a balanced double-entry accounting transaction.
 *
 * <p>Accounting model (Section 5.1, Page 9):
 * <ul>
 *   <li>Capture: Debit PSP_RECEIVABLE (gross), Credit MERCHANT_PAYABLE (net), Credit PLATFORM_FEE_REVENUE (fee)</li>
 *   <li>Refund (fee not returned policy): Debit MERCHANT_PAYABLE (amount), Credit PSP_RECEIVABLE (amount)</li>
 *   <li>Payout: Debit MERCHANT_PAYABLE (amount), Credit BANK (amount)</li>
 * </ul>
 */
public record LedgerTransaction(
        UUID paymentId,
        UUID merchantId,
        LedgerTransactionKind kind,
        Money grossAmount,
        Money feeAmount,
        String postedBy) {

    public LedgerTransaction {
        Objects.requireNonNull(merchantId, "merchantId is required");
        Objects.requireNonNull(kind, "kind is required");
        Objects.requireNonNull(grossAmount, "grossAmount is required");
        Objects.requireNonNull(feeAmount, "feeAmount is required");
        Objects.requireNonNull(postedBy, "postedBy is required");
        if (!grossAmount.isPositive()) {
            throw new IllegalArgumentException("Ledger transaction grossAmount must be > 0");
        }
        if (feeAmount.isNegative() || feeAmount.compareTo(grossAmount) >= 0) {
            throw new IllegalArgumentException("feeAmount must be >= 0 and < grossAmount");
        }
    }

    public static LedgerTransaction forCapture(
            UUID paymentId, UUID merchantId, Money grossAmount, Money feeAmount, String postedBy) {
        Objects.requireNonNull(paymentId, "paymentId is required for CAPTURE");
        return new LedgerTransaction(
                paymentId, merchantId, LedgerTransactionKind.CAPTURE, grossAmount, feeAmount, postedBy);
    }

    public static LedgerTransaction forRefund(
            UUID paymentId, UUID merchantId, Money refundAmount, String postedBy) {
        Objects.requireNonNull(paymentId, "paymentId is required for REFUND");
        return new LedgerTransaction(
                paymentId,
                merchantId,
                LedgerTransactionKind.REFUND,
                refundAmount,
                Money.ofNonNegative(0L, refundAmount.currency()),
                postedBy);
    }

    public static LedgerTransaction forPayout(
            UUID merchantId, Money payoutAmount, String postedBy) {
        return new LedgerTransaction(
                null,
                merchantId,
                LedgerTransactionKind.PAYOUT,
                payoutAmount,
                Money.ofNonNegative(0L, payoutAmount.currency()),
                postedBy);
    }

    public Money netMerchantAmount() {
        return grossAmount.subtract(feeAmount);
    }
}
