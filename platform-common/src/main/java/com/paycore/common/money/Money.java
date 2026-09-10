package com.paycore.common.money;

import java.util.Objects;

/**
 * Immutable monetary value represented strictly in minor units (paise for INR) as a 64-bit signed integer.
 *
 * <p>Design Rule (Section 5.1): All money is BIGINT in minor units (paise). Never floats or decimals-as-doubles.
 */
public record Money(long amountMinor, CurrencyCode currency) implements Comparable<Money> {

    public Money {
        Objects.requireNonNull(currency, "currency must not be null");
    }

    public static Money inr(long paise) {
        return new Money(paise, CurrencyCode.INR);
    }

    public static Money ofPositive(long amountMinor, CurrencyCode currency) {
        if (amountMinor <= 0) {
            throw new IllegalArgumentException("Amount must be strictly positive (> 0), got: " + amountMinor);
        }
        return new Money(amountMinor, currency);
    }

    public static Money ofNonNegative(long amountMinor, CurrencyCode currency) {
        if (amountMinor < 0) {
            throw new IllegalArgumentException("Amount must be non-negative (>= 0), got: " + amountMinor);
        }
        return new Money(amountMinor, currency);
    }

    public Money add(Money other) {
        requireSameCurrency(other);
        return new Money(Math.addExact(this.amountMinor, other.amountMinor), this.currency);
    }

    public Money subtract(Money other) {
        requireSameCurrency(other);
        return new Money(Math.subtractExact(this.amountMinor, other.amountMinor), this.currency);
    }

    /**
     * Computes platform fee using basis points (100 bps = 1%, 200 bps = 2%).
     * Uses exact integer floor division: (amount * bps) / 10_000.
     */
    public Money calculateFeeBps(int basisPoints) {
        if (basisPoints < 0 || basisPoints > 10_000) {
            throw new IllegalArgumentException("Invalid basis points: " + basisPoints);
        }
        long fee = Math.multiplyExact(this.amountMinor, (long) basisPoints) / 10_000L;
        return new Money(fee, this.currency);
    }

    public boolean isPositive() {
        return amountMinor > 0;
    }

    public boolean isZero() {
        return amountMinor == 0;
    }

    public boolean isNegative() {
        return amountMinor < 0;
    }

    private void requireSameCurrency(Money other) {
        Objects.requireNonNull(other, "other money must not be null");
        if (this.currency != other.currency) {
            throw new IllegalArgumentException(
                    "Currency mismatch: " + this.currency + " vs " + other.currency);
        }
    }

    @Override
    public int compareTo(Money other) {
        requireSameCurrency(other);
        return Long.compare(this.amountMinor, other.amountMinor);
    }
}
