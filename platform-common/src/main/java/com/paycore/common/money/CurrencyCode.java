package com.paycore.common.money;

public enum CurrencyCode {
    INR(2);

    private final int minorUnitDecimals;

    CurrencyCode(int minorUnitDecimals) {
        this.minorUnitDecimals = minorUnitDecimals;
    }

    public int minorUnitDecimals() {
        return minorUnitDecimals;
    }

    public static CurrencyCode fromString(String code) {
        if (code == null || code.isBlank()) {
            throw new IllegalArgumentException("Currency code is required");
        }
        try {
            return CurrencyCode.valueOf(code.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unsupported currency: " + code + " (only INR is supported in scope)");
        }
    }
}
