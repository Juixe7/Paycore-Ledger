package com.paycore.payments.merchant;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.HexFormat;

/**
 * Cryptographic API key generation, SHA-256 hashing, and constant-time comparison (Section 2 FR1 & Section 3 Security).
 * Raw API keys are returned once at merchant onboarding and never persisted or logged.
 */
public final class ApiKeyHasher {

    private static final SecureRandom SECURE_RANDOM = new SecureRandom();
    private static final HexFormat HEX = HexFormat.of();

    private ApiKeyHasher() {}

    public static String generateRawApiKey() {
        byte[] bytes = new byte[24];
        SECURE_RANDOM.nextBytes(bytes);
        return "pc_live_" + HEX.formatHex(bytes);
    }

    public static String generateWebhookSecret() {
        byte[] bytes = new byte[24];
        SECURE_RANDOM.nextBytes(bytes);
        return "whsec_" + HEX.formatHex(bytes);
    }

    public static String sha256Hex(String rawInput) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(rawInput.getBytes(StandardCharsets.UTF_8));
            return HEX.formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    public static boolean constantTimeEquals(String expectedHex, String actualHex) {
        if (expectedHex == null || actualHex == null) {
            return false;
        }
        byte[] a = expectedHex.getBytes(StandardCharsets.UTF_8);
        byte[] b = actualHex.getBytes(StandardCharsets.UTF_8);
        return MessageDigest.isEqual(a, b);
    }
}
