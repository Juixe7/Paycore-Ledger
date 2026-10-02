package com.paycore.webhook;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * HMAC-SHA256 webhook payload signer and constant-time verifier (Section 5.4).
 *
 * <p>Signature formula: {@code HMAC-SHA256(secret, timestamp + "." + body)}.
 */
public final class WebhookSigner {

    private static final String HMAC_SHA256 = "HmacSHA256";
    private static final HexFormat HEX = HexFormat.of();

    private WebhookSigner() {}

    public static String sign(String secret, String timestampSeconds, String body) {
        try {
            Mac mac = Mac.getInstance(HMAC_SHA256);
            SecretKeySpec keySpec = new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), HMAC_SHA256);
            mac.init(keySpec);
            String signingInput = timestampSeconds + "." + body;
            byte[] rawHmac = mac.doFinal(signingInput.getBytes(StandardCharsets.UTF_8));
            return HEX.formatHex(rawHmac);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to compute HMAC-SHA256 webhook signature", e);
        }
    }

    public static boolean verify(String secret, String timestampSeconds, String body, String candidateSignature) {
        if (secret == null || timestampSeconds == null || body == null || candidateSignature == null) {
            return false;
        }
        String expected = sign(secret, timestampSeconds, body);
        return MessageDigest.isEqual(
                expected.getBytes(StandardCharsets.UTF_8),
                candidateSignature.getBytes(StandardCharsets.UTF_8));
    }
}
