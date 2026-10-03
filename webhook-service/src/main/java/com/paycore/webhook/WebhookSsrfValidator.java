package com.paycore.webhook;

import com.paycore.common.errors.PayCoreException;
import java.net.InetAddress;
import java.net.URI;

/**
 * Delivery-time SSRF guard (Section 5.4).
 * Re-validates merchant webhook URLs right before executing the outbound HTTP POST
 * to prevent DNS rebinding or private/loopback IP access.
 */
public final class WebhookSsrfValidator {

    private WebhookSsrfValidator() {}

    public static void validateAtDeliveryTime(String url, boolean allowLocalInTest) {
        if (url == null || url.isBlank()) {
            throw PayCoreException.unprocessable("INVALID_WEBHOOK_URL", "Missing Webhook URL", "Webhook URL is blank");
        }
        URI uri = URI.create(url.trim());
        String host = uri.getHost();
        if (host == null || host.isBlank()) {
            throw PayCoreException.unprocessable("INVALID_WEBHOOK_URL", "Invalid Webhook Host", "Host is missing");
        }
        if (allowLocalInTest) {
            return;
        }
        try {
            InetAddress[] addresses = InetAddress.getAllByName(host);
            for (InetAddress addr : addresses) {
                if (addr.isLoopbackAddress()
                        || addr.isSiteLocalAddress()
                        || addr.isLinkLocalAddress()
                        || addr.isAnyLocalAddress()
                        || addr.isMulticastAddress()) {
                    throw PayCoreException.unprocessable(
                            "SSRF_DELIVERY_BLOCKED",
                            "Webhook Delivery Blocked by SSRF Guard",
                            "Resolved address for " + host + " is private, loopback, or link-local");
                }
            }
        } catch (PayCoreException e) {
            throw e;
        } catch (Exception e) {
            String lower = host.toLowerCase();
            if (lower.equals("localhost") || lower.startsWith("127.") || lower.startsWith("10.")
                    || lower.startsWith("192.168.") || lower.startsWith("169.254.")) {
                throw PayCoreException.unprocessable(
                        "SSRF_DELIVERY_BLOCKED",
                        "Webhook Delivery Blocked by SSRF Guard",
                        "Host " + host + " is a blocked private/loopback target");
            }
        }
    }
}
