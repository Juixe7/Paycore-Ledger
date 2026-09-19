package com.paycore.payments.merchant;

import com.paycore.common.errors.PayCoreException;
import java.net.InetAddress;
import java.net.URI;

/**
 * SSRF protection for merchant webhook URLs (Section 5.4).
 * Blocks loopback (127.0.0.0/8, ::1), RFC1918 private ranges (10/8, 172.16/12, 192.168/16),
 * link-local (169.254/16, cloud metadata), and multicast addresses.
 */
public final class SsrfUrlValidator {

    private SsrfUrlValidator() {}

    public static void validateWebhookUrl(String url, boolean allowLocalInDev) {
        if (url == null || url.isBlank()) {
            return;
        }
        URI uri;
        try {
            uri = URI.create(url.trim());
        } catch (IllegalArgumentException e) {
            throw PayCoreException.unprocessable("INVALID_WEBHOOK_URL", "Invalid Webhook URL", "Malformed webhook URL: " + url);
        }

        String scheme = uri.getScheme();
        if (scheme == null || (!scheme.equalsIgnoreCase("https") && !scheme.equalsIgnoreCase("http"))) {
            throw PayCoreException.unprocessable(
                    "INVALID_WEBHOOK_URL", "Invalid Webhook URL Scheme", "Webhook URL must use http or https");
        }

        String host = uri.getHost();
        if (host == null || host.isBlank()) {
            throw PayCoreException.unprocessable("INVALID_WEBHOOK_URL", "Invalid Webhook URL Host", "Webhook URL must include a valid host");
        }

        if (allowLocalInDev) {
            return;
        }

        try {
            InetAddress[] addresses = InetAddress.getAllByName(host);
            for (InetAddress addr : addresses) {
                if (addr.isLoopbackAddress()
                        || addr.isSiteLocalAddress()
                        || addr.isLinkLocalAddress()
                        || addr.isAnyLocalAddress()
                        || addr.isMulticastAddress()
                        || isCgnatOrMetadataIp(addr.getAddress())) {
                    throw PayCoreException.unprocessable(
                            "SSRF_BLOCKED_WEBHOOK_URL",
                            "Webhook URL Blocked by SSRF Policy",
                            "Webhook URL resolves to a private, loopback, or link-local address: " + host);
                }
            }
        } catch (PayCoreException e) {
            throw e;
        } catch (Exception e) {
            // If host is a literal private IP or unresolvable internal hostname
            String lower = host.toLowerCase();
            if (lower.equals("localhost") || lower.startsWith("127.") || lower.startsWith("10.")
                    || lower.startsWith("192.168.") || lower.startsWith("169.254.")) {
                throw PayCoreException.unprocessable(
                        "SSRF_BLOCKED_WEBHOOK_URL",
                        "Webhook URL Blocked by SSRF Policy",
                        "Webhook URL targets a disallowed private/loopback host: " + host);
            }
        }
    }

    private static boolean isCgnatOrMetadataIp(byte[] ipv4) {
        if (ipv4.length != 4) {
            return false;
        }
        int b0 = ipv4[0] & 0xFF;
        int b1 = ipv4[1] & 0xFF;
        // 100.64.0.0/10 (Carrier-grade NAT) or 169.254.169.254 (Cloud metadata)
        return (b0 == 100 && b1 >= 64 && b1 <= 127) || (b0 == 169 && b1 == 254);
    }
}
