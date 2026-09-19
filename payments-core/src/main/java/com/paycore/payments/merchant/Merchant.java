package com.paycore.payments.merchant;

import java.time.Instant;
import java.util.UUID;

public record Merchant(
        UUID id,
        String name,
        String apiKeyHash,
        String webhookUrl,
        String webhookSecret,
        int feeBps,
        Instant createdAt) {}
