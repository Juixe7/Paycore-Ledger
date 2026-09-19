package com.paycore.payments.merchant;

import com.paycore.common.clock.ClockProvider;
import com.paycore.common.errors.PayCoreException;
import com.paycore.common.ids.UuidV7;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class MerchantService {

    private final JdbcClient jdbcClient;
    private final ClockProvider clockProvider;
    private final boolean allowLocalWebhooks;

    public MerchantService(
            JdbcClient jdbcClient,
            ClockProvider clockProvider,
            @Value("${paycore.security.allow-local-webhooks:false}") boolean allowLocalWebhooks) {
        this.jdbcClient = jdbcClient;
        this.clockProvider = clockProvider;
        this.allowLocalWebhooks = allowLocalWebhooks;
    }

    @Transactional
    public CreatedMerchantResponse createMerchant(CreateMerchantRequest request) {
        if (request.name() == null || request.name().isBlank()) {
            throw PayCoreException.unprocessable("INVALID_MERCHANT_NAME", "Invalid Merchant Name", "Merchant name is required");
        }
        SsrfUrlValidator.validateWebhookUrl(request.webhookUrl(), allowLocalWebhooks);

        int feeBps = request.feeBps() != null ? request.feeBps() : 200; // Default 2.00% platform fee
        if (feeBps < 0 || feeBps > 10_000) {
            throw PayCoreException.unprocessable("INVALID_FEE_BPS", "Invalid Fee Basis Points", "feeBps must be between 0 and 10000");
        }

        Instant now = clockProvider.now();
        UUID merchantId = UuidV7.generate(now.toEpochMilli());
        String rawApiKey = ApiKeyHasher.generateRawApiKey();
        String apiKeyHash = ApiKeyHasher.sha256Hex(rawApiKey);
        String webhookSecret = request.webhookSecret() != null && !request.webhookSecret().isBlank()
                ? request.webhookSecret()
                : ApiKeyHasher.generateWebhookSecret();

        jdbcClient.sql("""
                INSERT INTO merchants (id, name, api_key_hash, webhook_url, webhook_secret, fee_bps, created_at)
                VALUES (:id, :name, :apiKeyHash, :webhookUrl, :webhookSecret, :feeBps, :createdAt)
                """)
                .param("id", merchantId)
                .param("name", request.name().trim())
                .param("apiKeyHash", apiKeyHash)
                .param("webhookUrl", request.webhookUrl())
                .param("webhookSecret", webhookSecret)
                .param("feeBps", feeBps)
                .param("createdAt", Timestamp.from(now))
                .update();

        return new CreatedMerchantResponse(
                merchantId,
                request.name().trim(),
                rawApiKey,
                request.webhookUrl(),
                webhookSecret,
                feeBps,
                now);
    }

    @Transactional(readOnly = true)
    public Optional<Merchant> authenticateByRawApiKey(String rawApiKey) {
        if (rawApiKey == null || rawApiKey.isBlank()) {
            return Optional.empty();
        }
        String candidateHash = ApiKeyHasher.sha256Hex(rawApiKey.trim());
        Optional<Merchant> found = jdbcClient.sql("""
                SELECT id, name, api_key_hash, webhook_url, webhook_secret, fee_bps, created_at
                FROM merchants
                WHERE api_key_hash = :apiKeyHash
                """)
                .param("apiKeyHash", candidateHash)
                .query((rs, rowNum) -> new Merchant(
                        rs.getObject("id", UUID.class),
                        rs.getString("name"),
                        rs.getString("api_key_hash"),
                        rs.getString("webhook_url"),
                        rs.getString("webhook_secret"),
                        rs.getInt("fee_bps"),
                        rs.getTimestamp("created_at").toInstant()))
                .optional();

        // Constant-time verification (Section 3 Security)
        return found.filter(m -> ApiKeyHasher.constantTimeEquals(m.apiKeyHash(), candidateHash));
    }

    @Transactional(readOnly = true)
    public Merchant getById(UUID merchantId) {
        return jdbcClient.sql("""
                SELECT id, name, api_key_hash, webhook_url, webhook_secret, fee_bps, created_at
                FROM merchants
                WHERE id = :id
                """)
                .param("id", merchantId)
                .query((rs, rowNum) -> new Merchant(
                        rs.getObject("id", UUID.class),
                        rs.getString("name"),
                        rs.getString("api_key_hash"),
                        rs.getString("webhook_url"),
                        rs.getString("webhook_secret"),
                        rs.getInt("fee_bps"),
                        rs.getTimestamp("created_at").toInstant()))
                .optional()
                .orElseThrow(() -> PayCoreException.notFound("Merchant", merchantId));
    }

    public record CreateMerchantRequest(
            String name,
            String webhookUrl,
            String webhookSecret,
            Integer feeBps) {}

    public record CreatedMerchantResponse(
            UUID id,
            String name,
            String apiKey,
            String webhookUrl,
            String webhookSecret,
            int feeBps,
            Instant createdAt) {}
}
