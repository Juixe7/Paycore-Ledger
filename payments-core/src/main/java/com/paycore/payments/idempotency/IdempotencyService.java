package com.paycore.payments.idempotency;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.paycore.common.clock.ClockProvider;
import com.paycore.common.errors.PayCoreException;
import com.paycore.payments.merchant.ApiKeyHasher;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Authoritative Idempotency Engine (Section 5.2 & Section 6.3).
 *
 * <p>Guarantees:
 * <ul>
 *   <li>Same key + same canonical request hash -> returns original status code and body</li>
 *   <li>Same key + different request hash -> 422 Unprocessable Entity</li>
 *   <li>Concurrent same-key requests -> exactly one executes; peers await completion without starving DB connections</li>
 *   <li>Business change + idempotency COMPLETED update commit in the SAME transaction when possible</li>
 *   <li>Expired IN_PROGRESS lock -> safe takeover backed by state-machine compare-and-set</li>
 *   <li>Redis is strictly a non-authoritative fast path; any Redis miss or failure falls through to Postgres</li>
 * </ul>
 */
@Service
public class IdempotencyService {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyService.class);
    private static final Duration LOCK_DURATION = Duration.ofSeconds(30);
    private static final Duration REDIS_TTL = Duration.ofHours(24);

    private static final ObjectMapper CANONICAL_MAPPER = JsonMapper.builder()
            .configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true)
            .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
            .build();

    private final JdbcClient jdbcClient;
    private final ObjectMapper objectMapper;
    private final ClockProvider clockProvider;
    private final TransactionTemplate requiresNewTx;
    private final TransactionTemplate requiredTx;
    private final StringRedisTemplate redisTemplate;
    private volatile boolean redisEnabled;
    private final long pollTimeoutMs;

    private final Map<String, CompletableFuture<StoredIdempotencyRecord>> inFlightFutures =
            new ConcurrentHashMap<>();

    public IdempotencyService(
            JdbcClient jdbcClient,
            ObjectMapper objectMapper,
            ClockProvider clockProvider,
            PlatformTransactionManager txManager,
            @Autowired(required = false) StringRedisTemplate redisTemplate,
            @Value("${paycore.redis.enabled:false}") boolean redisEnabled,
            @Value("${paycore.idempotency.poll-timeout-ms:10000}") long pollTimeoutMs) {
        this.jdbcClient = jdbcClient;
        this.objectMapper = objectMapper;
        this.clockProvider = clockProvider;
        this.requiresNewTx = new TransactionTemplate(txManager);
        this.requiresNewTx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        this.requiredTx = new TransactionTemplate(txManager);
        this.requiredTx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);
        this.redisTemplate = redisTemplate;
        this.redisEnabled = redisEnabled;
        this.pollTimeoutMs = pollTimeoutMs;
    }

    public <T> IdempotentResult<T> execute(
            UUID merchantId,
            String idempotencyKey,
            String httpMethod,
            String requestPath,
            Object requestBodyObj,
            int successStatusCode,
            Class<T> responseType,
            Supplier<T> action) {
        return executeInternal(
                merchantId,
                idempotencyKey,
                httpMethod,
                requestPath,
                requestBodyObj,
                successStatusCode,
                responseType,
                action,
                true);
    }

    /**
     * Used by two-transaction flows such as {@code authorizePayment} (Section 4.3) where the action
     * manages its own short transactions (Tx1 before PSP I/O, Tx2 after PSP I/O) so that zero DB
     * connections are held open while waiting on the external PSP network call.
     */
    public <T> IdempotentResult<T> executeTwoPhase(
            UUID merchantId,
            String idempotencyKey,
            String httpMethod,
            String requestPath,
            Object requestBodyObj,
            int successStatusCode,
            Class<T> responseType,
            Supplier<T> action) {
        return executeInternal(
                merchantId,
                idempotencyKey,
                httpMethod,
                requestPath,
                requestBodyObj,
                successStatusCode,
                responseType,
                action,
                false);
    }

    private <T> IdempotentResult<T> executeInternal(
            UUID merchantId,
            String idempotencyKey,
            String httpMethod,
            String requestPath,
            Object requestBodyObj,
            int successStatusCode,
            Class<T> responseType,
            Supplier<T> action,
            boolean wrapActionInOuterTx) {

        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            throw PayCoreException.unprocessable(
                    "MISSING_IDEMPOTENCY_KEY",
                    "Missing Idempotency-Key Header",
                    "Every mutating endpoint requires an Idempotency-Key header.");
        }

        String trimmedKey = idempotencyKey.trim();
        String requestHash = computeRequestHash(httpMethod, requestPath, requestBodyObj);
        String compositeKey = redisKey(merchantId, trimmedKey);

        // 1. Redis fast path (an optimisation only; Postgres is the authority - Section 5.2)
        Optional<StoredIdempotencyRecord> cached = tryReadFromRedis(merchantId, trimmedKey);
        if (cached.isPresent()) {
            StoredIdempotencyRecord rec = cached.get();
            if (!rec.requestHash().equals(requestHash)) {
                throw PayCoreException.idempotencyPayloadMismatch(trimmedKey);
            }
            if ("COMPLETED".equals(rec.status())) {
                return deserializeStoredResult(rec, responseType, true);
            }
        }

        // 2. Postgres authoritative claim (short independent transaction so concurrent threads see IN_PROGRESS)
        CompletableFuture<StoredIdempotencyRecord> myFuture = new CompletableFuture<>();
        inFlightFutures.putIfAbsent(compositeKey, myFuture);
        ClaimOutcome claim = claimInPostgres(merchantId, trimmedKey, requestHash);

        if (claim.state() == ClaimState.ALREADY_COMPLETED) {
            inFlightFutures.remove(compositeKey, myFuture);
            myFuture.complete(claim.record());
            cacheInRedisSafely(merchantId, trimmedKey, claim.record());
            return deserializeStoredResult(claim.record(), responseType, true);
        }

        if (claim.state() == ClaimState.IN_PROGRESS_BY_OTHER) {
            StoredIdempotencyRecord completed = waitForPeerCompletion(merchantId, trimmedKey, requestHash, compositeKey);
            return deserializeStoredResult(completed, responseType, true);
        }

        // We own the claim (CLAIMED or TAKEN_OVER): register our future for local waiting peers
        inFlightFutures.put(compositeKey, myFuture);
        try {
            T responseBody;
            if (wrapActionInOuterTx) {
                // Execute business handler + idempotency status='COMPLETED' update atomically in the SAME transaction (Section 5.2)
                responseBody = requiredTx.execute(txStatus -> {
                    T result = action.get();
                    String serializedResponse = serializeResponse(result);
                    StoredIdempotencyRecord completedRecord = completeClaim(
                            merchantId, trimmedKey, requestHash, successStatusCode, serializedResponse);

                    if (TransactionSynchronizationManager.isSynchronizationActive()) {
                        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                            @Override
                            public void afterCommit() {
                                cacheInRedisSafely(merchantId, trimmedKey, completedRecord);
                                myFuture.complete(completedRecord);
                                inFlightFutures.remove(compositeKey, myFuture);
                            }
                        });
                    } else {
                        cacheInRedisSafely(merchantId, trimmedKey, completedRecord);
                        myFuture.complete(completedRecord);
                        inFlightFutures.remove(compositeKey, myFuture);
                    }
                    return result;
                });
            } else {
                // Two-phase action (e.g. authorizePayment) manages its own Tx1 and Tx2 so zero DB connections are held during PSP I/O
                T result = action.get();
                String serializedResponse = serializeResponse(result);
                StoredIdempotencyRecord completedRecord = requiredTx.execute(txStatus ->
                        completeClaim(merchantId, trimmedKey, requestHash, successStatusCode, serializedResponse));
                cacheInRedisSafely(merchantId, trimmedKey, completedRecord);
                myFuture.complete(completedRecord);
                inFlightFutures.remove(compositeKey, myFuture);
                responseBody = result;
            }

            return new IdempotentResult<>(successStatusCode, responseBody, false);
        } catch (RuntimeException ex) {
            releaseInProgressClaimSafely(merchantId, trimmedKey);
            myFuture.completeExceptionally(ex);
            inFlightFutures.remove(compositeKey, myFuture);
            throw ex;
        }
    }

    public String computeRequestHash(String method, String path, Object requestBodyObj) {
        try {
            String canonicalBody = "";
            if (requestBodyObj != null) {
                if (requestBodyObj instanceof String rawStr) {
                    if (!rawStr.isBlank()) {
                        JsonNode tree = CANONICAL_MAPPER.readTree(rawStr);
                        canonicalBody = CANONICAL_MAPPER.writeValueAsString(tree);
                    }
                } else {
                    byte[] bytes = CANONICAL_MAPPER.writeValueAsBytes(requestBodyObj);
                    JsonNode tree = CANONICAL_MAPPER.readTree(bytes);
                    canonicalBody = CANONICAL_MAPPER.writeValueAsString(tree);
                }
            }
            String canonicalInput = method.toUpperCase() + "\n" + path + "\n" + canonicalBody;
            return ApiKeyHasher.sha256Hex(canonicalInput);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to canonicalize request for idempotency hashing", e);
        }
    }

    private ClaimOutcome claimInPostgres(UUID merchantId, String key, String requestHash) {
        return requiresNewTx.execute(status -> {
            Instant now = clockProvider.now();
            Timestamp nowTs = Timestamp.from(now);
            Timestamp lockedUntilTs = Timestamp.from(now.plus(LOCK_DURATION));

            int inserted = jdbcClient.sql("""
                    INSERT INTO idempotency_keys (
                        merchant_id, key, request_hash, status, response_code, response_body, locked_until, created_at
                    ) VALUES (
                        :merchantId, :key, :requestHash, 'IN_PROGRESS', NULL, NULL, :lockedUntil, :createdAt
                    )
                    ON CONFLICT (merchant_id, key) DO NOTHING
                    """)
                    .param("merchantId", merchantId)
                    .param("key", key)
                    .param("requestHash", requestHash)
                    .param("lockedUntil", lockedUntilTs)
                    .param("createdAt", nowTs)
                    .update();

            if (inserted == 1) {
                return new ClaimOutcome(ClaimState.CLAIMED, null);
            }

            StoredIdempotencyRecord existing = loadRecord(merchantId, key)
                    .orElseThrow(() -> PayCoreException.idempotencyInProgress(key));

            if (!existing.requestHash().equals(requestHash)) {
                throw PayCoreException.idempotencyPayloadMismatch(key);
            }

            if ("COMPLETED".equals(existing.status())) {
                return new ClaimOutcome(ClaimState.ALREADY_COMPLETED, existing);
            }

            // Status is IN_PROGRESS: check if lock expired for safe takeover (Section 5.2)
            if (!existing.lockedUntil().isAfter(now)) {
                int takenOver = jdbcClient.sql("""
                        UPDATE idempotency_keys
                        SET locked_until = :newLockedUntil
                        WHERE merchant_id = :merchantId
                          AND key = :key
                          AND status = 'IN_PROGRESS'
                          AND locked_until <= :now
                        """)
                        .param("newLockedUntil", lockedUntilTs)
                        .param("merchantId", merchantId)
                        .param("key", key)
                        .param("now", nowTs)
                        .update();
                if (takenOver == 1) {
                    return new ClaimOutcome(ClaimState.TAKEN_OVER, existing);
                }
            }

            return new ClaimOutcome(ClaimState.IN_PROGRESS_BY_OTHER, existing);
        });
    }

    private StoredIdempotencyRecord completeClaim(
            UUID merchantId, String key, String requestHash, int statusCode, String responseBody) {
        Instant now = clockProvider.now();
        jdbcClient.sql("""
                UPDATE idempotency_keys
                SET status = 'COMPLETED',
                    response_code = :responseCode,
                    response_body = :responseBody,
                    locked_until = :now
                WHERE merchant_id = :merchantId
                  AND key = :key
                """)
                .param("responseCode", statusCode)
                .param("responseBody", responseBody)
                .param("now", Timestamp.from(now))
                .param("merchantId", merchantId)
                .param("key", key)
                .update();

        return new StoredIdempotencyRecord(
                merchantId, key, requestHash, "COMPLETED", statusCode, responseBody, now);
    }

    private void releaseInProgressClaimSafely(UUID merchantId, String key) {
        try {
            requiresNewTx.executeWithoutResult(status -> jdbcClient.sql("""
                    DELETE FROM idempotency_keys
                    WHERE merchant_id = :merchantId
                      AND key = :key
                      AND status = 'IN_PROGRESS'
                    """)
                    .param("merchantId", merchantId)
                    .param("key", key)
                    .update());
        } catch (Exception e) {
            log.debug("Failed to release in-progress idempotency key {}: {}", key, e.getMessage());
        }
    }

    private StoredIdempotencyRecord waitForPeerCompletion(
            UUID merchantId, String key, String requestHash, String compositeKey) {
        CompletableFuture<StoredIdempotencyRecord> localFuture = inFlightFutures.get(compositeKey);
        if (localFuture != null) {
            try {
                StoredIdempotencyRecord rec = localFuture.get(pollTimeoutMs, TimeUnit.MILLISECONDS);
                if (!rec.requestHash().equals(requestHash)) {
                    throw PayCoreException.idempotencyPayloadMismatch(key);
                }
                return rec;
            } catch (PayCoreException e) {
                throw e;
            } catch (Exception ignored) {
                // Fall through to DB check if future failed or timed out
            }
        }

        long deadline = System.currentTimeMillis() + pollTimeoutMs;
        while (System.currentTimeMillis() < deadline) {
            Optional<StoredIdempotencyRecord> currentOpt = requiresNewTx.execute(s -> loadRecord(merchantId, key));
            if (currentOpt.isEmpty()) {
                throw PayCoreException.idempotencyInProgress(key);
            }
            StoredIdempotencyRecord current = currentOpt.get();
            if (!current.requestHash().equals(requestHash)) {
                throw PayCoreException.idempotencyPayloadMismatch(key);
            }
            if ("COMPLETED".equals(current.status())) {
                return current;
            }
            try {
                Thread.sleep(ThreadLocalRandom.current().nextInt(25, 75));
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw PayCoreException.idempotencyInProgress(key);
            }
        }
        throw PayCoreException.idempotencyInProgress(key);
    }

    private Optional<StoredIdempotencyRecord> loadRecord(UUID merchantId, String key) {
        return jdbcClient.sql("""
                SELECT merchant_id, key, request_hash, status, response_code, response_body, locked_until
                FROM idempotency_keys
                WHERE merchant_id = :merchantId AND key = :key
                """)
                .param("merchantId", merchantId)
                .param("key", key)
                .query((rs, rowNum) -> new StoredIdempotencyRecord(
                        rs.getObject("merchant_id", UUID.class),
                        rs.getString("key"),
                        rs.getString("request_hash"),
                        rs.getString("status"),
                        rs.getObject("response_code", Integer.class),
                        rs.getString("response_body"),
                        rs.getTimestamp("locked_until").toInstant()))
                .optional();
    }

    private Optional<StoredIdempotencyRecord> tryReadFromRedis(UUID merchantId, String key) {
        if (!redisEnabled || redisTemplate == null) {
            return Optional.empty();
        }
        try {
            String raw = redisTemplate.opsForValue().get(redisKey(merchantId, key));
            if (raw == null || raw.isBlank()) {
                return Optional.empty();
            }
            return Optional.of(objectMapper.readValue(raw, StoredIdempotencyRecord.class));
        } catch (Exception e) {
            // Section 5.2 & 5.3: On any miss or Redis error -> fall through to Postgres
            log.debug("Redis idempotency fast-path unavailable, falling through to Postgres: {}", e.getMessage());
            return Optional.empty();
        }
    }

    private void cacheInRedisSafely(UUID merchantId, String key, StoredIdempotencyRecord record) {
        if (!redisEnabled || redisTemplate == null || record == null) {
            return;
        }
        try {
            String json = objectMapper.writeValueAsString(record);
            redisTemplate.opsForValue().set(redisKey(merchantId, key), json, REDIS_TTL);
        } catch (Exception e) {
            log.debug("Redis idempotency cache write skipped: {}", e.getMessage());
        }
    }

    public void setRedisEnabled(boolean redisEnabled) {
        this.redisEnabled = redisEnabled;
    }

    public boolean isRedisEnabled() {
        return redisEnabled;
    }

    private static String redisKey(UUID merchantId, String key) {
        return "idem:" + merchantId + ":" + key;
    }

    private String serializeResponse(Object body) {
        try {
            return objectMapper.writeValueAsString(body);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize idempotent response body", e);
        }
    }

    private <T> IdempotentResult<T> deserializeStoredResult(
            StoredIdempotencyRecord record, Class<T> responseType, boolean replayed) {
        try {
            T body = objectMapper.readValue(record.responseBody(), responseType);
            int code = record.responseCode() != null ? record.responseCode() : 200;
            return new IdempotentResult<>(code, body, replayed);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to deserialize stored idempotent response", e);
        }
    }

    private enum ClaimState {
        CLAIMED,
        TAKEN_OVER,
        ALREADY_COMPLETED,
        IN_PROGRESS_BY_OTHER
    }

    private record ClaimOutcome(ClaimState state, StoredIdempotencyRecord record) {}

    public record StoredIdempotencyRecord(
            UUID merchantId,
            String key,
            String requestHash,
            String status,
            Integer responseCode,
            String responseBody,
            Instant lockedUntil) {}

    public record IdempotentResult<T>(int statusCode, T body, boolean replayed) {}
}
