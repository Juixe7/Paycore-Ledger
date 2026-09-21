package com.paycore.payments.psp;

import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

/**
 * Resilience4j-guarded PSP client (Section 6 Resilience & Section 4.3).
 *
 * <p>Supports both HTTP calls to {@code mock-psp} and deterministic in-process fault injection
 * for unit/integration/chaos tests (timeouts, 500s, declines, unknown outcomes resolved via lookup).
 */
@Component
public class ResilientPspClient implements PspClient {

    private static final Logger log = LoggerFactory.getLogger(ResilientPspClient.class);

    private final RestClient restClient;
    private final boolean httpEnabled;
    private final CircuitBreaker circuitBreaker;
    private final Bulkhead bulkhead;

    // In-memory ledger of PSP authorizations for embedded/fault-injection testing and idempotent PSP dedupe
    private final Map<String, PspStatusLookupResult> pspBook = new ConcurrentHashMap<>();
    private final Map<UUID, FaultScript> faultScriptsByPaymentId = new ConcurrentHashMap<>();
    private volatile FaultMode globalFaultMode = FaultMode.NONE;
    private volatile long globalLatencyMs = 0L;

    public ResilientPspClient(
            @Value("${paycore.psp.base-url:}") String pspBaseUrl,
            @Value("${paycore.psp.timeout-ms:1500}") int timeoutMs) {
        this.httpEnabled = pspBaseUrl != null && !pspBaseUrl.isBlank();
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(timeoutMs);
        factory.setReadTimeout(timeoutMs);
        this.restClient = httpEnabled
                ? RestClient.builder().baseUrl(pspBaseUrl.trim()).requestFactory(factory).build()
                : null;

        this.circuitBreaker = CircuitBreaker.of(
                "pspClient",
                CircuitBreakerConfig.custom()
                        .failureRateThreshold(50.0f)
                        .minimumNumberOfCalls(20)
                        .waitDurationInOpenState(Duration.ofSeconds(5))
                        .slidingWindowSize(40)
                        .build());

        this.bulkhead = Bulkhead.of(
                "pspClient",
                BulkheadConfig.custom()
                        .maxConcurrentCalls(256)
                        .maxWaitDuration(Duration.ofMillis(200))
                        .build());
    }

    @Override
    public PspAuthResult authorize(PspAuthRequest request) {
        long startNs = System.nanoTime();
        try {
            return Bulkhead.decorateSupplier(
                            bulkhead,
                            CircuitBreaker.decorateSupplier(circuitBreaker, () -> executeAuthorize(request)))
                    .get();
        } catch (Exception ex) {
            long latencyMs = Duration.ofNanos(System.nanoTime() - startNs).toMillis();
            log.warn("PSP call timed out or failed for pspRef={} (leaving payment AUTHORIZING for resolver): {}",
                    request.pspRef(), ex.getMessage());
            return new PspAuthResult(PspOutcome.UNKNOWN_TIMEOUT, request.pspRef(), latencyMs, ex.getMessage());
        }
    }

    @Override
    public PspCaptureResult capture(PspCaptureRequest request) {
        long startNs = System.nanoTime();
        String ref = request.pspRef() != null ? request.pspRef() : request.paymentId() + ":1";
        if (httpEnabled && restClient != null) {
            try {
                restClient.post()
                        .uri("/psp/v1/captures/{pspRef}", ref)
                        .body(Map.of(
                                "paymentId", request.paymentId().toString(),
                                "amountMinor", request.amountMinor(),
                                "currency", request.currency()))
                        .retrieve()
                        .toBodilessEntity();
            } catch (Exception ex) {
                log.warn("PSP capture HTTP call warning for pspRef={}: {}", ref, ex.getMessage());
            }
        }
        pspBook.put(ref, new PspStatusLookupResult(PspLookupState.AUTHORIZED, ref, request.amountMinor()));
        long latencyMs = Duration.ofNanos(System.nanoTime() - startNs).toMillis();
        return new PspCaptureResult(PspOutcome.AUTHORIZED, ref, latencyMs, "CAPTURED");
    }

    @Override
    public PspRefundResult refund(PspRefundRequest request) {
        long startNs = System.nanoTime();
        String ref = request.pspRef() != null ? request.pspRef() : request.paymentId() + ":1";
        if (httpEnabled && restClient != null) {
            try {
                restClient.post()
                        .uri("/psp/v1/refunds/{pspRef}", ref)
                        .body(Map.of(
                                "paymentId", request.paymentId().toString(),
                                "refundAmountMinor", request.refundAmountMinor(),
                                "currency", request.currency()))
                        .retrieve()
                        .toBodilessEntity();
            } catch (Exception ex) {
                log.warn("PSP refund HTTP call warning for pspRef={}: {}", ref, ex.getMessage());
            }
        }
        pspBook.computeIfPresent(ref, (k, existing) -> new PspStatusLookupResult(
                existing.state(),
                ref,
                Math.max(0L, existing.amountMinor() - request.refundAmountMinor())));
        long latencyMs = Duration.ofNanos(System.nanoTime() - startNs).toMillis();
        return new PspRefundResult(PspOutcome.AUTHORIZED, ref, latencyMs, "REFUNDED");
    }

    @Override
    public PspStatusLookupResult lookupByPspRef(String pspRef) {
        if (httpEnabled && restClient != null) {
            try {
                PspHttpLookupResponse resp = restClient.get()
                        .uri("/psp/v1/authorizations/{pspRef}", pspRef)
                        .retrieve()
                        .body(PspHttpLookupResponse.class);
                if (resp == null || "NOT_FOUND".equals(resp.status())) {
                    return new PspStatusLookupResult(PspLookupState.NOT_FOUND, pspRef, 0L);
                }
                PspLookupState state = ("AUTHORIZED".equals(resp.status()) || "CAPTURED".equals(resp.status()))
                        ? PspLookupState.AUTHORIZED
                        : PspLookupState.DECLINED;
                return new PspStatusLookupResult(state, pspRef, resp.amountMinor());
            } catch (Exception e) {
                log.warn("PSP lookup failed for pspRef={}: {}", pspRef, e.getMessage());
                return new PspStatusLookupResult(PspLookupState.LOOKUP_ERROR, pspRef, 0L);
            }
        }

        return pspBook.getOrDefault(
                pspRef, new PspStatusLookupResult(PspLookupState.NOT_FOUND, pspRef, 0L));
    }

    private PspAuthResult executeAuthorize(PspAuthRequest request) {
        long startNs = System.nanoTime();
        FaultScript script = faultScriptsByPaymentId.get(request.paymentId());
        FaultMode mode = script != null ? script.mode() : globalFaultMode;

        long delayMs = (script != null && script.latencyMs() > 0) ? script.latencyMs() : globalLatencyMs;
        if (delayMs > 0) {
            sleepQuietly(delayMs);
        }

        if (mode == FaultMode.TIMEOUT_AFTER_PSP_CHARGE) {
            // Classic unknown-outcome scenario: PSP charges the card and records AUTHORIZED,
            // but the network drops / times out before the HTTP response reaches payments-core!
            pspBook.put(
                    request.pspRef(),
                    new PspStatusLookupResult(PspLookupState.AUTHORIZED, request.pspRef(), request.amountMinor()));
            throw new RuntimeException("Simulated PSP read timeout after acquirer charge committed");
        }

        if (mode == FaultMode.TIMEOUT_BEFORE_PSP_CHARGE) {
            // Network drops before PSP receives request; PSP has NOT_FOUND for this pspRef
            throw new RuntimeException("Simulated PSP connection timeout before acquirer charge");
        }

        if (mode == FaultMode.HTTP_500) {
            throw new RuntimeException("Simulated PSP HTTP 500 Internal Server Error");
        }

        if (mode == FaultMode.DECLINE) {
            pspBook.put(
                    request.pspRef(),
                    new PspStatusLookupResult(PspLookupState.DECLINED, request.pspRef(), request.amountMinor()));
            long latencyMs = Duration.ofNanos(System.nanoTime() - startNs).toMillis();
            return new PspAuthResult(PspOutcome.DECLINED, request.pspRef(), latencyMs, "INSUFFICIENT_FUNDS");
        }

        if (httpEnabled && restClient != null) {
            PspHttpAuthResponse httpResp = restClient.post()
                    .uri("/psp/v1/authorizations")
                    .body(new PspHttpAuthRequest(
                            request.paymentId().toString(),
                            request.merchantId().toString(),
                            request.pspRef(),
                            request.amountMinor(),
                            request.currency()))
                    .retrieve()
                    .body(PspHttpAuthResponse.class);
            long latencyMs = Duration.ofNanos(System.nanoTime() - startNs).toMillis();
            if (httpResp != null && "AUTHORIZED".equals(httpResp.status())) {
                return new PspAuthResult(PspOutcome.AUTHORIZED, request.pspRef(), latencyMs, "OK");
            }
            return new PspAuthResult(
                    PspOutcome.DECLINED,
                    request.pspRef(),
                    latencyMs,
                    httpResp != null ? httpResp.reason() : "DECLINED");
        }

        // Idempotent PSP authorization in embedded mode
        PspStatusLookupResult recorded = pspBook.computeIfAbsent(
                request.pspRef(),
                ref -> new PspStatusLookupResult(PspLookupState.AUTHORIZED, ref, request.amountMinor()));

        long latencyMs = Duration.ofNanos(System.nanoTime() - startNs).toMillis();
        PspOutcome outcome = recorded.state() == PspLookupState.AUTHORIZED
                ? PspOutcome.AUTHORIZED
                : PspOutcome.DECLINED;
        return new PspAuthResult(outcome, request.pspRef(), latencyMs, "OK");
    }

    public void scriptFaultForPayment(UUID paymentId, FaultMode mode, long latencyMs) {
        faultScriptsByPaymentId.put(paymentId, new FaultScript(mode, latencyMs));
    }

    public void setGlobalFaultMode(FaultMode mode) {
        this.globalFaultMode = mode;
    }

    public void setSimulatedLatencyMs(long latencyMs) {
        this.globalLatencyMs = Math.max(0L, latencyMs);
    }

    public void clearFaults() {
        faultScriptsByPaymentId.clear();
        globalFaultMode = FaultMode.NONE;
        globalLatencyMs = 0L;
        circuitBreaker.reset();
    }

    private void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    public enum FaultMode {
        NONE,
        DECLINE,
        HTTP_500,
        TIMEOUT_AFTER_PSP_CHARGE,
        TIMEOUT_BEFORE_PSP_CHARGE
    }

    public record FaultScript(FaultMode mode, long latencyMs) {}

    record PspHttpAuthRequest(
            String paymentId, String merchantId, String pspRef, long amountMinor, String currency) {}

    record PspHttpAuthResponse(String pspRef, String status, String reason, long amountMinor) {}

    record PspHttpLookupResponse(String pspRef, String status, long amountMinor) {}
}
