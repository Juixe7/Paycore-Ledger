package com.paycore.mockpsp;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Simulated Acquirer / PSP Service with configurable latency, failure rates, timeouts,
 * idempotent psp_ref deduplication, and settlement CSV export (FR8 & Section 5.5).
 */
@RestController
public class MockPspController {

    private final Map<String, PspRecord> recordsByRef = new ConcurrentHashMap<>();
    private volatile FaultConfig faultConfig = new FaultConfig(0L, 0.0, 0.0, 0.0);

    @PostMapping("/psp/v1/authorizations")
    public ResponseEntity<AuthResponse> authorize(@RequestBody AuthRequest request) {
        FaultConfig cfg = this.faultConfig;
        if (cfg.latencyMs() > 0) {
            sleepQuietly(cfg.latencyMs());
        }

        double roll = ThreadLocalRandom.current().nextDouble();
        if (roll < cfg.timeoutAfterChargeRate()) {
            // Unknown outcome simulation: record AUTHORIZED in PSP ledger, then sleep past client read timeout
            recordsByRef.computeIfAbsent(
                    request.pspRef(),
                    ref -> new PspRecord(ref, request.paymentId(), request.amountMinor(), "AUTHORIZED", Instant.now()));
            sleepQuietly(3000L);
            return ResponseEntity.status(HttpStatus.GATEWAY_TIMEOUT).build();
        }

        if (roll < cfg.timeoutAfterChargeRate() + cfg.http500Rate()) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(new AuthResponse(request.pspRef(), "ERROR", "PSP_INTERNAL_ERROR", request.amountMinor()));
        }

        PspRecord record = recordsByRef.computeIfAbsent(request.pspRef(), ref -> {
            boolean declined = ThreadLocalRandom.current().nextDouble() < cfg.declineRate();
            String status = declined ? "DECLINED" : "AUTHORIZED";
            return new PspRecord(ref, request.paymentId(), request.amountMinor(), status, Instant.now());
        });

        return ResponseEntity.ok(new AuthResponse(
                record.pspRef(),
                record.status(),
                "AUTHORIZED".equals(record.status()) ? "OK" : "INSUFFICIENT_FUNDS",
                record.amountMinor()));
    }

    @GetMapping("/psp/v1/authorizations/{pspRef}")
    public ResponseEntity<LookupResponse> lookup(@PathVariable("pspRef") String pspRef) {
        PspRecord record = recordsByRef.get(pspRef);
        if (record == null) {
            return ResponseEntity.ok(new LookupResponse(pspRef, "NOT_FOUND", 0L));
        }
        return ResponseEntity.ok(new LookupResponse(record.pspRef(), record.status(), record.amountMinor()));
    }

    @PostMapping("/psp/v1/captures/{pspRef}")
    public ResponseEntity<AuthResponse> capture(
            @PathVariable("pspRef") String pspRef,
            @RequestBody(required = false) CaptureRequest request) {
        long reqAmount = request != null && request.amountMinor() > 0 ? request.amountMinor() : 0L;
        String payId = request != null && request.paymentId() != null ? request.paymentId() : pspRef;
        PspRecord updated = recordsByRef.compute(pspRef, (ref, existing) -> {
            long finalAmount = existing != null ? existing.amountMinor() : reqAmount;
            return new PspRecord(ref, existing != null ? existing.paymentId() : payId, finalAmount, "CAPTURED", Instant.now());
        });
        return ResponseEntity.ok(new AuthResponse(updated.pspRef(), updated.status(), "OK", updated.amountMinor()));
    }

    @PostMapping("/psp/v1/refunds/{pspRef}")
    public ResponseEntity<AuthResponse> refund(
            @PathVariable("pspRef") String pspRef,
            @RequestBody RefundPspRequest request) {
        long refundAmt = request != null ? Math.max(0L, request.refundAmountMinor()) : 0L;
        PspRecord updated = recordsByRef.computeIfPresent(pspRef, (ref, existing) -> {
            long netAmount = Math.max(0L, existing.amountMinor() - refundAmt);
            return new PspRecord(ref, existing.paymentId(), netAmount, "CAPTURED", existing.settledAt());
        });
        if (updated == null) {
            return ResponseEntity.ok(new AuthResponse(pspRef, "NOT_FOUND", "UNKNOWN_PSP_REF", 0L));
        }
        return ResponseEntity.ok(new AuthResponse(updated.pspRef(), updated.status(), "OK", updated.amountMinor()));
    }

    @PostMapping("/psp/v1/admin/fault-config")
    public ResponseEntity<FaultConfig> updateFaultConfig(@RequestBody FaultConfig config) {
        this.faultConfig = config;
        return ResponseEntity.ok(this.faultConfig);
    }

    @GetMapping(value = "/psp/v1/settlement.csv", produces = "text/csv")
    public ResponseEntity<String> exportSettlementCsv(
            @RequestParam(value = "injectDiscrepancies", defaultValue = "false") boolean injectDiscrepancies) {
        StringBuilder csv = new StringBuilder("psp_ref,amount,status,settled_at\n");
        int index = 0;
        for (PspRecord rec : recordsByRef.values()) {
            if (!"CAPTURED".equals(rec.status()) && !"AUTHORIZED".equals(rec.status())) {
                continue;
            }
            index++;
            if (injectDiscrepancies && index == 1) {
                // Inject AMOUNT_MISMATCH on row 1
                csv.append(rec.pspRef()).append(",")
                        .append(rec.amountMinor() + 500L).append(",SETTLED,")
                        .append(rec.settledAt()).append("\n");
            } else if (injectDiscrepancies && index == 2) {
                // Inject MISSING_IN_PSP_FILE by skipping row 2
                continue;
            } else {
                csv.append(rec.pspRef()).append(",")
                        .append(rec.amountMinor()).append(",SETTLED,")
                        .append(rec.settledAt()).append("\n");
            }
        }
        if (injectDiscrepancies) {
            // Inject MISSING_IN_LEDGER phantom row
            csv.append("phantom-psp-ref:1,99900,SETTLED,").append(Instant.now()).append("\n");
        }
        return ResponseEntity.ok()
                .contentType(MediaType.parseMediaType("text/csv"))
                .body(csv.toString());
    }

    @GetMapping("/healthz")
    public ResponseEntity<Map<String, String>> healthz() {
        return ResponseEntity.ok(Map.of("status", "UP", "service", "mock-psp"));
    }

    @GetMapping("/readyz")
    public ResponseEntity<Map<String, String>> readyz() {
        return ResponseEntity.ok(Map.of("status", "READY", "service", "mock-psp"));
    }

    private void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    public record AuthRequest(
            String paymentId, String merchantId, String pspRef, long amountMinor, String currency) {}

    public record CaptureRequest(String paymentId, long amountMinor, String currency) {}

    public record RefundPspRequest(String paymentId, long refundAmountMinor, String currency) {}

    public record AuthResponse(String pspRef, String status, String reason, long amountMinor) {}

    public record LookupResponse(String pspRef, String status, long amountMinor) {}

    public record FaultConfig(
            long latencyMs,
            double http500Rate,
            double declineRate,
            double timeoutAfterChargeRate) {}

    public record PspRecord(
            String pspRef, String paymentId, long amountMinor, String status, Instant settledAt) {}
}
