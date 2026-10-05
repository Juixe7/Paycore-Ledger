package com.paycore.reconciliation;

import com.paycore.reconciliation.ReconciliationService.DiscrepancyRecord;
import com.paycore.reconciliation.ReconciliationService.ReconRunReport;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST API for Settlement Reconciliation (Section 6.1):
 * <ul>
 *   <li>{@code POST /v1/admin/reconciliation} (accepts JSON or raw text/csv)</li>
 *   <li>{@code GET /v1/admin/reconciliation/{runId}/discrepancies}</li>
 * </ul>
 */
@RestController
public class ReconciliationController {

    private final ReconciliationService reconciliationService;

    public ReconciliationController(ReconciliationService reconciliationService) {
        this.reconciliationService = reconciliationService;
    }

    @PostMapping(value = "/v1/admin/reconciliation", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<ReconRunReport> runReconciliationJson(
            @RequestBody ReconciliationRequest request) {
        LocalDate date = request.settlementDate() != null ? request.settlementDate() : LocalDate.now();
        return ResponseEntity.ok(reconciliationService.runReconciliation(date, request.csvContent()));
    }

    @PostMapping(value = "/v1/admin/reconciliation", consumes = {"text/csv", MediaType.TEXT_PLAIN_VALUE})
    public ResponseEntity<ReconRunReport> runReconciliationCsv(
            @RequestParam(value = "settlementDate", required = false) LocalDate settlementDate,
            @RequestBody String csvContent) {
        LocalDate date = settlementDate != null ? settlementDate : LocalDate.now();
        return ResponseEntity.ok(reconciliationService.runReconciliation(date, csvContent));
    }

    @GetMapping("/v1/admin/reconciliation/{runId}/discrepancies")
    public ResponseEntity<List<DiscrepancyRecord>> getDiscrepancies(@PathVariable UUID runId) {
        return ResponseEntity.ok(reconciliationService.getDiscrepanciesForRun(runId));
    }

    @GetMapping("/healthz")
    public ResponseEntity<Map<String, String>> healthz() {
        return ResponseEntity.ok(Map.of("status", "UP", "service", "reconciliation"));
    }

    @GetMapping("/readyz")
    public ResponseEntity<Map<String, String>> readyz() {
        return ResponseEntity.ok(Map.of("status", "READY", "service", "reconciliation"));
    }

    public record ReconciliationRequest(LocalDate settlementDate, String csvContent) {}
}
