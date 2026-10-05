package com.paycore.reconciliation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.paycore.common.clock.ClockProvider;
import com.paycore.common.ids.UuidV7;
import java.io.BufferedReader;
import java.io.StringReader;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Settlement Reconciliation Engine (FR12, Section 5.1, Section 7.10).
 *
 * <p>Ingests a PSP daily settlement CSV ({@code psp_ref,amount,status,settled_at}) and compares
 * every row against PayCore's {@code payments} and {@code ledger_transactions} tables,
 * flagging all 4 discrepancy kinds:
 * <ul>
 *   <li>{@code MISSING_IN_LEDGER}</li>
 *   <li>{@code MISSING_IN_PSP_FILE}</li>
 *   <li>{@code AMOUNT_MISMATCH}</li>
 *   <li>{@code STATUS_MISMATCH}</li>
 * </ul>
 */
@Service
public class ReconciliationService {

    public enum DiscrepancyKind {
        MISSING_IN_LEDGER,
        MISSING_IN_PSP_FILE,
        AMOUNT_MISMATCH,
        STATUS_MISMATCH
    }

    private final JdbcClient jdbcClient;
    private final ClockProvider clockProvider;
    private final ObjectMapper objectMapper;

    public ReconciliationService(
            JdbcClient jdbcClient, ClockProvider clockProvider, ObjectMapper objectMapper) {
        this.jdbcClient = jdbcClient;
        this.clockProvider = clockProvider;
        this.objectMapper = objectMapper;
    }

    @Transactional(isolation = Isolation.READ_COMMITTED)
    public ReconRunReport runReconciliation(LocalDate settlementDate, String csvContent) {
        Map<String, PspSettlementRow> csvRowsByPspRef = parseSettlementCsv(csvContent);
        Map<String, InternalPaymentRecord> internalByPspRef = loadInternalPayments(settlementDate);

        UUID runId = UuidV7.generate();
        Instant now = clockProvider.now();
        List<DiscrepancyRecord> discrepancies = new ArrayList<>();
        int matched = 0;

        // 1. Check every row in the PSP settlement CSV against internal payments + ledger
        for (PspSettlementRow pspRow : csvRowsByPspRef.values()) {
            InternalPaymentRecord internal = internalByPspRef.get(pspRow.pspRef());
            if (internal == null || ("SETTLED".equalsIgnoreCase(pspRow.status()) && !internal.hasCaptureLedgerTxn())) {
                // Present in PSP settlement file, but missing in payments or missing CAPTURE ledger transaction
                discrepancies.add(new DiscrepancyRecord(
                        UuidV7.generate(),
                        runId,
                        pspRow.pspRef(),
                        internal != null ? internal.paymentId() : null,
                        DiscrepancyKind.MISSING_IN_LEDGER,
                        null,
                        pspRow.amount(),
                        Map.of(
                                "pspStatus", pspRow.status(),
                                "settledAt", pspRow.settledAt(),
                                "reason", internal == null
                                        ? "psp_ref not found in payments table"
                                        : "payment has no CAPTURE ledger transaction")));
                continue;
            }

            // Verify status compatibility first
            if (!isStatusCompatible(pspRow.status(), internal.status())) {
                discrepancies.add(new DiscrepancyRecord(
                        UuidV7.generate(),
                        runId,
                        pspRow.pspRef(),
                        internal.paymentId(),
                        DiscrepancyKind.STATUS_MISMATCH,
                        internal.expectedSettlementAmount(),
                        pspRow.amount(),
                        Map.of(
                                "expectedStatus", internal.status(),
                                "actualPspStatus", pspRow.status(),
                                "settledAt", pspRow.settledAt())));
                continue;
            }

            // Verify amount
            long expectedAmt = internal.expectedSettlementAmount();
            if (expectedAmt != pspRow.amount()) {
                discrepancies.add(new DiscrepancyRecord(
                        UuidV7.generate(),
                        runId,
                        pspRow.pspRef(),
                        internal.paymentId(),
                        DiscrepancyKind.AMOUNT_MISMATCH,
                        expectedAmt,
                        pspRow.amount(),
                        Map.of(
                                "status", internal.status(),
                                "pspStatus", pspRow.status(),
                                "deltaPaise", pspRow.amount() - expectedAmt)));
                continue;
            }

            matched++;
        }

        // 2. Check every internal CAPTURED / PARTIALLY_REFUNDED / REFUNDED payment (with a psp_ref) to see if it is missing from the PSP CSV
        for (InternalPaymentRecord internal : internalByPspRef.values()) {
            if (("CAPTURED".equals(internal.status())
                            || "PARTIALLY_REFUNDED".equals(internal.status())
                            || "REFUNDED".equals(internal.status()))
                    && !csvRowsByPspRef.containsKey(internal.pspRef())) {
                discrepancies.add(new DiscrepancyRecord(
                        UuidV7.generate(),
                        runId,
                        internal.pspRef(),
                        internal.paymentId(),
                        DiscrepancyKind.MISSING_IN_PSP_FILE,
                        internal.expectedSettlementAmount(),
                        null,
                        Map.of(
                                "internalStatus", internal.status(),
                                "reason", "CAPTURED payment in ledger missing from PSP settlement CSV")));
            }
        }

        // Persist recon_runs header
        jdbcClient.sql("""
                INSERT INTO recon_runs (id, settlement_date, matched, discrepancies, created_at)
                VALUES (:id, :settlementDate, :matched, :discrepancies, :createdAt)
                """)
                .param("id", runId)
                .param("settlementDate", Date.valueOf(settlementDate))
                .param("matched", matched)
                .param("discrepancies", discrepancies.size())
                .param("createdAt", Timestamp.from(now))
                .update();

        // Persist recon_discrepancies rows
        for (DiscrepancyRecord d : discrepancies) {
            String detailsJson;
            try {
                detailsJson = objectMapper.writeValueAsString(d.details());
            } catch (Exception e) {
                detailsJson = "{}";
            }
            jdbcClient.sql("""
                    INSERT INTO recon_discrepancies (
                        id, run_id, psp_ref, payment_id, kind, expected_amt, actual_amt, details
                    ) VALUES (
                        :id, :runId, :pspRef, :paymentId, :kind, :expectedAmt, :actualAmt, CAST(:details AS jsonb)
                    )
                    """)
                    .param("id", d.id())
                    .param("runId", d.runId())
                    .param("pspRef", d.pspRef())
                    .param("paymentId", d.paymentId())
                    .param("kind", d.kind().name())
                    .param("expectedAmt", d.expectedAmt())
                    .param("actualAmt", d.actualAmt())
                    .param("details", detailsJson)
                    .update();
        }

        return new ReconRunReport(
                runId,
                settlementDate,
                matched,
                discrepancies.size(),
                now,
                discrepancies);
    }

    @Transactional(readOnly = true)
    public List<DiscrepancyRecord> getDiscrepanciesForRun(UUID runId) {
        return jdbcClient.sql("""
                SELECT id, run_id, psp_ref, payment_id, kind, expected_amt, actual_amt, details::text AS details_text
                FROM recon_discrepancies
                WHERE run_id = :runId
                ORDER BY id ASC
                """)
                .param("runId", runId)
                .query((rs, rowNum) -> {
                    Long expected = rs.getLong("expected_amt");
                    if (rs.wasNull()) expected = null;
                    Long actual = rs.getLong("actual_amt");
                    if (rs.wasNull()) actual = null;
                    Map<String, Object> detailsMap = Map.of();
                    try {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> parsed = objectMapper.readValue(rs.getString("details_text"), Map.class);
                        detailsMap = parsed;
                    } catch (Exception ignored) {
                    }
                    return new DiscrepancyRecord(
                            rs.getObject("id", UUID.class),
                            rs.getObject("run_id", UUID.class),
                            rs.getString("psp_ref"),
                            rs.getObject("payment_id", UUID.class),
                            DiscrepancyKind.valueOf(rs.getString("kind")),
                            expected,
                            actual,
                            detailsMap);
                })
                .list();
    }

    private boolean isStatusCompatible(String pspStatus, String internalStatus) {
        String psp = pspStatus.trim().toUpperCase();
        String internal = internalStatus.trim().toUpperCase();
        return switch (psp) {
            case "SETTLED", "CAPTURED" -> "CAPTURED".equals(internal)
                    || "PARTIALLY_REFUNDED".equals(internal)
                    || "REFUNDED".equals(internal);
            case "AUTHORIZED" -> "AUTHORIZED".equals(internal);
            case "VOIDED" -> "VOIDED".equals(internal);
            case "FAILED" -> "FAILED".equals(internal);
            default -> psp.equals(internal);
        };
    }

    private Map<String, PspSettlementRow> parseSettlementCsv(String csvContent) {
        Map<String, PspSettlementRow> map = new LinkedHashMap<>();
        if (csvContent == null || csvContent.isBlank()) {
            return map;
        }
        try (BufferedReader reader = new BufferedReader(new StringReader(csvContent.trim()))) {
            String line;
            boolean firstLine = true;
            while ((line = reader.readLine()) != null) {
                String trimmed = line.trim();
                if (trimmed.isEmpty()) {
                    continue;
                }
                if (firstLine && trimmed.toLowerCase().startsWith("psp_ref")) {
                    firstLine = false;
                    continue;
                }
                firstLine = false;
                String[] parts = trimmed.split(",", -1);
                if (parts.length < 4) {
                    throw new IllegalArgumentException("Invalid CSV line (expected psp_ref,amount,status,settled_at): " + trimmed);
                }
                String pspRef = parts[0].trim();
                long amount = Long.parseLong(parts[1].trim());
                String status = parts[2].trim();
                String settledAt = parts[3].trim();
                map.put(pspRef, new PspSettlementRow(pspRef, amount, status, settledAt));
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse settlement CSV: " + e.getMessage(), e);
        }
        return map;
    }

    private Map<String, InternalPaymentRecord> loadInternalPayments(LocalDate settlementDate) {
        Instant startOfDay = settlementDate.atStartOfDay(java.time.ZoneOffset.UTC).toInstant();
        Instant endOfDay = settlementDate.plusDays(1).atStartOfDay(java.time.ZoneOffset.UTC).toInstant();
        List<InternalPaymentRecord> records = jdbcClient.sql("""
                SELECT
                    p.id AS payment_id,
                    p.psp_ref,
                    p.status,
                    p.amount,
                    p.captured_amount,
                    p.refunded_amount,
                    EXISTS (
                        SELECT 1 FROM ledger_transactions lt
                        WHERE lt.payment_id = p.id AND lt.kind = 'CAPTURE'
                    ) AS has_capture_txn
                FROM payments p
                WHERE p.psp_ref IS NOT NULL
                  AND p.updated_at >= :startOfDay
                  AND p.updated_at < :endOfDay
                """)
                .param("startOfDay", Timestamp.from(startOfDay))
                .param("endOfDay", Timestamp.from(endOfDay))
                .query((rs, rowNum) -> new InternalPaymentRecord(
                        rs.getObject("payment_id", UUID.class),
                        rs.getString("psp_ref"),
                        rs.getString("status"),
                        rs.getLong("amount"),
                        rs.getLong("captured_amount"),
                        rs.getLong("refunded_amount"),
                        rs.getBoolean("has_capture_txn")))
                .list();

        Map<String, InternalPaymentRecord> byPspRef = new LinkedHashMap<>();
        for (InternalPaymentRecord r : records) {
            byPspRef.put(r.pspRef(), r);
        }
        return byPspRef;
    }

    public record PspSettlementRow(String pspRef, long amount, String status, String settledAt) {}

    public record InternalPaymentRecord(
            UUID paymentId,
            String pspRef,
            String status,
            long amount,
            long capturedAmount,
            long refundedAmount,
            boolean hasCaptureLedgerTxn) {
        public long expectedSettlementAmount() {
            if (("CAPTURED".equalsIgnoreCase(status)
                            || "PARTIALLY_REFUNDED".equalsIgnoreCase(status)
                            || "REFUNDED".equalsIgnoreCase(status))
                    && capturedAmount > 0) {
                return capturedAmount - refundedAmount;
            }
            return amount;
        }
    }

    public record DiscrepancyRecord(
            UUID id,
            UUID runId,
            String pspRef,
            UUID paymentId,
            DiscrepancyKind kind,
            Long expectedAmt,
            Long actualAmt,
            Map<String, Object> details) {}

    public record ReconRunReport(
            UUID runId,
            LocalDate settlementDate,
            int matched,
            int discrepancyCount,
            Instant createdAt,
            List<DiscrepancyRecord> discrepancies) {}
}
