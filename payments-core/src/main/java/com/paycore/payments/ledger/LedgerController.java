package com.paycore.payments.ledger;

import com.paycore.common.errors.PayCoreException;
import jakarta.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST endpoints for ledger entries, merchant balance, and the ledger invariant checker (Section 5.6).
 * Kept inside {@code com.paycore.payments.ledger} so that ledger queries remain strictly encapsulated.
 */
@RestController
class LedgerController {

    static final String MERCHANT_ID_ATTR = "paycore.merchantId";

    private final LedgerService ledgerService;
    private final LedgerInvariantChecker invariantChecker;

    LedgerController(LedgerService ledgerService, LedgerInvariantChecker invariantChecker) {
        this.ledgerService = ledgerService;
        this.invariantChecker = invariantChecker;
    }

    @GetMapping("/v1/payments/{id}/ledger")
    ResponseEntity<List<LedgerService.LedgerEntryView>> getPaymentLedgerEntries(
            @PathVariable("id") UUID paymentId) {
        return ResponseEntity.ok(ledgerService.findEntriesForPayment(paymentId));
    }

    @GetMapping("/v1/merchants/me/balance")
    ResponseEntity<LedgerService.MerchantBalanceView> getMerchantBalance(HttpServletRequest request) {
        UUID merchantId = (UUID) request.getAttribute(MERCHANT_ID_ATTR);
        if (merchantId == null) {
            throw PayCoreException.unauthorized("Missing authenticated merchant context");
        }
        return ResponseEntity.ok(ledgerService.getMerchantBalance(merchantId));
    }

    @GetMapping("/v1/admin/invariants")
    ResponseEntity<LedgerInvariantChecker.InvariantReport> checkInvariants() {
        LedgerInvariantChecker.InvariantReport report = invariantChecker.checkInvariants();
        return ResponseEntity.status(report.healthy() ? 200 : 500).body(report);
    }

    @org.springframework.web.bind.annotation.PostMapping("/v1/admin/ledger/sharding")
    ResponseEntity<LedgerService.SystemBalancesView> toggleSharding(
            @org.springframework.web.bind.annotation.RequestParam("enabled") boolean enabled) {
        ledgerService.setShardedFeeAccounts(enabled);
        return ResponseEntity.ok(ledgerService.getSystemBalancesSummary());
    }

    @GetMapping("/v1/admin/ledger/system-balances")
    ResponseEntity<LedgerService.SystemBalancesView> getSystemBalances() {
        return ResponseEntity.ok(ledgerService.getSystemBalancesSummary());
    }
}
