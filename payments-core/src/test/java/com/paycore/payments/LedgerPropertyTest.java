package com.paycore.payments;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.paycore.common.errors.PayCoreException;
import com.paycore.payments.merchant.MerchantService;
import com.paycore.payments.payment.Payment;
import com.paycore.payments.payment.PaymentService;
import com.paycore.payments.psp.ResilientPspClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.lifecycle.BeforeTry;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Property-Based Ledger Testing with jqwik (Section 7.3).
 *
 * <p>Generates random sequences of CREATE, AUTHORIZE, CAPTURE, PARTIAL_REFUND, VOID, PAYOUT,
 * and PSP_FAULT operations across multiple merchants. After every randomized sequence, asserts:
 * <ul>
 *   <li>Every transaction balances (SUM(debit) == SUM(credit))</li>
 *   <li>Global debits equal global credits</li>
 *   <li>No negative MERCHANT_PAYABLE balance</li>
 *   <li>account_balances equal the sum of ledger_entries for every account (drift == 0)</li>
 * </ul>
 */
class LedgerPropertyTest {

    private static PostgreSQLContainer<?> postgres;
    private static ConfigurableApplicationContext context;
    private static MerchantService merchantService;
    private static PaymentService paymentService;
    private static ResilientPspClient pspClient;
    private static int serverPort;

    private synchronized static void ensureStarted() {
        if (context != null) {
            return;
        }
        postgres = new PostgreSQLContainer<>("postgres:16-alpine");
        postgres.start();

        SpringApplication app = new SpringApplication(PaymentsCoreApplication.class);
        context = app.run(
                "--server.port=0",
                "--spring.datasource.url=" + postgres.getJdbcUrl(),
                "--spring.datasource.username=" + postgres.getUsername(),
                "--spring.datasource.password=" + postgres.getPassword(),
                "--paycore.redis.enabled=false",
                "--paycore.resolver.interval-ms=3600000");
        merchantService = context.getBean(MerchantService.class);
        paymentService = context.getBean(PaymentService.class);
        pspClient = context.getBean(ResilientPspClient.class);
        serverPort = context.getEnvironment().getProperty("local.server.port", Integer.class, 8080);
    }

    @BeforeTry
    void initBeforeTry() {
        ensureStarted();
        pspClient.clearFaults();
    }

    @Property(tries = 25, seed = "20261011")
    void randomMultiMerchantPaymentAndLedgerSequencesPreserveAllAccountingInvariants(
            @ForAll("operationSequences") List<LedgerOp> sequence) {

        // Provision 3 distinct merchants for this property trial
        List<MerchantService.CreatedMerchantResponse> merchants = List.of(
                merchantService.createMerchant(new MerchantService.CreateMerchantRequest(
                        "PropMerch-A-" + UUID.randomUUID(), "https://example.com/a", null, 150)),
                merchantService.createMerchant(new MerchantService.CreateMerchantRequest(
                        "PropMerch-B-" + UUID.randomUUID(), "https://example.com/b", null, 200)),
                merchantService.createMerchant(new MerchantService.CreateMerchantRequest(
                        "PropMerch-C-" + UUID.randomUUID(), "https://example.com/c", null, 250)));

        List<List<UUID>> paymentsPerMerchant = List.of(
                new ArrayList<>(), new ArrayList<>(), new ArrayList<>());

        for (LedgerOp op : sequence) {
            int mIdx = Math.floorMod(op.merchantSlot(), merchants.size());
            MerchantService.CreatedMerchantResponse m = merchants.get(mIdx);
            List<UUID> mPayments = paymentsPerMerchant.get(mIdx);

            try {
                switch (op.kind()) {
                    case CREATE_AND_CAPTURE -> {
                        Payment p = paymentService.createPayment(
                                m.id(), new PaymentService.CreatePaymentRequest(op.amountMinor(), "INR", Map.of()));
                        mPayments.add(p.id());
                        paymentService.authorizePayment(m.id(), p.id());
                        paymentService.capturePayment(m.id(), p.id(), m.feeBps());
                    }
                    case CREATE_ONLY -> {
                        Payment p = paymentService.createPayment(
                                m.id(), new PaymentService.CreatePaymentRequest(op.amountMinor(), "INR", Map.of()));
                        mPayments.add(p.id());
                    }
                    case AUTHORIZE -> {
                        if (!mPayments.isEmpty()) {
                            UUID pId = mPayments.get(Math.floorMod(op.targetSlot(), mPayments.size()));
                            paymentService.authorizePayment(m.id(), pId);
                        }
                    }
                    case CAPTURE -> {
                        if (!mPayments.isEmpty()) {
                            UUID pId = mPayments.get(Math.floorMod(op.targetSlot(), mPayments.size()));
                            paymentService.capturePayment(m.id(), pId, m.feeBps());
                        }
                    }
                    case PARTIAL_REFUND -> {
                        if (!mPayments.isEmpty()) {
                            UUID pId = mPayments.get(Math.floorMod(op.targetSlot(), mPayments.size()));
                            paymentService.refundPayment(
                                    m.id(), pId, new PaymentService.RefundRequest(op.amountMinor(), "jqwik-refund"));
                        }
                    }
                    case VOID -> {
                        if (!mPayments.isEmpty()) {
                            UUID pId = mPayments.get(Math.floorMod(op.targetSlot(), mPayments.size()));
                            paymentService.voidPayment(m.id(), pId);
                        }
                    }
                    case PAYOUT -> {
                        paymentService.createPayout(
                                m.id(), new PaymentService.PayoutRequest(op.amountMinor(), "INR"));
                    }
                    case PSP_TIMEOUT_AND_RESOLVE -> {
                        Payment p = paymentService.createPayment(
                                m.id(), new PaymentService.CreatePaymentRequest(op.amountMinor(), "INR", Map.of()));
                        mPayments.add(p.id());
                        pspClient.scriptFaultForPayment(
                                p.id(), ResilientPspClient.FaultMode.TIMEOUT_AFTER_PSP_CHARGE, 0L);
                        paymentService.authorizePayment(m.id(), p.id());
                        paymentService.resolveStuckAuthorizingPayments(Duration.ZERO);
                    }
                }
            } catch (PayCoreException expectedDomainGuard) {
                // Illegal state transitions, over-refunds, or over-payouts are cleanly rejected by domain/ledger guards
            }
        }

        paymentService.resolveStuckAuthorizingPayments(Duration.ZERO);

        // Verify all mathematical ledger invariants after the randomized sequence
        TestRestTemplate client = new TestRestTemplate();
        ResponseEntity<JsonNode> invResp = client.getForEntity(
                "http://localhost:" + serverPort + "/v1/admin/invariants", JsonNode.class);
        assertThat(invResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        JsonNode report = invResp.getBody();
        assertThat(report.get("status").asText()).isEqualTo("PASS");
        assertThat(report.get("healthy").asBoolean()).isTrue();
        assertThat(report.get("globalImbalance").asLong()).isZero();
        assertThat(report.get("unbalancedTransactions").asLong()).isZero();
        assertThat(report.get("negativeMerchantAccounts").asLong()).isZero();
        assertThat(report.get("ledgerDrift").asLong()).isZero();
        assertThat(report.get("illegalPayments").asLong()).isZero();
    }

    @Provide
    Arbitrary<List<LedgerOp>> operationSequences() {
        Arbitrary<OpKind> kinds = Arbitraries.of(OpKind.class);
        Arbitrary<Integer> merchantSlots = Arbitraries.integers().between(0, 2);
        Arbitrary<Integer> targetSlots = Arbitraries.integers().between(0, 10);
        Arbitrary<Long> amounts = Arbitraries.longs().between(100L, 25_000L);

        Arbitrary<LedgerOp> singleOp = Combinators.combine(kinds, merchantSlots, targetSlots, amounts)
                .as(LedgerOp::new);
        return singleOp.list().ofMinSize(15).ofMaxSize(35);
    }

    enum OpKind {
        CREATE_AND_CAPTURE,
        CREATE_ONLY,
        AUTHORIZE,
        CAPTURE,
        PARTIAL_REFUND,
        VOID,
        PAYOUT,
        PSP_TIMEOUT_AND_RESOLVE
    }

    record LedgerOp(OpKind kind, int merchantSlot, int targetSlot, long amountMinor) {}
}
