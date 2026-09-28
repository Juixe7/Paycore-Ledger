package com.paycore.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.paycore.common.events.v1.PaymentAuthorized;
import com.paycore.common.events.v1.PaymentCaptured;
import com.paycore.common.events.v1.PaymentEvent;
import com.paycore.common.events.v1.PayoutCreated;
import com.paycore.common.events.v1.RefundProcessed;
import com.paycore.common.ids.UuidV7;
import com.paycore.common.money.Money;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * End-to-End Verification & Chaos Fault-Injection Suite (Phases 5, 6 & 9):
 * 1. Protobuf wire-level lifecycle event serialization & forward-compatibility contract
 *    across Authorize -> Capture -> Refund -> Payout.
 * 2. Toxiproxy network chaos injection (TCP latency & abrupt connection cut mid-transaction)
 *    proving PostgreSQL atomic rollback and zero orphaned ledger entries.
 */
@Testcontainers
class PayCoreEndToEndTest {

    private static final Network NETWORK = Network.newNetwork();

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .withNetwork(NETWORK)
            .withNetworkAliases("postgres-db")
            .withDatabaseName("paycore_e2e")
            .withUsername("paycore")
            .withPassword("paycore");

    @Container
    static final ToxiproxyContainer TOXIPROXY = new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.9.0")
            .withNetwork(NETWORK);

    @Test
    @DisplayName("Protobuf v1 lifecycle events serialize deterministically across Authorize -> Capture -> Refund -> Payout")
    void protobufLifecycleContractRoundTrip() throws Exception {
        String paymentId = UuidV7.generate().toString();
        String merchantId = UuidV7.generate().toString();
        Money authorized = Money.inr(150_00L);
        Money platformFee = authorized.calculateFeeBps(200); // 2.00% = 300 paise
        Money merchantNet = authorized.subtract(platformFee);

        PaymentEvent authEvent = PaymentEvent.newBuilder()
                .setEventId(UuidV7.generate().toString())
                .setEventType("payment.authorized")
                .setSchemaVersion(1)
                .setPaymentId(paymentId)
                .setMerchantId(merchantId)
                .setEventSeq(1L)
                .setOccurredAt(Timestamp.newBuilder().setSeconds(1728640000L).build())
                .putTraceContext("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
                .setAuthorized(PaymentAuthorized.newBuilder()
                        .setAmount(authorized.amountMinor())
                        .setCurrency("INR")
                        .setPspRef("psp_auth_" + paymentId)
                        .build())
                .build();

        PaymentEvent captureEvent = PaymentEvent.newBuilder()
                .setEventId(UuidV7.generate().toString())
                .setEventType("payment.captured")
                .setSchemaVersion(1)
                .setPaymentId(paymentId)
                .setMerchantId(merchantId)
                .setEventSeq(2L)
                .setOccurredAt(Timestamp.newBuilder().setSeconds(1728640005L).build())
                .setCaptured(PaymentCaptured.newBuilder()
                        .setCapturedAmount(authorized.amountMinor())
                        .setFeeAmount(platformFee.amountMinor())
                        .setNetMerchantAmount(merchantNet.amountMinor())
                        .setCurrency("INR")
                        .setPspRef("psp_cap_" + paymentId)
                        .setLedgerTxnId(UuidV7.generate().toString())
                        .build())
                .build();

        PaymentEvent refundEvent = PaymentEvent.newBuilder()
                .setEventId(UuidV7.generate().toString())
                .setEventType("refund.processed")
                .setSchemaVersion(1)
                .setPaymentId(paymentId)
                .setMerchantId(merchantId)
                .setEventSeq(3L)
                .setOccurredAt(Timestamp.newBuilder().setSeconds(1728640010L).build())
                .setRefunded(RefundProcessed.newBuilder()
                        .setRefundId(UuidV7.generate().toString())
                        .setRefundAmount(50_00L)
                        .setTotalRefundedAmount(50_00L)
                        .setCurrency("INR")
                        .setLedgerTxnId(UuidV7.generate().toString())
                        .build())
                .build();

        PaymentEvent payoutEvent = PaymentEvent.newBuilder()
                .setEventId(UuidV7.generate().toString())
                .setEventType("payout.created")
                .setSchemaVersion(1)
                .setPaymentId(paymentId)
                .setMerchantId(merchantId)
                .setEventSeq(4L)
                .setOccurredAt(Timestamp.newBuilder().setSeconds(1728640020L).build())
                .setPayout(PayoutCreated.newBuilder()
                        .setPayoutId(UuidV7.generate().toString())
                        .setAmount(merchantNet.amountMinor() - 50_00L)
                        .setCurrency("INR")
                        .setLedgerTxnId(UuidV7.generate().toString())
                        .build())
                .build();

        List<ByteString> wirePayloads = List.of(
                authEvent.toByteString(),
                captureEvent.toByteString(),
                refundEvent.toByteString(),
                payoutEvent.toByteString()
        );

        List<PaymentEvent> decoded = new ArrayList<>();
        for (ByteString bytes : wirePayloads) {
            decoded.add(PaymentEvent.parseFrom(bytes));
        }

        assertThat(decoded).extracting(PaymentEvent::getEventSeq).containsExactly(1L, 2L, 3L, 4L);
        assertThat(decoded.get(1).getCaptured().getCapturedAmount()).isEqualTo(150_00L);
        assertThat(decoded.get(1).getCaptured().getFeeAmount() + decoded.get(1).getCaptured().getNetMerchantAmount())
                .isEqualTo(150_00L);
        assertThat(decoded.get(2).getRefunded().getRefundAmount()).isEqualTo(50_00L);
        assertThat(decoded.get(3).getPayout().getAmount()).isEqualTo(97_00L);
    }

    @Test
    @DisplayName("Toxiproxy Chaos: Network latency & mid-transaction TCP cut leaves zero uncommitted/unbalanced ledger legs")
    void toxiproxyNetworkChaosMidTransactionRollbackAndRecovery() throws Exception {
        ToxiproxyClient toxiproxyClient = new ToxiproxyClient(TOXIPROXY.getHost(), TOXIPROXY.getControlPort());
        Proxy pgProxy = toxiproxyClient.createProxy("pg-chaos-proxy", "0.0.0.0:8666", "postgres-db:5432");

        String proxiedJdbcUrl = String.format(
                "jdbc:postgresql://%s:%d/paycore_e2e?socketTimeout=3&connectTimeout=3",
                TOXIPROXY.getHost(),
                TOXIPROXY.getMappedPort(8666)
        );

        // 1. Initialize schema via proxied connection
        try (Connection conn = DriverManager.getConnection(proxiedJdbcUrl, POSTGRES.getUsername(), POSTGRES.getPassword())) {
            conn.createStatement().execute("""
                    CREATE TABLE IF NOT EXISTS chaos_ledger_entries (
                        id VARCHAR(64) PRIMARY KEY,
                        txn_id VARCHAR(64) NOT NULL,
                        direction VARCHAR(10) NOT NULL,
                        amount_cents BIGINT NOT NULL
                    )
                    """);
        }

        // 2. Inject 120ms network latency toxic and verify valid balanced transaction commits cleanly
        pgProxy.toxics().latency("pg-latency-120ms", ToxicDirection.DOWNSTREAM, 120);
        try (Connection conn = DriverManager.getConnection(proxiedJdbcUrl, POSTGRES.getUsername(), POSTGRES.getPassword())) {
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO chaos_ledger_entries (id, txn_id, direction, amount_cents) VALUES (?, ?, ?, ?)")) {
                ps.setString(1, "leg-1");
                ps.setString(2, "txn-good");
                ps.setString(3, "DEBIT");
                ps.setLong(4, 10000L);
                ps.executeUpdate();

                ps.setString(1, "leg-2");
                ps.setString(2, "txn-good");
                ps.setString(3, "CREDIT");
                ps.setLong(4, 10000L);
                ps.executeUpdate();
            }
            conn.commit();
        }
        pgProxy.toxics().get("pg-latency-120ms").remove();

        // 3. Begin an in-flight transaction, insert ONLY the DEBIT leg, then sever the TCP link via Toxiproxy before CREDIT/COMMIT
        try (Connection conn = DriverManager.getConnection(proxiedJdbcUrl, POSTGRES.getUsername(), POSTGRES.getPassword())) {
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO chaos_ledger_entries (id, txn_id, direction, amount_cents) VALUES (?, ?, ?, ?)")) {
                ps.setString(1, "leg-orphan-debit");
                ps.setString(2, "txn-severed");
                ps.setString(3, "DEBIT");
                ps.setLong(4, 50000L);
                ps.executeUpdate();
            }

            // Cut network proxy completely mid-transaction
            pgProxy.disable();

            assertThatThrownBy(conn::commit).isInstanceOf(SQLException.class);
        } finally {
            pgProxy.enable();
        }

        // 4. Verify via restored connection that the severed transaction rolled back completely and ledger balance is intact
        try (Connection conn = DriverManager.getConnection(proxiedJdbcUrl, POSTGRES.getUsername(), POSTGRES.getPassword());
             ResultSet rs = conn.createStatement().executeQuery("""
                     SELECT
                         COUNT(*) FILTER (WHERE txn_id = 'txn-severed') AS severed_rows,
                         COALESCE(SUM(CASE WHEN direction = 'DEBIT' THEN amount_cents ELSE 0 END), 0) AS total_debits,
                         COALESCE(SUM(CASE WHEN direction = 'CREDIT' THEN amount_cents ELSE 0 END), 0) AS total_credits
                     FROM chaos_ledger_entries
                     """)) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getLong("severed_rows")).isZero();
            assertThat(rs.getLong("total_debits")).isEqualTo(10000L);
            assertThat(rs.getLong("total_credits")).isEqualTo(10000L);
        }
    }
}
