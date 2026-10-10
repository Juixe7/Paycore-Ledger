package com.paycore.payments;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.paycore.common.ids.UuidV7;
import com.paycore.common.tracing.TraceHeaders;
import com.paycore.payments.psp.ResilientPspClient;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Phase 6 & Phase 7 Verification:
 * - Custom Prometheus/Micrometer SLO metrics + OpenAPI 3.1 contract check
 * - Stretch A/B Benchmark 1: Idempotency Replay Latency
 * - Stretch A/B Benchmark 2: 16-Way Sharded System Accounts vs Unsharded Single Account under Concurrent Captures
 * - Stretch A/B Benchmark 3: Java 21 Virtual Threads vs Bounded Platform Threads under PSP Latency
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureObservability
@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class StretchAbBenchmarkTest {

    private static final Logger log = LoggerFactory.getLogger(StretchAbBenchmarkTest.class);

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("paycore_benchmark_test")
            .withUsername("paycore")
            .withPassword("paycore");

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("paycore.redis.enabled", () -> "false");
        registry.add("paycore.outbox.relay-enabled", () -> "false");
        registry.add("paycore.resolver.interval-ms", () -> "3600000");
        registry.add("management.endpoints.web.exposure.include", () -> "health,info,prometheus,metrics");
    }

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private ResilientPspClient pspClient;

    @Autowired
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        pspClient.clearFaults();
    }

    @Test
    @DisplayName("1. Phase 6 Observability & OpenAPI 3.1 Contract Verification")
    void prometheusMetricsAndOpenApiContractVerified() throws Exception {
        // Trigger invariant check so drift gauge is updated
        ResponseEntity<String> invResp = restTemplate.getForEntity("/v1/admin/invariants", String.class);
        assertThat(invResp.getStatusCode()).isEqualTo(HttpStatus.OK);

        ResponseEntity<String> promResp = restTemplate.getForEntity("/actuator/prometheus", String.class);
        assertThat(promResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(promResp.getBody())
                .contains("paycore_ledger_invariant_drift_paise")
                .contains("paycore_outbox_unpublished_rows")
                .contains("paycore_outbox_oldest_unpublished_age_seconds");

        Path openApiPath = Path.of("../openapi/paycore-openapi.yaml");
        if (!Files.exists(openApiPath)) {
            openApiPath = Path.of("openapi/paycore-openapi.yaml");
        }
        assertThat(Files.exists(openApiPath)).isTrue();
        String yaml = Files.readString(openApiPath);
        assertThat(yaml)
                .contains("openapi: 3.1.0")
                .contains("/v1/payments:")
                .contains("/v1/payments/{id}/authorize:")
                .contains("/v1/payments/{id}/capture:")
                .contains("/v1/payments/{id}/refunds:")
                .contains("/v1/payouts:")
                .contains("/v1/admin/invariants:");
    }

    @Test
    @DisplayName("2. Stretch A/B Benchmark: 16-Way Sharded Fee & PSP Accounts vs Unsharded Single Row (64 Concurrent Captures)")
    void shardedFeeAccountVsUnshardedBenchmark() throws Exception {
        int concurrency = 64;
        int merchantCount = 16;
        List<String> apiKeys = new ArrayList<>(merchantCount);
        for (int m = 0; m < merchantCount; m++) {
            apiKeys.add(createMerchant("BenchMerchant-" + m, 290));
        }

        // --- A: Baseline (Unsharded: sharded=false) ---
        restTemplate.postForEntity("/v1/admin/ledger/sharding?enabled=false", null, String.class);
        List<PaymentTarget> unshardedTargets = prepareAuthorizedPayments(concurrency, apiKeys);
        BenchmarkStats unshardedStats = runConcurrentCaptures(unshardedTargets);

        // --- B: Treatment (16-Way Sharded: sharded=true) ---
        restTemplate.postForEntity("/v1/admin/ledger/sharding?enabled=true", null, String.class);
        List<PaymentTarget> shardedTargets = prepareAuthorizedPayments(concurrency, apiKeys);
        BenchmarkStats shardedStats = runConcurrentCaptures(shardedTargets);

        log.info(
                "A/B Hot Account Benchmark ({} concurrent captures) -> Unsharded p50={}ms p99={}ms total={}ms | 16-Shard p50={}ms p99={}ms total={}ms",
                concurrency,
                unshardedStats.p50Ms(),
                unshardedStats.p99Ms(),
                unshardedStats.wallClockMs(),
                shardedStats.p50Ms(),
                shardedStats.p99Ms(),
                shardedStats.wallClockMs());

        // Verify system balances & shard distribution
        ResponseEntity<String> sysBalResp =
                restTemplate.getForEntity("/v1/admin/ledger/system-balances", String.class);
        assertThat(sysBalResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        JsonNode sysBal = objectMapper.readTree(sysBalResp.getBody());
        assertThat(sysBal.get("shardedModeEnabled").asBoolean()).isTrue();
        assertThat(sysBal.get("activeFeeShardsPopulated").asLong()).isGreaterThan(4L);

        // Verify ledger invariants remain 100% healthy (drift == 0) across both unsharded and sharded runs
        ResponseEntity<String> invResp = restTemplate.getForEntity("/v1/admin/invariants", String.class);
        assertThat(invResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        JsonNode invJson = objectMapper.readTree(invResp.getBody());
        assertThat(invJson.get("status").asText()).isEqualTo("PASS");
        assertThat(invJson.get("ledgerDrift").asLong()).isZero();

        // Reset sharding flag back to default
        restTemplate.postForEntity("/v1/admin/ledger/sharding?enabled=false", null, String.class);
    }

    @Test
    @DisplayName("3. Stretch A/B Benchmark: Java 21 Virtual Threads vs Bounded Platform Pool under 80ms PSP I/O Latency")
    void virtualThreadsVsBoundedPlatformPoolUnderPspLatency() throws Exception {
        String apiKey = createMerchant("VtBenchMerchant", 250);
        int requestCount = 48;
        pspClient.setSimulatedLatencyMs(80L);

        try {
            // Warm up virtual thread carrier pool and HikariCP connections
            List<UUID> warmupIds = createPaymentsOnly(8, apiKey);
            try (ExecutorService warmupPool = Executors.newVirtualThreadPerTaskExecutor()) {
                List<Future<ResponseEntity<String>>> warmupFutures = new ArrayList<>(8);
                for (UUID pid : warmupIds) {
                    warmupFutures.add(warmupPool.submit(() -> postJson(
                            "/v1/payments/" + pid + "/authorize",
                            apiKey,
                            "auth-warm-" + pid,
                            Map.of())));
                }
                for (Future<ResponseEntity<String>> f : warmupFutures) {
                    assertThat(f.get(30, TimeUnit.SECONDS).getStatusCode()).isEqualTo(HttpStatus.OK);
                }
            }

            // Prepare payments for Platform Thread Pool vs Virtual Thread Pool
            List<UUID> platformPaymentIds = createPaymentsOnly(requestCount, apiKey);
            List<UUID> virtualPaymentIds = createPaymentsOnly(requestCount, apiKey);

            // A: Bounded Platform Thread Pool (8 threads -> 48 requests take 6 waves of 80ms = ~480ms+)
            long platformStartNs = System.nanoTime();
            try (ExecutorService platformPool = Executors.newFixedThreadPool(8)) {
                List<Future<ResponseEntity<String>>> futures = new ArrayList<>(requestCount);
                for (UUID pid : platformPaymentIds) {
                    futures.add(platformPool.submit(() -> postJson(
                            "/v1/payments/" + pid + "/authorize",
                            apiKey,
                            "auth-plat-" + pid,
                            Map.of())));
                }
                for (Future<ResponseEntity<String>> f : futures) {
                    assertThat(f.get(30, TimeUnit.SECONDS).getStatusCode()).isEqualTo(HttpStatus.OK);
                }
            }
            long platformWallMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - platformStartNs);

            // B: Java 21 Virtual Thread Executor (all 48 requests execute concurrently in 1 wave)
            long virtualStartNs = System.nanoTime();
            try (ExecutorService virtualPool = Executors.newVirtualThreadPerTaskExecutor()) {
                List<Future<ResponseEntity<String>>> futures = new ArrayList<>(requestCount);
                for (UUID pid : virtualPaymentIds) {
                    futures.add(virtualPool.submit(() -> postJson(
                            "/v1/payments/" + pid + "/authorize",
                            apiKey,
                            "auth-vt-" + pid,
                            Map.of())));
                }
                for (Future<ResponseEntity<String>> f : futures) {
                    assertThat(f.get(30, TimeUnit.SECONDS).getStatusCode()).isEqualTo(HttpStatus.OK);
                }
            }
            long virtualWallMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - virtualStartNs);

            log.info(
                    "A/B Virtual Threads Benchmark ({} concurrent authorizations @ 80ms PSP latency) -> Bounded Platform Pool (8 threads)={}ms | Java 21 Virtual Threads={}ms",
                    requestCount,
                    platformWallMs,
                    virtualWallMs);

            assertThat(virtualWallMs).isLessThan(platformWallMs);
        } finally {
            pspClient.clearFaults();
        }
    }

    private record PaymentTarget(UUID paymentId, String apiKey) {}

    private record BenchmarkStats(long p50Ms, long p99Ms, long wallClockMs) {}

    private List<PaymentTarget> prepareAuthorizedPayments(int count, List<String> apiKeys) throws Exception {
        List<PaymentTarget> targets = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String apiKey = apiKeys.get(i % apiKeys.size());
            ResponseEntity<String> createResp = postJson(
                    "/v1/payments", apiKey, "prep-c-" + UuidV7.generate(), Map.of("amount", 10_000L, "currency", "INR"));
            UUID paymentId = UUID.fromString(objectMapper.readTree(createResp.getBody()).get("id").asText());
            ResponseEntity<String> authResp = postJson(
                    "/v1/payments/" + paymentId + "/authorize", apiKey, "prep-a-" + paymentId, Map.of());
            assertThat(authResp.getStatusCode()).isEqualTo(HttpStatus.OK);
            targets.add(new PaymentTarget(paymentId, apiKey));
        }
        return targets;
    }

    private List<UUID> createPaymentsOnly(int count, String apiKey) throws Exception {
        List<UUID> ids = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            ResponseEntity<String> createResp = postJson(
                    "/v1/payments", apiKey, "vt-c-" + UuidV7.generate(), Map.of("amount", 5_000L, "currency", "INR"));
            UUID paymentId = UUID.fromString(objectMapper.readTree(createResp.getBody()).get("id").asText());
            ids.add(paymentId);
        }
        return ids;
    }

    private BenchmarkStats runConcurrentCaptures(List<PaymentTarget> targets) throws Exception {
        CountDownLatch ready = new CountDownLatch(targets.size());
        CountDownLatch start = new CountDownLatch(1);
        List<Long> latenciesMs = Collections.synchronizedList(new ArrayList<>(targets.size()));

        long wallStart = System.nanoTime();
        try (ExecutorService exec = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<?>> futures = new ArrayList<>(targets.size());
            for (PaymentTarget t : targets) {
                futures.add(exec.submit(() -> {
                    ready.countDown();
                    try {
                        start.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    long t0 = System.nanoTime();
                    ResponseEntity<String> resp = postJson(
                            "/v1/payments/" + t.paymentId() + "/capture",
                            t.apiKey(),
                            "cap-bench-" + t.paymentId(),
                            Map.of());
                    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
                    assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.OK);
                    latenciesMs.add(elapsedMs);
                }));
            }
            ready.await(10, TimeUnit.SECONDS);
            start.countDown();
            for (Future<?> f : futures) {
                f.get(30, TimeUnit.SECONDS);
            }
        }
        long wallMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - wallStart);
        Collections.sort(latenciesMs);
        long p50 = latenciesMs.get(latenciesMs.size() / 2);
        long p99 = latenciesMs.get(Math.min(latenciesMs.size() - 1, (int) (latenciesMs.size() * 0.99)));
        return new BenchmarkStats(p50, p99, wallMs);
    }

    private String createMerchant(String name, int feeBps) throws Exception {
        ResponseEntity<String> resp = restTemplate.postForEntity(
                "/v1/merchants",
                Map.of(
                        "name", name,
                        "email", name.toLowerCase() + "-" + UuidV7.generate() + "@example.com",
                        "feeBps", feeBps,
                        "webhookUrl", "https://merchant.example.com/webhooks"),
                String.class);
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        return objectMapper.readTree(resp.getBody()).get("apiKey").asText();
    }

    private ResponseEntity<String> postJson(
            String path, String apiKey, String idempotencyKey, Map<String, Object> body) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set(TraceHeaders.API_KEY_HEADER, apiKey);
        headers.set(TraceHeaders.IDEMPOTENCY_KEY, idempotencyKey);
        return restTemplate.exchange(path, HttpMethod.POST, new HttpEntity<>(body, headers), String.class);
    }
}
