package com.paycore.payments;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.paycore.common.events.v1.PaymentEvent;
import com.paycore.payments.outbox.OutboxPollingRelay;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Live Multi-Container & Broker Integration Test verifying:
 * 1. Real Redis 7 container (`redis:7-alpine`) fast-path idempotency caching + mid-test Redis container outage fallback to PostgreSQL.
 * 2. Real Kafka broker (`@EmbeddedKafka`) receiving Protobuf v1 `PaymentEvent` messages published by `OutboxPollingRelay`.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(
        partitions = 3,
        topics = {OutboxPollingRelay.PAYMENTS_EVENTS_TOPIC},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers")
@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class LiveRedisAndRedpandaIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(LiveRedisAndRedpandaIntegrationTest.class);

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("paycore_live_infra")
            .withUsername("paycore")
            .withPassword("paycore");

    @Container
    @SuppressWarnings("resource")
    static final GenericContainer<?> REDIS = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
        registry.add("paycore.redis.enabled", () -> "true");
        registry.add("paycore.outbox.relay-enabled", () -> "false");
        registry.add("paycore.resolver.interval-ms", () -> "3600000");
    }

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private OutboxPollingRelay outboxPollingRelay;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    @DisplayName("Live Redis 7 + Live Kafka Broker: Redis fast-path cache hit, Redis outage Postgres fallback, and Outbox -> Kafka Protobuf streaming")
    void liveRedisFastPathAndOutageFallbackPlusRedpandaOutboxStreaming() throws Exception {
        // 1. Onboard merchant
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        ResponseEntity<String> merchantResp = restTemplate.exchange(
                "/v1/merchants",
                HttpMethod.POST,
                new HttpEntity<>(
                        objectMapper.writeValueAsString(Map.of(
                                "name", "LiveInfraMerchant",
                                "email", "infra@paycore.io",
                                "feeBps", 200)),
                        headers),
                String.class);
        assertThat(merchantResp.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        JsonNode merchantJson = objectMapper.readTree(merchantResp.getBody());
        String merchantId = merchantJson.get("id").asText();
        String apiKey = merchantJson.get("apiKey").asText();

        // 2. Create payment with Idempotency-Key -> populates both PostgreSQL and live Redis 7 container
        String idemKey = "live-redis-idem-001";
        ResponseEntity<String> createResp = postJson(
                "/v1/payments",
                apiKey,
                idemKey,
                Map.of("amount", 120_00L, "currency", "INR", "metadata", Map.of("order_id", "cust-live-1")));
        assertThat(createResp.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(createResp.getHeaders().getFirst("Idempotent-Replayed")).isEqualTo("false");
        String paymentId = objectMapper.readTree(createResp.getBody()).get("id").asText();

        // 3. Verify key is genuinely stored inside the live Redis 7 container
        String redisKey = "idem:" + merchantId + ":" + idemKey;
        String cachedJson = redisTemplate.opsForValue().get(redisKey);
        assertThat(cachedJson).isNotBlank();
        assertThat(cachedJson).contains("\"status\":\"COMPLETED\"");
        log.info("[LIVE-REDIS] Verified cached idempotency record in Redis 7 container under key={}", redisKey);

        // 4. Replay request -> served directly from live Redis 7 fast path
        ResponseEntity<String> replayFromRedis = postJson(
                "/v1/payments",
                apiKey,
                idemKey,
                Map.of("amount", 120_00L, "currency", "INR", "metadata", Map.of("order_id", "cust-live-1")));
        assertThat(replayFromRedis.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(replayFromRedis.getHeaders().getFirst("Idempotent-Replayed")).isEqualTo("true");
        assertThat(objectMapper.readTree(replayFromRedis.getBody()).get("id").asText()).isEqualTo(paymentId);

        // 5. Authorize, Capture, and Partially Refund the payment so 3 events (seq 1, 2, 3) are in outbox_events
        ResponseEntity<String> authResp = postJson(
                "/v1/payments/" + paymentId + "/authorize",
                apiKey,
                "live-auth-001",
                Map.of());
        assertThat(authResp.getStatusCode()).isEqualTo(HttpStatus.OK);

        ResponseEntity<String> capResp = postJson(
                "/v1/payments/" + paymentId + "/capture",
                apiKey,
                "live-cap-001",
                Map.of());
        assertThat(capResp.getStatusCode()).isEqualTo(HttpStatus.OK);

        ResponseEntity<String> refResp = postJson(
                "/v1/payments/" + paymentId + "/refunds",
                apiKey,
                "live-ref-001",
                Map.of("amount", 20_00L, "reason", "Partial return"));
        assertThat(refResp.getStatusCode()).isEqualTo(HttpStatus.OK);

        // 6. Trigger OutboxPollingRelay -> publishes Protobuf PaymentEvent bytes to live Kafka broker
        int publishedCount = outboxPollingRelay.relayNextBatch();
        assertThat(publishedCount).isEqualTo(3);

        Long unpublishedRemaining = jdbcClient.sql("SELECT COUNT(*) FROM outbox_events WHERE published_at IS NULL")
                .query(Long.class)
                .single();
        assertThat(unpublishedRemaining).isZero();

        // 7. Consume the published Protobuf events from the live Kafka broker
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker.getBrokersAsString());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "live-e2e-verifier-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        List<PaymentEvent> receivedEvents = new ArrayList<>();
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(OutboxPollingRelay.PAYMENTS_EVENTS_TOPIC));
            long deadline = System.currentTimeMillis() + 10_000L;
            while (System.currentTimeMillis() < deadline && receivedEvents.size() < 3) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(250));
                for (ConsumerRecord<String, byte[]> record : records) {
                    if (paymentId.equals(record.key())) {
                        PaymentEvent event = PaymentEvent.parseFrom(record.value());
                        receivedEvents.add(event);
                    }
                }
            }
        }

        assertThat(receivedEvents).hasSize(3);
        assertThat(receivedEvents).extracting(PaymentEvent::getEventSeq).containsExactly(1L, 2L, 3L);
        assertThat(receivedEvents)
                .extracting(PaymentEvent::getEventType)
                .containsExactly("payment.authorized", "payment.captured", "refund.processed");
        assertThat(receivedEvents.get(1).getCaptured().getCapturedAmount()).isEqualTo(120_00L);
        assertThat(receivedEvents.get(1).getCaptured().getFeeAmount()).isEqualTo(2_40L);
        assertThat(receivedEvents.get(1).getCaptured().getNetMerchantAmount()).isEqualTo(117_60L);
        assertThat(receivedEvents.get(2).getRefunded().getRefundAmount()).isEqualTo(20_00L);
        log.info("[LIVE-KAFKA] Consumed 3 Protobuf PaymentEvents (seq 1..3) from live broker topic={}",
                OutboxPollingRelay.PAYMENTS_EVENTS_TOPIC);

        // 8. Stop the live Redis 7 container mid-test and prove idempotent replay falls back to PostgreSQL seamlessly
        REDIS.stop();
        ResponseEntity<String> replayDuringRedisOutage = postJson(
                "/v1/payments",
                apiKey,
                idemKey,
                Map.of("amount", 120_00L, "currency", "INR", "metadata", Map.of("order_id", "cust-live-1")));
        assertThat(replayDuringRedisOutage.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(replayDuringRedisOutage.getHeaders().getFirst("Idempotent-Replayed")).isEqualTo("true");
        assertThat(objectMapper.readTree(replayDuringRedisOutage.getBody()).get("id").asText())
                .isEqualTo(paymentId);
        log.info("[LIVE-REDIS-OUTAGE] Redis container stopped -> idempotent replay succeeded via PostgreSQL fallback!");
    }

    private ResponseEntity<String> postJson(String path, String apiKey, String idempotencyKey, Object body)
            throws Exception {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("X-API-Key", apiKey);
        headers.set("Idempotency-Key", idempotencyKey);
        String json = body == null ? "{}" : objectMapper.writeValueAsString(body);
        return restTemplate.exchange(path, HttpMethod.POST, new HttpEntity<>(json, headers), String.class);
    }
}
