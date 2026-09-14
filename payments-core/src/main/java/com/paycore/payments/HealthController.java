package com.paycore.payments;

import java.util.Map;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HealthController {

    private final JdbcClient jdbcClient;

    public HealthController(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    @GetMapping("/healthz")
    public ResponseEntity<Map<String, String>> healthz() {
        return ResponseEntity.ok(Map.of("status", "UP", "service", "payments-core"));
    }

    @GetMapping("/readyz")
    public ResponseEntity<Map<String, String>> readyz() {
        Integer one = jdbcClient.sql("SELECT 1").query(Integer.class).single();
        if (one != null && one == 1) {
            return ResponseEntity.ok(Map.of("status", "READY", "database", "UP"));
        }
        return ResponseEntity.status(503).body(Map.of("status", "NOT_READY"));
    }
}
