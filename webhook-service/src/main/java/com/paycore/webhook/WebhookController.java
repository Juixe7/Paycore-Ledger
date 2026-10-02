package com.paycore.webhook;

import com.paycore.common.errors.PayCoreException;
import jakarta.servlet.http.HttpServletRequest;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Webhook Delivery, DLQ, Replay, and Consumer Lag REST APIs (Section 5.6).
 */
@RestController
public class WebhookController {

    private final WebhookDeliveryService webhookService;

    public WebhookController(WebhookDeliveryService webhookService) {
        this.webhookService = webhookService;
    }

    @PostMapping("/v1/webhook-endpoints")
    public ResponseEntity<Map<String, String>> registerEndpoint(@RequestBody RegisterEndpointRequest request) {
        webhookService.registerMerchantEndpoint(
                request.merchantId(), request.webhookUrl(), request.webhookSecret());
        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of("status", "REGISTERED"));
    }

    @GetMapping("/v1/webhook-deliveries")
    public ResponseEntity<List<WebhookDeliveryService.WebhookDeliveryRow>> listDeliveries(
            @RequestParam(value = "merchantId", required = false) UUID merchantId) {
        return ResponseEntity.ok(webhookService.listDeliveries(merchantId));
    }

    @PostMapping({"/v1/webhook-deliveries/{id}/replay", "/v1/admin/webhooks/{id}/replay"})
    public ResponseEntity<WebhookDeliveryService.DeliveryOutcome> replayDelivery(
            @PathVariable("id") UUID deliveryId) {
        return ResponseEntity.ok(webhookService.replayDelivery(deliveryId));
    }

    @GetMapping("/v1/admin/dlq")
    public ResponseEntity<List<WebhookDeliveryService.DlqEventRow>> listDlq() {
        return ResponseEntity.ok(webhookService.listDlqEvents());
    }

    @PostMapping("/v1/admin/dlq/{id}/replay")
    public ResponseEntity<WebhookDeliveryService.DlqEventRow> replayDlq(
            @PathVariable("id") UUID dlqId) {
        return ResponseEntity.ok(webhookService.replayDlqEvent(dlqId));
    }

    @GetMapping("/v1/admin/consumers/lag")
    public ResponseEntity<WebhookDeliveryService.ConsumerLagView> getConsumerLag() {
        return ResponseEntity.ok(webhookService.getConsumerLagStats());
    }

    @GetMapping("/healthz")
    public ResponseEntity<Map<String, String>> healthz() {
        return ResponseEntity.ok(Map.of("status", "UP", "service", "webhook-service"));
    }

    @GetMapping("/readyz")
    public ResponseEntity<Map<String, String>> readyz() {
        return ResponseEntity.ok(Map.of("status", "READY", "service", "webhook-service"));
    }

    @ExceptionHandler(PayCoreException.class)
    public ResponseEntity<ProblemDetail> handlePayCoreException(
            PayCoreException ex, HttpServletRequest request) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(
                HttpStatus.valueOf(ex.httpStatus()), ex.getMessage());
        problem.setType(ex.typeUri());
        problem.setTitle(ex.title());
        problem.setInstance(URI.create(request.getRequestURI()));
        problem.setProperty("errorCode", ex.errorCode());
        return ResponseEntity.status(ex.httpStatus())
                .contentType(MediaType.APPLICATION_PROBLEM_JSON)
                .body(problem);
    }

    public record RegisterEndpointRequest(UUID merchantId, String webhookUrl, String webhookSecret) {}
}
