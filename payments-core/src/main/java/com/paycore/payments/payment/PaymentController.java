package com.paycore.payments.payment;

import com.paycore.common.errors.PayCoreException;
import com.paycore.common.tracing.TraceHeaders;
import com.paycore.payments.idempotency.IdempotencyService;
import com.paycore.payments.merchant.Merchant;
import com.paycore.payments.merchant.MerchantAuthFilter;
import jakarta.servlet.http.HttpServletRequest;
import java.util.Map;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class PaymentController {

    private final PaymentService paymentService;
    private final IdempotencyService idempotencyService;

    public PaymentController(PaymentService paymentService, IdempotencyService idempotencyService) {
        this.paymentService = paymentService;
        this.idempotencyService = idempotencyService;
    }

    @PostMapping("/v1/payments")
    public ResponseEntity<Payment> createPayment(
            HttpServletRequest httpRequest,
            @RequestHeader(value = TraceHeaders.IDEMPOTENCY_KEY, required = false) String idempotencyKey,
            @RequestBody PaymentService.CreatePaymentRequest body) {
        UUID merchantId = requireMerchantId(httpRequest);
        IdempotencyService.IdempotentResult<Payment> result = idempotencyService.execute(
                merchantId,
                idempotencyKey,
                "POST",
                "/v1/payments",
                body,
                HttpStatus.CREATED.value(),
                Payment.class,
                () -> paymentService.createPayment(merchantId, body));
        return toIdempotentResponse(result);
    }

    @PostMapping("/v1/payments/{id}/authorize")
    public ResponseEntity<Payment> authorizePayment(
            HttpServletRequest httpRequest,
            @PathVariable("id") UUID paymentId,
            @RequestHeader(value = TraceHeaders.IDEMPOTENCY_KEY, required = false) String idempotencyKey,
            @RequestBody(required = false) Map<String, Object> body) {
        UUID merchantId = requireMerchantId(httpRequest);
        String path = "/v1/payments/" + paymentId + "/authorize";
        IdempotencyService.IdempotentResult<Payment> result = idempotencyService.executeTwoPhase(
                merchantId,
                idempotencyKey,
                "POST",
                path,
                body != null ? body : Map.of(),
                HttpStatus.OK.value(),
                Payment.class,
                () -> paymentService.authorizePayment(merchantId, paymentId));
        return toIdempotentResponse(result);
    }

    @PostMapping("/v1/payments/{id}/capture")
    public ResponseEntity<Payment> capturePayment(
            HttpServletRequest httpRequest,
            @PathVariable("id") UUID paymentId,
            @RequestHeader(value = TraceHeaders.IDEMPOTENCY_KEY, required = false) String idempotencyKey,
            @RequestBody(required = false) Map<String, Object> body) {
        Merchant merchant = requireMerchant(httpRequest);
        String path = "/v1/payments/" + paymentId + "/capture";
        IdempotencyService.IdempotentResult<Payment> result = idempotencyService.execute(
                merchant.id(),
                idempotencyKey,
                "POST",
                path,
                body != null ? body : Map.of(),
                HttpStatus.OK.value(),
                Payment.class,
                () -> paymentService.capturePayment(merchant.id(), paymentId, merchant.feeBps()));
        return toIdempotentResponse(result);
    }

    @PostMapping("/v1/payments/{id}/void")
    public ResponseEntity<Payment> voidPayment(
            HttpServletRequest httpRequest,
            @PathVariable("id") UUID paymentId,
            @RequestHeader(value = TraceHeaders.IDEMPOTENCY_KEY, required = false) String idempotencyKey,
            @RequestBody(required = false) Map<String, Object> body) {
        UUID merchantId = requireMerchantId(httpRequest);
        String path = "/v1/payments/" + paymentId + "/void";
        IdempotencyService.IdempotentResult<Payment> result = idempotencyService.execute(
                merchantId,
                idempotencyKey,
                "POST",
                path,
                body != null ? body : Map.of(),
                HttpStatus.OK.value(),
                Payment.class,
                () -> paymentService.voidPayment(merchantId, paymentId));
        return toIdempotentResponse(result);
    }

    @PostMapping("/v1/payments/{id}/refunds")
    public ResponseEntity<PaymentService.RefundResponse> refundPayment(
            HttpServletRequest httpRequest,
            @PathVariable("id") UUID paymentId,
            @RequestHeader(value = TraceHeaders.IDEMPOTENCY_KEY, required = false) String idempotencyKey,
            @RequestBody PaymentService.RefundRequest body) {
        UUID merchantId = requireMerchantId(httpRequest);
        String path = "/v1/payments/" + paymentId + "/refunds";
        IdempotencyService.IdempotentResult<PaymentService.RefundResponse> result = idempotencyService.execute(
                merchantId,
                idempotencyKey,
                "POST",
                path,
                body,
                HttpStatus.OK.value(),
                PaymentService.RefundResponse.class,
                () -> paymentService.refundPayment(merchantId, paymentId, body));
        return toIdempotentResponse(result);
    }

    @GetMapping("/v1/payments/{id}")
    public ResponseEntity<Payment> getPayment(
            HttpServletRequest httpRequest,
            @PathVariable("id") UUID paymentId) {
        UUID merchantId = requireMerchantId(httpRequest);
        return ResponseEntity.ok(paymentService.getPayment(merchantId, paymentId));
    }

    @PostMapping("/v1/payouts")
    public ResponseEntity<PaymentService.PayoutResponse> createPayout(
            HttpServletRequest httpRequest,
            @RequestHeader(value = TraceHeaders.IDEMPOTENCY_KEY, required = false) String idempotencyKey,
            @RequestBody PaymentService.PayoutRequest body) {
        UUID merchantId = requireMerchantId(httpRequest);
        IdempotencyService.IdempotentResult<PaymentService.PayoutResponse> result = idempotencyService.execute(
                merchantId,
                idempotencyKey,
                "POST",
                "/v1/payouts",
                body,
                HttpStatus.CREATED.value(),
                PaymentService.PayoutResponse.class,
                () -> paymentService.createPayout(merchantId, body));
        return toIdempotentResponse(result);
    }

    private <T> ResponseEntity<T> toIdempotentResponse(IdempotencyService.IdempotentResult<T> result) {
        return ResponseEntity.status(result.statusCode())
                .header("Idempotent-Replayed", String.valueOf(result.replayed()))
                .body(result.body());
    }

    private UUID requireMerchantId(HttpServletRequest request) {
        UUID merchantId = (UUID) request.getAttribute(MerchantAuthFilter.MERCHANT_ID_ATTR);
        if (merchantId == null) {
            throw PayCoreException.unauthorized("Missing authenticated merchant context");
        }
        return merchantId;
    }

    private Merchant requireMerchant(HttpServletRequest request) {
        Merchant merchant = (Merchant) request.getAttribute(MerchantAuthFilter.MERCHANT_ATTR);
        if (merchant == null) {
            throw PayCoreException.unauthorized("Missing authenticated merchant context");
        }
        return merchant;
    }
}
