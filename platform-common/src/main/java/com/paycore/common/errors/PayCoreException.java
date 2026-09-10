package com.paycore.common.errors;

import java.net.URI;

/**
 * Base domain exception carrying an HTTP status code, RFC 7807 problem type URI, and machine-readable error code.
 */
public class PayCoreException extends RuntimeException {

    private final int httpStatus;
    private final String errorCode;
    private final String title;

    public PayCoreException(int httpStatus, String errorCode, String title, String detail) {
        super(detail);
        this.httpStatus = httpStatus;
        this.errorCode = errorCode;
        this.title = title;
    }

    public PayCoreException(int httpStatus, String errorCode, String title, String detail, Throwable cause) {
        super(detail, cause);
        this.httpStatus = httpStatus;
        this.errorCode = errorCode;
        this.title = title;
    }

    public int httpStatus() {
        return httpStatus;
    }

    public String errorCode() {
        return errorCode;
    }

    public String title() {
        return title;
    }

    public URI typeUri() {
        return URI.create("https://paycore.dev/problems/" + errorCode.toLowerCase().replace('_', '-'));
    }

    public static PayCoreException notFound(String resource, Object id) {
        return new PayCoreException(
                404,
                "RESOURCE_NOT_FOUND",
                "Resource Not Found",
                resource + " not found with id: " + id);
    }

    public static PayCoreException unauthorized(String detail) {
        return new PayCoreException(
                401,
                "UNAUTHORIZED_MERCHANT",
                "Unauthorized Merchant",
                detail);
    }

    public static PayCoreException idempotencyPayloadMismatch(String key) {
        return new PayCoreException(
                422,
                "IDEMPOTENCY_KEY_PAYLOAD_MISMATCH",
                "Idempotency Key Reused With Different Request Payload",
                "Idempotency-Key '" + key + "' was already used with a different request method, path, or body.");
    }

    public static PayCoreException idempotencyInProgress(String key) {
        return new PayCoreException(
                409,
                "IDEMPOTENCY_KEY_IN_PROGRESS",
                "Concurrent Request With Same Idempotency Key In Progress",
                "A request with Idempotency-Key '" + key + "' is currently being processed. Retry shortly.");
    }

    public static PayCoreException invalidStateTransition(String paymentId, String currentState, String targetOperation) {
        return new PayCoreException(
                409,
                "INVALID_PAYMENT_STATE",
                "Invalid Payment State Transition",
                "Payment " + paymentId + " in state '" + currentState + "' cannot perform '" + targetOperation + "'.");
    }

    public static PayCoreException unprocessable(String errorCode, String title, String detail) {
        return new PayCoreException(422, errorCode, title, detail);
    }
}
