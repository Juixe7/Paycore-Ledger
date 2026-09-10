package com.paycore.common.tracing;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * W3C Trace Context and correlation header constants and thread-local/scoped propagation helper.
 */
public final class TraceHeaders {

    public static final String TRACEPARENT = "traceparent";
    public static final String TRACESTATE = "tracestate";
    public static final String REQUEST_ID = "X-Request-Id";
    public static final String IDEMPOTENCY_KEY = "Idempotency-Key";
    public static final String API_KEY_HEADER = "X-Api-Key";

    public static final String WEBHOOK_SIGNATURE = "X-PayCore-Signature";
    public static final String WEBHOOK_TIMESTAMP = "X-PayCore-Timestamp";
    public static final String WEBHOOK_EVENT_ID = "X-PayCore-Event-Id";

    private static final ThreadLocal<Map<String, String>> CONTEXT =
            ThreadLocal.withInitial(HashMap::new);

    private TraceHeaders() {}

    public static void setContext(Map<String, String> headers) {
        Map<String, String> copy = new HashMap<>();
        if (headers != null) {
            copy.putAll(headers);
        }
        CONTEXT.set(copy);
    }

    public static void put(String key, String value) {
        if (value != null && !value.isBlank()) {
            CONTEXT.get().put(key, value);
        }
    }

    public static Map<String, String> currentContext() {
        return Collections.unmodifiableMap(new HashMap<>(CONTEXT.get()));
    }

    public static String currentTraceparentOrGenerate() {
        String existing = CONTEXT.get().get(TRACEPARENT);
        if (existing != null && !existing.isBlank()) {
            return existing;
        }
        String traceId = UUID.randomUUID().toString().replace("-", "");
        String spanId = UUID.randomUUID().toString().replace("-", "").substring(0, 16);
        String generated = "00-" + traceId + "-" + spanId + "-01";
        CONTEXT.get().put(TRACEPARENT, generated);
        return generated;
    }

    public static void clear() {
        CONTEXT.remove();
    }
}
