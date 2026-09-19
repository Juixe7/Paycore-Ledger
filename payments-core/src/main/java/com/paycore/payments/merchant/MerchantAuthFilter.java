package com.paycore.payments.merchant;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.paycore.common.tracing.TraceHeaders;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util. LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 10)
public class MerchantAuthFilter extends OncePerRequestFilter {

    public static final String MERCHANT_ID_ATTR = "paycore.merchantId";
    public static final String MERCHANT_ATTR = "paycore.merchant";

    private final MerchantService merchantService;
    private final ObjectMapper objectMapper;

    public MerchantAuthFilter(MerchantService merchantService, ObjectMapper objectMapper) {
        this.merchantService = merchantService;
        this.objectMapper = objectMapper;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        String path = request.getRequestURI();
        if (path == null) {
            return true;
        }
        return path.equals("/v1/merchants")
                || path.startsWith("/v1/admin/")
                || path.startsWith("/actuator")
                || path.equals("/healthz")
                || path.equals("/readyz")
                || path.equals("/metrics")
                || path.startsWith("/v3/api-docs")
                || path.startsWith("/swagger-ui");
    }

    @Override
    protected void doFilterInternal(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain filterChain) throws ServletException, IOException {
        try {
            TraceHeaders.put(TraceHeaders.TRACEPARENT, request.getHeader(TraceHeaders.TRACEPARENT));
            TraceHeaders.put(TraceHeaders.REQUEST_ID, request.getHeader(TraceHeaders.REQUEST_ID));

            String rawApiKey = extractApiKey(request);
            Optional<Merchant> authenticated = merchantService.authenticateByRawApiKey(rawApiKey);
            if (authenticated.isEmpty()) {
                writeUnauthorizedProblem(request, response);
                return;
            }

            Merchant merchant = authenticated.get();
            request.setAttribute(MERCHANT_ID_ATTR, merchant.id());
            request.setAttribute(MERCHANT_ATTR, merchant);
            filterChain.doFilter(request, response);
        } finally {
            TraceHeaders.clear();
        }
    }

    private String extractApiKey(HttpServletRequest request) {
        String apiKeyHeader = request.getHeader(TraceHeaders.API_KEY_HEADER);
        if (apiKeyHeader != null && !apiKeyHeader.isBlank()) {
            return apiKeyHeader.trim();
        }
        String authHeader = request.getHeader("Authorization");
        if (authHeader != null && authHeader.regionMatches(true, 0, "Bearer ", 0, 7)) {
            return authHeader.substring(7).trim();
        }
        return null;
    }

    private void writeUnauthorizedProblem(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        response.setContentType(MediaType.APPLICATION_PROBLEM_JSON_VALUE);
        Map<String, Object> problem = new LinkedHashMap<>();
        problem.put("type", "https://paycore.dev/problems/unauthorized-merchant");
        problem.put("title", "Unauthorized Merchant");
        problem.put("status", 401);
        problem.put("detail", "Valid X-Api-Key or Authorization Bearer API key is required.");
        problem.put("instance", request.getRequestURI());
        problem.put("errorCode", "UNAUTHORIZED_MERCHANT");
        objectMapper.writeValue(response.getOutputStream(), problem);
    }
}
