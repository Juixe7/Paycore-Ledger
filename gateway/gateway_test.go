package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestGatewayRoutingAuthRateLimitAndTracing(t *testing.T) {
	var lastPaymentsTraceparent string
	var lastReadModelTraceparent string

	paymentsUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lastPaymentsTraceparent = r.Header.Get("traceparent")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"upstream":"payments-core","path":"` + r.URL.Path + `"}`))
	}))
	defer paymentsUpstream.Close()

	readModelUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lastReadModelTraceparent = r.Header.Get("traceparent")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"upstream":"read-model-service","path":"` + r.URL.Path + `"}`))
	}))
	defer readModelUpstream.Close()

	webhookUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"upstream":"webhook-service"}`))
	}))
	defer webhookUpstream.Close()

	reconUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"upstream":"reconciliation"}`))
	}))
	defer reconUpstream.Close()

	gw, err := NewGateway(GatewayConfig{
		PaymentsCoreURL:   paymentsUpstream.URL,
		WebhookServiceURL: webhookUpstream.URL,
		ReadModelURL:      readModelUpstream.URL,
		ReconciliationURL: reconUpstream.URL,
		RateLimitRPS:      1,
		RateLimitBurst:    3,
		MaxBodyBytes:      256,
		UpstreamTimeout:   2 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewGateway failed: %v", err)
	}

	gatewayServer := httptest.NewServer(gw)
	defer gatewayServer.Close()

	client := gatewayServer.Client()

	// 1. Missing X-API-Key on protected route -> 401 RFC 7807 ProblemDetail
	reqNoAuth, _ := http.NewRequest(http.MethodPost, gatewayServer.URL+"/v1/payments", strings.NewReader(`{"amount":1000}`))
	respNoAuth, err := client.Do(reqNoAuth)
	if err != nil {
		t.Fatal(err)
	}
	if respNoAuth.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", respNoAuth.StatusCode)
	}

	// 2. Missing X-Idempotency-Key on POST /v1/payments -> 400 RFC 7807 ProblemDetail
	reqNoIdem, _ := http.NewRequest(http.MethodPost, gatewayServer.URL+"/v1/payments", strings.NewReader(`{"amount":1000}`))
	reqNoIdem.Header.Set("X-API-Key", "pk_test_1")
	respNoIdem, err := client.Do(reqNoIdem)
	if err != nil {
		t.Fatal(err)
	}
	if respNoIdem.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing X-Idempotency-Key, got %d", respNoIdem.StatusCode)
	}

	// 3. Valid POST /v1/payments -> routes to payments-core and injects W3C traceparent
	reqCreate, _ := http.NewRequest(http.MethodPost, gatewayServer.URL+"/v1/payments", strings.NewReader(`{"amount":1000,"currency":"INR"}`))
	reqCreate.Header.Set("X-API-Key", "pk_test_routing")
	reqCreate.Header.Set("X-Idempotency-Key", "idem-gw-1")
	respCreate, err := client.Do(reqCreate)
	if err != nil {
		t.Fatal(err)
	}
	if respCreate.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 from payments-core, got %d", respCreate.StatusCode)
	}
	if !strings.HasPrefix(lastPaymentsTraceparent, "00-") {
		t.Fatalf("expected valid W3C traceparent propagated to payments-core, got %q", lastPaymentsTraceparent)
	}

	// 4. GET /v1/payments (collection) -> routes to CQRS read-model-service and preserves traceparent
	customTrace := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	reqList, _ := http.NewRequest(http.MethodGet, gatewayServer.URL+"/v1/payments?limit=10", nil)
	reqList.Header.Set("X-API-Key", "pk_test_routing")
	reqList.Header.Set("traceparent", customTrace)
	respList, err := client.Do(reqList)
	if err != nil {
		t.Fatal(err)
	}
	if respList.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from read-model-service, got %d", respList.StatusCode)
	}
	if lastReadModelTraceparent != customTrace {
		t.Fatalf("expected custom traceparent %q preserved, got %q", customTrace, lastReadModelTraceparent)
	}
	var listBody map[string]string
	_ = json.NewDecoder(respList.Body).Decode(&listBody)
	if listBody["upstream"] != "read-model-service" {
		t.Fatalf("expected upstream read-model-service, got %v", listBody)
	}

	// 5. Payload exceeding MaxBodyBytes (256 bytes) -> 413 Request Entity Too Large
	bigPayload := bytes.Repeat([]byte("a"), 512)
	reqBig, _ := http.NewRequest(http.MethodPost, gatewayServer.URL+"/v1/payments", bytes.NewReader(bigPayload))
	reqBig.Header.Set("X-API-Key", "pk_test_size")
	reqBig.Header.Set("X-Idempotency-Key", "idem-big")
	respBig, err := client.Do(reqBig)
	if err != nil {
		t.Fatal(err)
	}
	if respBig.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413 for oversized body, got %d", respBig.StatusCode)
	}

	// 6. Token-bucket rate limit enforcement (burst = 3 -> 4th immediate request returns 429 + Retry-After)
	for i := 1; i <= 4; i++ {
		reqRL, _ := http.NewRequest(http.MethodGet, gatewayServer.URL+"/v1/payments", nil)
		reqRL.Header.Set("X-API-Key", "pk_test_ratelimit_burst")
		respRL, err := client.Do(reqRL)
		if err != nil {
			t.Fatal(err)
		}
		if i <= 3 && respRL.StatusCode != http.StatusOK {
			t.Fatalf("request %d within burst should succeed (200), got %d", i, respRL.StatusCode)
		}
		if i == 4 {
			if respRL.StatusCode != http.StatusTooManyRequests {
				t.Fatalf("4th request exceeding burst should return 429, got %d", respRL.StatusCode)
			}
			if respRL.Header.Get("Retry-After") == "" {
				t.Fatalf("expected Retry-After header on 429 response")
			}
		}
	}

	// 7. Stale rate-limit bucket eviction prevents unbounded memory growth
	evicted := gw.evictStaleBuckets(time.Now().Add(10 * time.Minute))
	if evicted < 1 {
		t.Fatalf("expected stale rate-limit buckets to be evicted, got %d", evicted)
	}
}
