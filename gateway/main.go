package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// GatewayConfig holds upstream service URLs and edge protection limits.
type GatewayConfig struct {
	ListenAddr        string
	PaymentsCoreURL   string
	WebhookServiceURL string
	ReadModelURL      string
	ReconciliationURL string
	RateLimitRPS      float64
	RateLimitBurst    int
	MaxBodyBytes      int64
	UpstreamTimeout   time.Duration
}

type tokenBucket struct {
	mu         sync.Mutex
	tokens     float64
	lastRefill time.Time
}

// Gateway implements the PayCore Edge Gateway (Section 1.2 Stretch 4 & Section 4.1).
type Gateway struct {
	cfg             GatewayConfig
	buckets         sync.Map
	paymentsProxy   *httputil.ReverseProxy
	webhookProxy    *httputil.ReverseProxy
	readModelProxy  *httputil.ReverseProxy
	reconProxy      *httputil.ReverseProxy
	totalRequests   atomic.Uint64
	rateLimitedReqs atomic.Uint64
}

type problemDetail struct {
	Type      string `json:"type"`
	Title     string `json:"title"`
	Status    int    `json:"status"`
	Detail    string `json:"detail"`
	Instance  string `json:"instance"`
	ErrorCode string `json:"errorCode"`
	Timestamp string `json:"timestamp"`
}

func NewGateway(cfg GatewayConfig) (*Gateway, error) {
	if cfg.RateLimitRPS <= 0 {
		cfg.RateLimitRPS = 100
	}
	if cfg.RateLimitBurst <= 0 {
		cfg.RateLimitBurst = 200
	}
	if cfg.MaxBodyBytes <= 0 {
		cfg.MaxBodyBytes = 1024 * 1024 // 1 MiB
	}
	if cfg.UpstreamTimeout <= 0 {
		cfg.UpstreamTimeout = 10 * time.Second
	}

	paymentsProxy, err := newReverseProxy(cfg.PaymentsCoreURL, cfg.UpstreamTimeout)
	if err != nil {
		return nil, fmt.Errorf("invalid payments-core URL: %w", err)
	}
	webhookProxy, err := newReverseProxy(cfg.WebhookServiceURL, cfg.UpstreamTimeout)
	if err != nil {
		return nil, fmt.Errorf("invalid webhook-service URL: %w", err)
	}
	readModelProxy, err := newReverseProxy(cfg.ReadModelURL, cfg.UpstreamTimeout)
	if err != nil {
		return nil, fmt.Errorf("invalid read-model-service URL: %w", err)
	}
	reconProxy, err := newReverseProxy(cfg.ReconciliationURL, cfg.UpstreamTimeout)
	if err != nil {
		return nil, fmt.Errorf("invalid reconciliation URL: %w", err)
	}

	gw := &Gateway{
		cfg:            cfg,
		paymentsProxy:  paymentsProxy,
		webhookProxy:   webhookProxy,
		readModelProxy: readModelProxy,
		reconProxy:     reconProxy,
	}
	go gw.startBucketEvictionLoop(5 * time.Minute)
	return gw, nil
}

func (g *Gateway) startBucketEvictionLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for now := range ticker.C {
		g.evictStaleBuckets(now)
	}
}

func (g *Gateway) evictStaleBuckets(now time.Time) int {
	idleTTLSeconds := math.Max(60.0, 10.0*float64(g.cfg.RateLimitBurst)/g.cfg.RateLimitRPS)
	evicted := 0
	g.buckets.Range(func(key, value any) bool {
		b := value.(*tokenBucket)
		b.mu.Lock()
		idleSec := now.Sub(b.lastRefill).Seconds()
		b.mu.Unlock()
		if idleSec > idleTTLSeconds {
			g.buckets.Delete(key)
			evicted++
		}
		return true
	})
	return evicted
}

func newReverseProxy(rawURL string, timeout time.Duration) (*httputil.ReverseProxy, error) {
	target, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Transport = &http.Transport{
		ResponseHeaderTimeout: timeout,
		MaxIdleConns:          512,
		MaxIdleConnsPerHost:   256,
		IdleConnTimeout:       90 * time.Second,
	}
	return proxy, nil
}

func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	g.totalRequests.Add(1)
	path := r.URL.Path

	// 1. Gateway health, readiness & Prometheus metrics endpoints
	if path == "/healthz" || path == "/readyz" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"UP","service":"paycore-go-gateway"}`))
		return
	}
	if path == "/metrics" {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w,
			"# HELP paycore_gateway_requests_total Total HTTP requests handled by Go gateway\n"+
				"# TYPE paycore_gateway_requests_total counter\n"+
				"paycore_gateway_requests_total %d\n"+
				"# HELP paycore_gateway_rate_limited_total Total HTTP requests rejected with 429 by Go gateway\n"+
				"# TYPE paycore_gateway_rate_limited_total counter\n"+
				"paycore_gateway_rate_limited_total %d\n",
			g.totalRequests.Load(),
			g.rateLimitedReqs.Load())
		return
	}

	// 2. Ensure W3C traceparent header exists and propagate to upstream & response
	traceparent := strings.TrimSpace(r.Header.Get("traceparent"))
	if traceparent == "" {
		traceparent = generateTraceparent()
		r.Header.Set("traceparent", traceparent)
	}
	w.Header().Set("traceparent", traceparent)
	w.Header().Set("X-Trace-Id", extractTraceID(traceparent))

	// 3. Enforce payload size limit at the edge
	if r.ContentLength > g.cfg.MaxBodyBytes {
		writeProblem(w, r, http.StatusRequestEntityTooLarge, "PAYLOAD_TOO_LARGE",
			"Request Payload Too Large",
			fmt.Sprintf("Request body exceeds maximum allowed size of %d bytes", g.cfg.MaxBodyBytes))
		return
	}
	if r.Body != nil {
		r.Body = http.MaxBytesReader(w, r.Body, g.cfg.MaxBodyBytes)
	}

	// 4. Enforce X-API-Key authentication on merchant-scoped routes
	apiKey := strings.TrimSpace(r.Header.Get("X-API-Key"))
	if requiresMerchantAuth(r.Method, path) && apiKey == "" {
		writeProblem(w, r, http.StatusUnauthorized, "MISSING_API_KEY",
			"Unauthorized",
			"Missing required X-API-Key header at edge gateway")
		return
	}

	// 5. Per-merchant (or per-IP) token-bucket rate limiting
	rateKey := apiKey
	if rateKey == "" {
		rateKey = r.RemoteAddr
	}
	allowed, remaining, retryAfterSec := g.allowRequest(rateKey)
	w.Header().Set("X-RateLimit-Limit", strconv.Itoa(g.cfg.RateLimitBurst))
	w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(remaining))
	if !allowed {
		g.rateLimitedReqs.Add(1)
		w.Header().Set("Retry-After", strconv.Itoa(retryAfterSec))
		writeProblem(w, r, http.StatusTooManyRequests, "RATE_LIMIT_EXCEEDED",
			"Too Many Requests",
			"Merchant token-bucket rate limit exceeded; back off and retry after Retry-After seconds")
		return
	}

	// 6. Enforce Idempotency-Key (or X-Idempotency-Key) on state-changing money mutations at the edge
	if requiresIdempotencyKey(r.Method, path) {
		idemKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
		if idemKey == "" {
			idemKey = strings.TrimSpace(r.Header.Get("X-Idempotency-Key"))
			if idemKey != "" {
				r.Header.Set("Idempotency-Key", idemKey)
			}
		}
		if idemKey == "" {
			writeProblem(w, r, http.StatusBadRequest, "MISSING_IDEMPOTENCY_KEY",
				"Missing Idempotency-Key Header",
				"All state-changing payment and payout requests must include Idempotency-Key")
			return
		}
	}

	// 7. Route to appropriate backend microservice (CQRS split + domain routing)
	targetProxy := g.selectUpstreamProxy(r.Method, path)
	targetProxy.ServeHTTP(w, r)
}

func (g *Gateway) selectUpstreamProxy(method, path string) *httputil.ReverseProxy {
	// CQRS Read Model routes
	if (method == http.MethodGet && path == "/v1/payments") ||
		path == "/v1/merchants/me/summary" ||
		strings.HasPrefix(path, "/v1/admin/read-model") {
		return g.readModelProxy
	}
	// Webhook Service routes
	if strings.HasPrefix(path, "/v1/webhook-") ||
		strings.HasPrefix(path, "/v1/admin/webhooks") ||
		strings.HasPrefix(path, "/v1/admin/dlq") ||
		strings.HasPrefix(path, "/v1/admin/consumers") {
		return g.webhookProxy
	}
	// Reconciliation Service routes
	if strings.HasPrefix(path, "/v1/admin/reconciliation") {
		return g.reconProxy
	}
	// Default: Authoritative Payments Core & Ledger Service
	return g.paymentsProxy
}

func (g *Gateway) allowRequest(key string) (bool, int, int) {
	now := time.Now()
	val, _ := g.buckets.LoadOrStore(key, &tokenBucket{
		tokens:     float64(g.cfg.RateLimitBurst),
		lastRefill: now,
	})
	b := val.(*tokenBucket)
	b.mu.Lock()
	defer b.mu.Unlock()

	elapsed := now.Sub(b.lastRefill).Seconds()
	if elapsed > 0 {
		b.tokens = math.Min(float64(g.cfg.RateLimitBurst), b.tokens+elapsed*g.cfg.RateLimitRPS)
		b.lastRefill = now
	}

	if b.tokens >= 1.0 {
		b.tokens -= 1.0
		return true, int(math.Floor(b.tokens)), 0
	}

	deficit := 1.0 - b.tokens
	retrySec := int(math.Ceil(deficit / g.cfg.RateLimitRPS))
	if retrySec < 1 {
		retrySec = 1
	}
	return false, 0, retrySec
}

func requiresMerchantAuth(method, path string) bool {
	if path == "/v1/merchants" && method == http.MethodPost {
		return false
	}
	if strings.HasPrefix(path, "/v1/admin/") {
		return false
	}
	return strings.HasPrefix(path, "/v1/payments") ||
		strings.HasPrefix(path, "/v1/payouts") ||
		strings.HasPrefix(path, "/v1/merchants/me")
}

func requiresIdempotencyKey(method, path string) bool {
	if method != http.MethodPost {
		return false
	}
	return path == "/v1/payments" ||
		path == "/v1/payouts" ||
		(strings.HasPrefix(path, "/v1/payments/") &&
			(strings.HasSuffix(path, "/authorize") ||
				strings.HasSuffix(path, "/capture") ||
				strings.HasSuffix(path, "/void") ||
				strings.HasSuffix(path, "/refunds")))
}

func generateTraceparent() string {
	var traceID [16]byte
	var spanID [8]byte
	_, _ = io.ReadFull(rand.Reader, traceID[:])
	_, _ = io.ReadFull(rand.Reader, spanID[:])
	return fmt.Sprintf("00-%s-%s-01", hex.EncodeToString(traceID[:]), hex.EncodeToString(spanID[:]))
}

func extractTraceID(traceparent string) string {
	parts := strings.Split(traceparent, "-")
	if len(parts) >= 2 && len(parts[1]) == 32 {
		return parts[1]
	}
	return traceparent
}

func writeProblem(w http.ResponseWriter, r *http.Request, status int, code, title, detail string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	body, _ := json.Marshal(problemDetail{
		Type:      "https://api.paycore.internal/errors/" + strings.ToLower(strings.ReplaceAll(code, "_", "-")),
		Title:     title,
		Status:    status,
		Detail:    detail,
		Instance:  r.URL.Path,
		ErrorCode: code,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	})
	_, _ = w.Write(body)
}

func envOrDefault(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func main() {
	rps, _ := strconv.ParseFloat(envOrDefault("RATE_LIMIT_RPS", "200"), 64)
	burst, _ := strconv.Atoi(envOrDefault("RATE_LIMIT_BURST", "400"))

	cfg := GatewayConfig{
		ListenAddr:        envOrDefault("GATEWAY_LISTEN_ADDR", ":8090"),
		PaymentsCoreURL:   envOrDefault("PAYMENTS_CORE_URL", "http://localhost:8080"),
		WebhookServiceURL: envOrDefault("WEBHOOK_SERVICE_URL", "http://localhost:8082"),
		ReadModelURL:      envOrDefault("READ_MODEL_URL", "http://localhost:8083"),
		ReconciliationURL: envOrDefault("RECONCILIATION_URL", "http://localhost:8084"),
		RateLimitRPS:      rps,
		RateLimitBurst:    burst,
		MaxBodyBytes:      1024 * 1024,
		UpstreamTimeout:   10 * time.Second,
	}

	gw, err := NewGateway(cfg)
	if err != nil {
		log.Fatalf("Failed to initialize PayCore Go Edge Gateway: %v", err)
	}

	log.Printf("PayCore Go Edge Gateway listening on %s", cfg.ListenAddr)
	if err := http.ListenAndServe(cfg.ListenAddr, gw); err != nil {
		log.Fatalf("Gateway server exited: %v", err)
	}
}
