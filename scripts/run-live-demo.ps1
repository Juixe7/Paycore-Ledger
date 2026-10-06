# =============================================================================
# PayCore End-to-End Live Interactive Demo Script (scripts/run-live-demo.ps1)
# =============================================================================
# Usage:
#   .\scripts\run-live-demo.ps1 [-BaseUrl "http://localhost:8081"] [-StartInfra]
# =============================================================================

param(
    [string]$BaseUrl = "http://localhost:8081",
    [switch]$StartInfra = $false
)

$ErrorActionPreference = "Stop"

Write-Host "==================================================================" -ForegroundColor Cyan
Write-Host "   PayCore: Live Payment Orchestration & Double-Entry Ledger Demo " -ForegroundColor Cyan
Write-Host "==================================================================" -ForegroundColor Cyan

if ($StartInfra) {
    Write-Host "`n[0/7] Starting Docker Compose Lite Stack (Postgres 16, Redis 7, Redpanda, Prometheus, Grafana)..." -ForegroundColor Yellow
    docker compose -f deploy/docker-compose.lite.yml up -d
    Start-Sleep -Seconds 5
}

Write-Host "`n[1/7] Onboarding Merchant ('Acme Footwear India', 2.00% = 200 bps fee)..." -ForegroundColor Yellow
$merchantBody = @{
    name       = "Acme Footwear India"
    email      = "treasury@acmefootwear.in"
    feeBps     = 200
    webhookUrl = "https://example.com/webhooks/paycore"
} | ConvertTo-Json

$merchant = Invoke-RestMethod -Method Post -Uri "$BaseUrl/v1/merchants" `
    -ContentType "application/json" `
    -Body $merchantBody

$apiKey = $merchant.apiKey
Write-Host "  -> Merchant ID : $($merchant.id)" -ForegroundColor Green
Write-Host "  -> Raw API Key : $apiKey" -ForegroundColor Green

$runSuffix = [Guid]::NewGuid().ToString().Substring(0, 8)
$createIdemKey = "demo-create-$runSuffix"

Write-Host "`n[2/7] Creating Payment of INR 1,000.00 (100,000 paise) + Testing Idempotent Replay..." -ForegroundColor Yellow
$createBody = @{
    amount      = 100000
    currency    = "INR"
    metadata    = @{ order_id = "ord_$runSuffix" }
} | ConvertTo-Json

$payment1 = Invoke-WebRequest -Method Post -Uri "$BaseUrl/v1/payments" `
    -Headers @{ "X-API-Key" = $apiKey; "Idempotency-Key" = $createIdemKey } `
    -ContentType "application/json" `
    -Body $createBody

$paymentObj = $payment1.Content | ConvertFrom-Json
$paymentId = $paymentObj.id
Write-Host "  -> First Call   : HTTP $($payment1.StatusCode), paymentId=$paymentId, status=$($paymentObj.status), Idempotent-Replayed=$($payment1.Headers['Idempotent-Replayed'])" -ForegroundColor Green

$paymentReplay = Invoke-WebRequest -Method Post -Uri "$BaseUrl/v1/payments" `
    -Headers @{ "X-API-Key" = $apiKey; "Idempotency-Key" = $createIdemKey } `
    -ContentType "application/json" `
    -Body $createBody
Write-Host "  -> Replay Call  : HTTP $($paymentReplay.StatusCode), Idempotent-Replayed=$($paymentReplay.Headers['Idempotent-Replayed']) (Zero duplicate DB row!)" -ForegroundColor Green

Write-Host "`n[3/7] Authorizing Payment (Two-Phase Authorize -> External PSP Hold)..." -ForegroundColor Yellow
$auth = Invoke-RestMethod -Method Post -Uri "$BaseUrl/v1/payments/$paymentId/authorize" `
    -Headers @{ "X-API-Key" = $apiKey; "Idempotency-Key" = "demo-auth-$runSuffix" } `
    -ContentType "application/json" `
    -Body "{}"
Write-Host "  -> Status       : $($auth.status) | PSP Ref: $($auth.pspRef)" -ForegroundColor Green

Write-Host "`n[4/7] Capturing Payment (Posts 3-Leg Double-Entry Ledger Transaction)..." -ForegroundColor Yellow
$capture = Invoke-RestMethod -Method Post -Uri "$BaseUrl/v1/payments/$paymentId/capture" `
    -Headers @{ "X-API-Key" = $apiKey; "Idempotency-Key" = "demo-cap-$runSuffix" } `
    -ContentType "application/json" `
    -Body "{}"
Write-Host "  -> Status       : $($capture.status) | Captured: $($capture.capturedAmount) paise" -ForegroundColor Green

Write-Host "`n[5/7] Issuing Partial Refund of INR 200.00 (20,000 paise)..." -ForegroundColor Yellow
$refundBody = @{
    amount = 20000
    reason = "Customer returned 1 item"
} | ConvertTo-Json
$refund = Invoke-RestMethod -Method Post -Uri "$BaseUrl/v1/payments/$paymentId/refunds" `
    -Headers @{ "X-API-Key" = $apiKey; "Idempotency-Key" = "demo-ref-$runSuffix" } `
    -ContentType "application/json" `
    -Body $refundBody
Write-Host "  -> Status       : $($refund.status) | Refunded So Far: $($refund.refundedAmount) paise" -ForegroundColor Green

Write-Host "`n[6/7] Inspecting Double-Entry Ledger Postings for Payment $paymentId..." -ForegroundColor Yellow
$ledger = Invoke-RestMethod -Method Get -Uri "$BaseUrl/v1/payments/$paymentId/ledger" `
    -Headers @{ "X-API-Key" = $apiKey }
$ledger | ConvertTo-Json -Depth 6 | Write-Host -ForegroundColor Gray

Write-Host "`n[7/7] Auditing Global Double-Entry Ledger Invariants (/v1/admin/invariants)..." -ForegroundColor Yellow
$invariants = Invoke-RestMethod -Method Get -Uri "$BaseUrl/v1/admin/invariants" `
    -Headers @{ "X-API-Key" = $apiKey }
$invariants | ConvertTo-Json -Depth 5 | Write-Host -ForegroundColor Green

Write-Host "`n==================================================================" -ForegroundColor Cyan
Write-Host "   Demo Complete! Global Ledger Drift = $($invariants.globalDriftPaise) paise " -ForegroundColor Cyan
Write-Host "==================================================================" -ForegroundColor Cyan
