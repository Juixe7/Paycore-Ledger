# =============================================================================
# PayCore Automated Backup -> Scratch Restore -> Ledger Invariant Verification
# (Section 9.3 Backup & Disaster Recovery Verification)
# =============================================================================
param(
    [string]$ContainerName = "paycore-postgres",
    [string]$DbUser = "paycore",
    [string]$SourceDb = "paycore",
    [string]$ScratchDb = "paycore_restore_verify"
)

$ErrorActionPreference = "Stop"

Write-Host "==> [1/4] Dumping source database '$SourceDb' from container '$ContainerName'..." -ForegroundColor Cyan
$dumpFile = Join-Path $env:TEMP "paycore_backup_$(Get-Date -Format 'yyyyMMdd_HHmmss').sql"
docker exec $ContainerName pg_dump -U $DbUser -d $SourceDb --no-owner --no-privileges > $dumpFile

Write-Host "==> [2/4] Recreating scratch database '$ScratchDb'..." -ForegroundColor Cyan
docker exec $ContainerName psql -U $DbUser -d postgres -c "DROP DATABASE IF EXISTS $ScratchDb;"
docker exec $ContainerName psql -U $DbUser -d postgres -c "CREATE DATABASE $ScratchDb;"

Write-Host "==> [3/4] Restoring backup into '$ScratchDb'..." -ForegroundColor Cyan
Get-Content $dumpFile | docker exec -i $ContainerName psql -U $DbUser -d $ScratchDb -v ON_ERROR_STOP=1 | Out-Null

Write-Host "==> [4/4] Running REPEATABLE READ double-entry ledger invariant check on restored DB..." -ForegroundColor Cyan
$invariantSql = @"
SELECT
    COALESCE(SUM(CASE WHEN direction = 'DEBIT'  THEN amount ELSE 0 END), 0) -
    COALESCE(SUM(CASE WHEN direction = 'CREDIT' THEN amount ELSE 0 END), 0) AS global_imbalance
FROM ledger_entries;
"@

$imbalance = (docker exec $ContainerName psql -U $DbUser -d $ScratchDb -t -A -c $invariantSql).Trim()
if ($imbalance -ne "0") {
    throw "CRITICAL: Restored backup failed ledger invariant check! global_imbalance=$imbalance"
}

Write-Host "PASS: Restored database '$ScratchDb' verified with global_imbalance = 0 paise." -ForegroundColor Green
Remove-Item $dumpFile -ErrorAction SilentlyContinue
