$ErrorActionPreference = "Continue"

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host " PayCore Pre-Flight Doctor (RAM, Docker, Ports, Tooling)" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

$cs = Get-CimInstance Win32_ComputerSystem
$ramGb = [math]::Round($cs.TotalPhysicalMemory / 1GB, 2)
$cores = $cs.NumberOfLogicalProcessors

Write-Host ("[RAM]   {0} GB total across {1} logical processors" -f $ramGb, $cores)
if ($ramGb -ge 12) {
    Write-Host "        -> Eligible for both 'lite' (4-6 GB) and 'full' (12-16 GB) profiles" -ForegroundColor Green
} elseif ($ramGb -ge 6) {
    Write-Host "        -> Eligible for 'lite' profile (drop Loki/Debezium locally for full)" -ForegroundColor Yellow
} else {
    Write-Host "        -> WARNING: Less than 6 GB RAM detected" -ForegroundColor Red
}

$javaCmd = if ($env:JAVA_HOME -and (Test-Path "$env:JAVA_HOME\bin\java.exe")) { "$env:JAVA_HOME\bin\java.exe" }
           elseif (Test-Path "$env:USERPROFILE\.jdks\temurin-21\bin\java.exe") { "$env:USERPROFILE\.jdks\temurin-21\bin\java.exe" }
           else { "java" }

try {
    $javaVer = & $javaCmd -version 2>&1 | Select-Object -First 1
    Write-Host "[JAVA]  $javaVer" -ForegroundColor Green
} catch {
    Write-Host "[JAVA]  NOT FOUND (Install JDK 21)" -ForegroundColor Red
}

try {
    $dockerVer = docker --version 2>&1
    docker info > $null 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "[DOCKER] $dockerVer (Daemon: RUNNING)" -ForegroundColor Green
    } else {
        Write-Host "[DOCKER] $dockerVer (Daemon: STOPPED - start Docker Desktop)" -ForegroundColor Yellow
    }
} catch {
    Write-Host "[DOCKER] NOT FOUND" -ForegroundColor Red
}

try {
    $goVer = go version 2>&1
    Write-Host "[GO]    $goVer" -ForegroundColor Green
} catch {
    Write-Host "[GO]    NOT FOUND" -ForegroundColor Yellow
}

$ports = @(5432, 6379, 8080, 8081, 8082, 8083, 8084, 9092, 3000)
$busyPorts = @()
foreach ($p in $ports) {
    $conn = Get-NetTCPConnection -LocalPort $p -State Listen -ErrorAction SilentlyContinue
    if ($conn) { $busyPorts += $p }
}
if ($busyPorts.Count -eq 0) {
    Write-Host "[PORTS] All PayCore ports (5432, 6379, 8080-8084, 9092, 3000) are free" -ForegroundColor Green
} else {
    Write-Host ("[PORTS] Active listeners on ports: {0}" -f ($busyPorts -join ", ")) -ForegroundColor Yellow
}
Write-Host "==========================================================" -ForegroundColor Cyan
