# Failure-recovery demo (Windows PowerShell).
# Run from the project root after:  docker compose up -d --build
#
#   1. upload files
#   2. kill a storage node that holds a copy
#   3. watch the worker detect the failure and rebuild the missing copies
#   4. download and verify the file is byte-for-byte intact
#   5. bring the node back and watch the extra copies get trimmed

$ErrorActionPreference = "Stop"
$Api = "http://localhost:8080"
$H = @{ "X-Api-Key" = "dev-key" }

function Show-Cluster {
    (Invoke-RestMethod "$Api/admin/cluster" -Headers $H).nodes | Format-Table id, status, replicas
}

Write-Host "`n== Waiting for 4 storage nodes to register ==" -ForegroundColor Cyan
do {
    Start-Sleep 2
    try { $up = @((Invoke-RestMethod "$Api/admin/cluster" -Headers $H).nodes | Where-Object status -eq "UP").Count } catch { $up = 0 }
    Write-Host "  $up UP"
} while ($up -lt 4)

Write-Host "`n== Uploading 20 files + demo.bin (5 MB) ==" -ForegroundColor Cyan
$tmp = Join-Path $env:TEMP "minis3-demo.bin"
$bytes = New-Object byte[] (5MB); (New-Object Random).NextBytes($bytes)
[IO.File]::WriteAllBytes($tmp, $bytes)
$original = (Get-FileHash $tmp -Algorithm SHA256).Hash.ToLower()
Invoke-RestMethod -Method Put "$Api/buckets/demo/objects/demo.bin" -Headers $H -InFile $tmp -ContentType "application/octet-stream" | Format-List
1..20 | ForEach-Object {
    Invoke-RestMethod -Method Put "$Api/buckets/demo/objects/files/file-$_.txt" -Headers $H -Body "hello from file $_" -ContentType "text/plain" | Out-Null
}
Start-Sleep 2
Show-Cluster

$rep = Invoke-RestMethod "$Api/admin/replicas/demo/demo.bin" -Headers $H
Write-Host "demo.bin is stored on: $($rep.replicas.id -join ', ')"
$victim = $rep.replicas[0].id
$service = $victim -replace "node-", "storage-"

Write-Host "`n== Killing $service ($victim) ==" -ForegroundColor Red
docker compose stop $service | Out-Null

Write-Host "Waiting ~15s: 6s heartbeat timeout + repair pass..."
Start-Sleep 15
Show-Cluster
$rep = Invoke-RestMethod "$Api/admin/replicas/demo/demo.bin" -Headers $H
$rep.replicas | Format-Table id, status

Write-Host "`n== Downloading demo.bin with $victim still dead ==" -ForegroundColor Cyan
$out = Join-Path $env:TEMP "minis3-demo-download.bin"
$resp = Invoke-WebRequest "$Api/buckets/demo/objects/demo.bin" -Headers $H -OutFile $out -PassThru
$downloaded = (Get-FileHash $out -Algorithm SHA256).Hash.ToLower()
Write-Host "served by : $($resp.Headers['X-Served-By'])"
Write-Host "original  : $original"
Write-Host "downloaded: $downloaded"
if ($original -eq $downloaded) { Write-Host "MATCH - no data lost" -ForegroundColor Green } else { Write-Host "MISMATCH" -ForegroundColor Red; exit 1 }

Write-Host "`n== Bringing $service back ==" -ForegroundColor Cyan
docker compose start $service | Out-Null
Write-Host "Waiting ~15s for it to rejoin and for GC to trim the now-extra copies..."
Start-Sleep 15
Show-Cluster
Write-Host "Every object should be back to exactly 3 copies. Worker log:" -ForegroundColor Cyan
docker compose logs worker --tail 25
