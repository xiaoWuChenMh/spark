# Spark Storage - Dashboard 静态化启动脚本 (PowerShell)
# 监听 127.0.0.1:9000,优先 Python,fallback 到 npx serve
$ErrorActionPreference = 'Stop'

$ScriptDir   = Split-Path -Parent $MyInvocation.MyCommand.Path
$Port        = 9000
$HostAddr    = '127.0.0.1'
$Url         = "http://${HostAddr}:${Port}"

Set-Location $ScriptDir

function Open-Browser($u) {
    try { Start-Process $u | Out-Null } catch { }
}

if (Get-Command python -ErrorAction SilentlyContinue) {
    Write-Host "[start-dashboard] Using Python http.server on $Url" -ForegroundColor Cyan
    Open-Browser $Url
    python -m http.server $Port --bind $HostAddr
    return
}

if (Get-Command py -ErrorAction SilentlyContinue) {
    Write-Host "[start-dashboard] Using py launcher + http.server on $Url" -ForegroundColor Cyan
    Open-Browser $Url
    py -3 -m http.server $Port --bind $HostAddr
    return
}

if (Get-Command npx -ErrorAction SilentlyContinue) {
    Write-Host "[start-dashboard] Falling back to npx serve on $Url" -ForegroundColor Yellow
    Open-Browser $Url
    npx --yes serve -l $Port .
    return
}

Write-Host "[start-dashboard] ERROR: neither python nor npx found in PATH." -ForegroundColor Red
exit 1
