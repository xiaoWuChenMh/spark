# Spark RPC - Dashboard 启动脚本 (PowerShell)
# 监听 127.0.0.1:9053
$ErrorActionPreference = 'Stop'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$Port      = 9053
$HostAddr  = '127.0.0.1'
$Url       = "http://${HostAddr}:${Port}"

Set-Location $ScriptDir

function Open-Browser($u) {
    try { Start-Process $u | Out-Null } catch { }
}

if (Get-Command python -ErrorAction SilentlyContinue) {
    Write-Host "[rpc-dashboard] Using Python http.server on $Url" -ForegroundColor Cyan
    Open-Browser $Url
    python -m http.server $Port --bind $HostAddr
    return
}

if (Get-Command py -ErrorAction SilentlyContinue) {
    Write-Host "[rpc-dashboard] Using py launcher + http.server on $Url" -ForegroundColor Cyan
    Open-Browser $Url
    py -3 -m http.server $Port --bind $HostAddr
    return
}

Write-Host "[rpc-dashboard] ERROR: python not found in PATH." -ForegroundColor Red
exit 1
