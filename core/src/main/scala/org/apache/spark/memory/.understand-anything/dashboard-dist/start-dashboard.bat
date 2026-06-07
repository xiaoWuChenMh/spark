@echo off
REM Spark Storage - Dashboard 静态化启动脚本 (Windows .bat)
REM 监听 127.0.0.1:9000,优先 Python,fallback 到 npx serve
setlocal

set "SCRIPT_DIR=%~dp0"
set "PORT=9000"
set "HOST=127.0.0.1"

cd /d "%SCRIPT_DIR%"

where python >nul 2>nul
if %errorlevel%==0 (
    echo [start-dashboard] Using Python http.server on http://%HOST%:%PORT%
    start "" "http://%HOST%:%PORT%"
    python -m http.server %PORT% --bind %HOST%
    goto :eof
)

where py >nul 2>nul
if %errorlevel%==0 (
    echo [start-dashboard] Using py launcher + http.server on http://%HOST%:%PORT%
    start "" "http://%HOST%:%PORT%"
    py -3 -m http.server %PORT% --bind %HOST%
    goto :eof
)

where npx >nul 2>nul
if %errorlevel%==0 (
    echo [start-dashboard] Falling back to npx serve on http://%HOST%:%PORT%
    start "" "http://%HOST%:%PORT%"
    npx --yes serve -l %PORT% .
    goto :eof
)

echo [start-dashboard] ERROR: neither python nor npx found in PATH.
exit /b 1
