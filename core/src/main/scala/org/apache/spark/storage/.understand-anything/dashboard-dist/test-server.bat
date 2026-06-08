@echo off
REM 简单的HTTP服务器测试脚本
setlocal

set "SCRIPT_DIR=%~dp0"
set "PORT=9006"  REM 使用新的端口避免冲突
set "HOST=127.0.0.1"
set "URL=http://%HOST%:%PORT%"

cd /d "%SCRIPT_DIR%"

echo ================================
echo 简单HTTP服务器测试
echo ================================
echo 时间: %date% %time%
echo 端口: %PORT%
echo 地址: %URL%
echo.

REM 检查端口是否可用
echo [1] 检查端口 %PORT% 是否可用...
netstat -ano | findstr :%PORT% >nul
if %errorlevel%==0 (
    echo [!] 端口 %PORT% 已被占用，请关闭占用端口的程序
    netstat -ano | findstr :%PORT%
    goto :error
) else (
    echo [√] 端口 %PORT% 可用
)

echo.
echo [2] 检查静态文件...
if not exist "index.html" (
    echo [!] index.html 不存在！
    goto :error
) else (
    echo [√] index.html 存在
)

if not exist "knowledge-graph.json" (
    echo [√] knowledge-graph.json 不存在（可选文件）
) else (
    echo [√] knowledge-graph.json 存在
)

echo.
echo [3] 启动简单的Python HTTP服务器...
echo.

echo 在另一个窗口中，浏览器将自动打开: %URL%
echo 如果浏览器报错，请等待几秒钟后刷新页面。
echo 服务器日志将显示在下方，按 Ctrl+C 停止服务器。
echo.

echo ------------------------------
echo 等待5秒让服务器完全启动...
echo ------------------------------

REM 尝试使用Python
where python >nul 2>nul
if %errorlevel%==0 (
    start "" "%URL%"
    timeout /t 2 /nobreak >nul
    echo [√] 使用 Python 启动服务器
    echo [√] 服务器地址: %URL%
    echo [√] 按 Ctrl+C 停止服务器
    echo.
    python -m http.server %PORT% --bind %HOST%
    goto :eof
)

REM 尝试使用py启动器
where py >nul 2>nul
if %errorlevel%==0 (
    start "" "%URL%"
    timeout /t 2 /nobreak >nul
    echo [√] 使用 py 启动器启动服务器
    echo [√] 服务器地址: %URL%
    echo [√] 按 Ctrl+C 停止服务器
    echo.
    py -3 -m http.server %PORT% --bind %HOST%
    goto :eof
)

echo [!] Python 和 py 都未找到

echo.
echo [4] 备选方案：使用PowerShell启动...
echo.

echo 注意：PowerShell服务器可能没有Python稳定
echo.

start "" "%URL%"
timeout /t 2 /nobreak >nul
powershell -Command "$listener = New-Object System.Net.HttpListener; $listener.Prefixes.Add('http://%HOST%:%PORT%/'); $listener.Start(); Write-Host '[√] PowerShell HTTP服务器已启动: %URL%'; while ($true) { $context = $listener.GetContext(); $response = $context.Response; $content = Get-Content 'index.html' -Raw; $buffer = [System.Text.Encoding]::UTF8.GetBytes($content); $response.ContentLength64 = $buffer.Length; $response.OutputStream.Write($buffer, 0, $buffer.Length); $response.Close() }"

goto :eof

:error
echo.
echo ================================
echo 测试失败
echo ================================
echo.
echo 可能的解决方案:
echo 1. 安装Python: https://www.python.org/downloads/
echo 2. 关闭占用端口 %PORT% 的程序
echo 3. 运行 diagnose-http.bat 进行详细诊断
echo 4. 使用 start-dashboard-alt.bat 尝试其他端口
echo.
pause
exit /b 1