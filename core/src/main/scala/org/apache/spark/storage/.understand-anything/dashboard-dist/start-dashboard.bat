@echo off
REM Spark Storage - Dashboard 静态化启动脚本 (Windows .bat)
REM 监听 127.0.0.1:9000,优先 Python,fallback 到 npx serve
setlocal

set "SCRIPT_DIR=%~dp0"
set "PORT=9000"
set "HOST=127.0.0.1"
set "URL=http://%HOST%:%PORT%"

cd /d "%SCRIPT_DIR%"

echo [start-dashboard] 启动Dashboard...
echo 时间: %date% %time%
echo 端口: %PORT%
echo 地址: %URL%
echo.

REM 检查端口是否可用
echo [1] 检查端口 %PORT% 是否可用...
netstat -ano | findstr :%PORT% >nul
if %errorlevel%==0 (
    echo [!] 端口 %PORT% 已被占用，正在尝试备用端口...
    
    REM 尝试备用端口9006（test-server.bat使用的端口）
    set "ALT_PORT=9006"
    set "ALT_URL=http://%HOST%:%ALT_PORT%"
    
    netstat -ano | findstr :%ALT_PORT% >nul
    if %errorlevel%==0 (
        echo [!] 备用端口 %ALT_PORT% 也被占用
        echo.
        echo 端口占用情况:
        netstat -ano | findstr :9000 :9006
        echo.
        echo 请运行 diagnose-http.bat 进行详细诊断
        echo 或使用 start-dashboard-alt.bat 尝试其他端口
        goto :error
    ) else (
        echo [√] 备用端口 %ALT_PORT% 可用
        echo [√] 将使用端口 %ALT_PORT%
        set "PORT=%ALT_PORT%"
        set "URL=%ALT_PORT%"
    )
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
echo [3] 启动HTTP服务器...
echo.

where python >nul 2>nul
if %errorlevel%==0 (
    echo [√] 使用Python http.server启动
    start "" "%URL%"
    timeout /t 2 /nobreak >nul
    echo [√] 服务器已启动: %URL%
    echo [√] 请等待几秒钟后刷新浏览器页面
    echo.
    python -m http.server %PORT% --bind %HOST%
    goto :eof
)

where py >nul 2>nul
if %errorlevel%==0 (
    echo [√] 使用py启动器启动
    start "" "%URL%"
    timeout /t 2 /nobreak >nul
    echo [√] 服务器已启动: %URL%
    echo [√] 请等待几秒钟后刷新浏览器页面
    echo.
    py -3 -m http.server %PORT% --bind %HOST%
    goto :eof
)

where npx >nul 2>nul
if %errorlevel%==0 (
    echo [√] 使用npx serve启动
    start "" "%URL%"
    timeout /t 2 /nobreak >nul
    echo [√] 服务器已启动: %URL%
    echo [√] 请等待几秒钟后刷新浏览器页面
    echo.
    npx --yes serve -l %PORT% .
    goto :eof
)

echo [!] 错误: Python和npx都未找到

:error
echo.
echo ================================
echo 启动失败
echo ================================
echo.
echo 可能的解决方案:
echo 1. 安装Python: https://www.python.org/downloads/
echo 2. 安装Node.js (包含npx): https://nodejs.org/
echo 3. 运行 diagnose-http.bat 进行详细诊断
echo 4. 使用 test-server.bat (使用端口9006)
echo 5. 使用 start-dashboard-alt.bat 尝试多个端口

echo.
pause
exit /b 1
