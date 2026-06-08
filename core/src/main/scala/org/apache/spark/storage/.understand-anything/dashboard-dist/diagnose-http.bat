@echo off
REM HTTP服务器诊断脚本 - 解决ERR_INVALID_HTTP_RESPONSE错误
setlocal

set "SCRIPT_DIR=%~dp0"
set "HOST=127.0.0.1"
set "PORTS=9000 9001 9002 9003 9004 9005"
set "DIAGNOSIS_FILE=%SCRIPT_DIR%http_diagnosis.txt"

cd /d "%SCRIPT_DIR%"

echo ================================
echo HTTP服务器诊断报告
echo ================================
echo 诊断时间: %date% %time%
echo 当前目录: %SCRIPT_DIR%
echo.

REM 清理旧的诊断文件
del "%DIAGNOSIS_FILE%" 2>nul

echo [1/5] 检查端口占用情况... >> "%DIAGNOSIS_FILE%"
echo. >> "%DIAGNOSIS_FILE%"

for %%P in (%PORTS%) do (
    netstat -ano | findstr :%%P >nul
    if !errorlevel!==0 (
        echo 端口 %%P 已被占用 >> "%DIAGNOSIS_FILE%"
        netstat -ano | findstr :%%P >> "%DIAGNOSIS_FILE%"
        echo. >> "%DIAGNOSIS_FILE%"
    ) else (
        echo 端口 %%P 可用 >> "%DIAGNOSIS_FILE%"
    )
)

echo. >> "%DIAGNOSIS_FILE%"
echo [2/5] 检查静态文件是否存在... >> "%DIAGNOSIS_FILE%"
echo. >> "%DIAGNOSIS_FILE%"

if exist "index.html" (
    echo √ index.html 存在 (大小: %~z0) >> "%DIAGNOSIS_FILE%"
) else (
    echo × index.html 不存在 >> "%DIAGNOSIS_FILE%"
)

if exist "knowledge-graph.json" (
    echo √ knowledge-graph.json 存在 >> "%DIAGNOSIS_FILE%"
) else (
    echo × knowledge-graph.json 不存在 >> "%DIAGNOSIS_FILE%"
)

if exist "config.json" (
    echo √ config.json 存在 >> "%DIAGNOSIS_FILE%"
) else (
    echo × config.json 不存在 >> "%DIAGNOSIS_FILE%"
)

echo. >> "%DIAGNOSIS_FILE%"
echo [3/5] 测试HTTP服务器响应... >> "%DIAGNOSIS_FILE%"
echo. >> "%DIAGNOSIS_FILE%"

REM 检查是否有curl可用
where curl >nul 2>nul
if !errorlevel!==0 (
    echo 使用curl进行HTTP测试... >> "%DIAGNOSIS_FILE%"
    
    for %%P in (%PORTS%) do (
        echo 测试 http://%HOST%:%%P/ ... >> "%DIAGNOSIS_FILE%"
        curl -v -s -m 5 "http://%HOST%:%%P/" --output curl_test_%%P.html 2>> "%DIAGNOSIS_FILE%"
        echo. >> "%DIAGNOSIS_FILE%"
        
        if exist "curl_test_%%P.html" (
            echo √ 端口 %%P 返回了响应 >> "%DIAGNOSIS_FILE%"
            echo 响应大小: %~z0 字节 >> "%DIAGNOSIS_FILE%"
            del "curl_test_%%P.html" 2>nul
        ) else (
            echo × 端口 %%P 无响应或超时 >> "%DIAGNOSIS_FILE%"
        )
        echo. >> "%DIAGNOSIS_FILE%"
    )
) else (
    REM 如果没有curl，尝试使用PowerShell
    echo 使用PowerShell进行HTTP测试... >> "%DIAGNOSIS_FILE%"
    
    for %%P in (%PORTS%) do (
        echo 测试 http://%HOST%:%%P/ ... >> "%DIAGNOSIS_FILE%"
        powershell -Command "try {$response = Invoke-WebRequest -Uri 'http://%HOST%:%%P/' -TimeoutSec 3; Write-Output ('√ HTTP状态码: ' + $response.StatusCode); Write-Output ('√ 内容类型: ' + $response.Headers['Content-Type'])} catch {Write-Output ('× 错误: ' + $_.Exception.Message)}" >> "%DIAGNOSIS_FILE%" 2>&1
        echo. >> "%DIAGNOSIS_FILE%"
    )
)

echo. >> "%DIAGNOSIS_FILE%"
echo [4/5] 检查Windows防火墙设置... >> "%DIAGNOSIS_FILE%"
echo. >> "%DIAGNOSIS_FILE%"

echo 检查防火墙规则: >> "%DIAGNOSIS_FILE%"
netsh advfirewall firewall show rule name=all | findstr /C:"9000" /C:"localhost" >> "%DIAGNOSIS_FILE%" 2>nul || echo 未找到相关防火墙规则 >> "%DIAGNOSIS_FILE%"

echo. >> "%DIAGNOSIS_FILE%"
echo [5/5] 生成解决方案建议... >> "%DIAGNOSIS_FILE%"
echo. >> "%DIAGNOSIS_FILE%"

echo 可能的解决方案: >> "%DIAGNOSIS_FILE%"
echo 1. 如果端口被占用，请关闭占用端口的程序或使用其他端口 >> "%DIAGNOSIS_FILE%"
echo 2. 如果防火墙阻止访问，请添加例外规则: >> "%DIAGNOSIS_FILE%"
echo     netsh advfirewall firewall add rule name="Dashboard HTTP" dir=in action=allow protocol=TCP localport=9000-9005 >> "%DIAGNOSIS_FILE%"
echo 3. 如果HTTP服务器启动但响应无效，尝试清理浏览器缓存 >> "%DIAGNOSIS_FILE%"
echo 4. 使用 start-dashboard-alt.bat 尝试不同端口 >> "%DIAGNOSIS_FILE%"
echo 5. 以管理员身份运行启动脚本 >> "%DIAGNOSIS_FILE%"

echo. >> "%DIAGNOSIS_FILE%"
echo ================================
echo 诊断完成！请查看 %DIAGNOSIS_FILE%
echo ================================

echo.
type "%DIAGNOSIS_FILE%"
echo.
echo 诊断报告已保存到: %DIAGNOSIS_FILE%
echo.
echo 请根据诊断报告中的建议进行修复。

pause