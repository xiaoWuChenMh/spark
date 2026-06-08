@echo off
REM Spark Storage - Dashboard 静态化启动脚本 (Windows .bat) - 增强版
REM 尝试多个端口: 9000-9005
setlocal

set "SCRIPT_DIR=%~dp0"
set "HOST=127.0.0.1"

cd /d "%SCRIPT_DIR%"

echo [start-dashboard-alt] 正在检查环境...
echo.

REM 检查Python
where python >nul 2>nul
if %errorlevel%==0 (
    echo [√] Python 已安装: python --version
    python --version
) else (
    echo [×] Python 未在PATH中找到
)

echo.

REM 检查py启动器
where py >nul 2>nul
if %errorlevel%==0 (
    echo [√] py 启动器已安装
) else (
    echo [×] py 启动器未在PATH中找到
)

echo.

REM 检查npx
where npx >nul 2>nul
if %errorlevel%==0 (
    echo [√] npx 已安装
    npx --version
) else (
    echo [×] npx 未在PATH中找到
)

echo.
echo [start-dashboard-alt] 正在检查端口可用性...
echo.

REM 尝试多个端口
set "PORTS=9000 9001 9002 9003 9004 9005"

for %%P in (%PORTS%) do (
    echo 尝试端口 %%P...
    
    REM 检查端口是否被占用
    netstat -ano | findstr :%%P >nul
    if %errorlevel%==0 (
        echo [!] 端口 %%P 已被占用，尝试下一个端口
    ) else (
        echo [√] 端口 %%P 可用
        
        REM 优先使用Python
        where python >nul 2>nul
        if %errorlevel%==0 (
            echo [start-dashboard-alt] 使用Python http.server启动在 http://%HOST%:%%P
            start "" "http://%HOST%:%%P"
            python -m http.server %%P --bind %HOST%
            goto :eof
        )
        
        REM 其次使用py启动器
        where py >nul 2>nul
        if %errorlevel%==0 (
            echo [start-dashboard-alt] 使用py启动器启动在 http://%HOST%:%%P
            start "" "http://%HOST%:%%P"
            py -3 -m http.server %%P --bind %HOST%
            goto :eof
        )
        
        REM 最后使用npx serve
        where npx >nul 2>nul
        if %errorlevel%==0 (
            echo [start-dashboard-alt] 使用npx serve启动在 http://%HOST%:%%P
            start "" "http://%HOST%:%%P"
            npx --yes serve -l %%P .
            goto :eof
        )
    )
)

echo.
echo [start-dashboard-alt] 错误: 所有端口都已被占用或无可用运行时环境
echo.
echo 可能的解决方案:
echo 1. 安装Python (https://www.python.org/downloads/)
echo 2. 安装Node.js (https://nodejs.org/)
echo 3. 关闭占用端口 %%P 的程序
echo 4. 使用管理员权限运行此脚本
echo.

exit /b 1