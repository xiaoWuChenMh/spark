@echo off
REM Spark Storage - Dashboard 服务停止脚本 (Windows .bat)
setlocal

echo ==================================================
echo Dashboard 服务停止工具
echo ==================================================
echo.

echo 正在扫描占用端口 9000、9001、9002、9003、9004、9005、9006 的进程...
echo.

REM 定义要检查的端口列表
set "PORTS=9000 9001 9002 9003 9004 9005 9006"
set "PID_FOUND=0"

for %%P in (%PORTS%) do (
    echo 检查端口 %%P...
    
    REM 检查端口是否被占用
    for /f "tokens=5" %%a in ('netstat -ano ^| findstr :%%P ^| findstr LISTENING') do (
        set "PID=%%a"
        if "!PID!" neq "" (
            echo [×] 端口 %%P 被进程 PID !PID! 占用
            
            REM 获取进程详细信息
            echo 正在获取进程信息...
            wmic process where "ProcessId=!PID!" get ProcessId,Name,CommandLine,ExecutablePath /format:list >nul 2>nul
            if !errorlevel!==0 (
                for /f "tokens=2 delims==" %%i in ('wmic process where "ProcessId=!PID!" get Name /format:list ^| findstr Name') do (
                    set "PROCESS_NAME=%%i"
                    echo    进程名称: !PROCESS_NAME!
                )
            ) else (
                echo    无法获取进程详细信息
            )
            
            REM 询问用户是否停止该进程
            echo.
            set /p "STOP_PROCESS=是否停止此进程 (PID !PID!)? [Y/N]: "
            if /i "!STOP_PROCESS!"=="Y" (
                echo 正在停止进程 PID !PID!...
                taskkill /F /PID !PID! >nul 2>nul
                if !errorlevel!==0 (
                    echo [√] 进程 PID !PID! 已成功停止
                ) else (
                    echo [!] 无法停止进程 PID !PID!，可能需要管理员权限
                    echo     请以管理员身份重新运行此脚本
                )
            ) else (
                echo [√] 保持进程运行
            )
            set "PID_FOUND=1"
            echo.
        )
    )
    
    REM 如果没有找到进程
    set "PID="
    set "STOP_PROCESS="
    for /f "tokens=5" %%a in ('netstat -ano ^| findstr :%%P ^| findstr LISTENING') do set "PID=%%a"
    if "!PID!"=="" (
        echo [√] 端口 %%P 未被占用
    )
    echo.
)

echo.
echo ==================================================
if "!PID_FOUND!"=="1" (
    echo 清理完成！
    echo 可能需要重新启动浏览器才能访问本地服务
) else (
    echo 未发现正在运行的Dashboard服务
    echo 端口 9000-9006 均未被占用
)
echo ==================================================
echo.

echo 操作方法总结:
echo 1. 如果服务仍在运行但此脚本未能停止，请尝试:
echo    - 以管理员身份运行此脚本
    - 手动使用: taskkill /F /IM python.exe
    - 手动使用: taskkill /F /IM node.exe
    - 手动使用: taskkill /F /IM py.exe

echo 2. 快速停止方法（在运行服务的命令行窗口）:
echo    - 按 Ctrl+C 直接停止

echo 3. 浏览器相关:
echo    - 清除浏览器缓存: Ctrl+Shift+Delete
    - 使用无痕模式访问

echo.
echo 如需再次启动服务，请运行: start-dashboard.bat
echo 或: test-server.bat (端口9006)
echo.
pause