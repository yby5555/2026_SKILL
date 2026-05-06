@echo off
setlocal

REM 功能说明：
REM 1. 固定使用项目内的 .venv310 Python
REM 2. 直接启动 flow_task_runtime\consumer.py
REM 3. 失败时暂停窗口，方便查看报错

set "ROOT_DIR=%~dp0.."
set "PYTHON_EXE=%ROOT_DIR%\.venv310\Scripts\python.exe"
set "CONSUMER_PY=%~dp0consumer.py"

if not exist "%PYTHON_EXE%" (
    echo [ERROR] Python not found: %PYTHON_EXE%
    pause
    exit /b 1
)

"%PYTHON_EXE%" "%CONSUMER_PY%"
set "EXIT_CODE=%ERRORLEVEL%"

if not "%EXIT_CODE%"=="0" (
    echo.
    echo [ERROR] Consumer exited with code %EXIT_CODE%
    pause
)

exit /b %EXIT_CODE%
