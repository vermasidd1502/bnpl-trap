@echo off
REM ===================================================================
REM  BNPL pod -- daily driver wrapper for Windows Task Scheduler.
REM  Runs the ingest chain, refreshes BSI, restarts the pod, cleans disk.
REM  All real logging happens inside daily_driver.py (logs\daily\).
REM ===================================================================

set "POD_DIR=C:\Users\siddh\Desktop\spring 2026\580\BNPL\bnpl-pod"
set "PYTHON=C:\Users\siddh\AppData\Local\Programs\Python\Python314\python.exe"

cd /d "%POD_DIR%"
"%PYTHON%" "%POD_DIR%\scripts\daily_driver.py"

exit /b 0
