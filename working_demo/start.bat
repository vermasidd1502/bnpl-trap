@echo off
cd /d "%~dp0"
echo ============================================================
echo  Apollo Hermes x BearWatch -- WORKING DEMO
echo  Installing dependencies (first run only) ...
echo ============================================================
python -m pip install -q -r requirements.txt
echo.
echo  Launching server at http://127.0.0.1:5000/
echo  (Browser will auto-open. Ctrl+C to stop.)
echo ============================================================
python app.py
pause
