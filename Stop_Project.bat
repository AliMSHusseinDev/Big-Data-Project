@echo off
setlocal
cd /d "%~dp0"
echo End the simulation and wait for Completed in the dashboard to finalize a report.
docker compose stop
set "result=%ERRORLEVEL%"
pause
exit /b %result%
