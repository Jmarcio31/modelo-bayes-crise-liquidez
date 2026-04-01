@echo off
cd /d C:\liquidez-dashboard

REM Subir servidor local
python -m http.server 8000

pause