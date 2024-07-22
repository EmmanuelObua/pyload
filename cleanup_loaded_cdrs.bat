@echo off
REM Change directory to where your script is located
cd "C:\Scheduled tasks\GeneralEtlPython"

REM Activate virtual environment
call venv\Scripts\activate

REM Run the Python script
python cleanup_loaded_cdrs.py

pause