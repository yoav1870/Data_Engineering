@echo off
echo 🚀 Generating dimension CSV files on your host machine...

REM Check if Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo ❌ Python is not installed or not in PATH
    echo Please install Python 3.9+ and try again
    pause
    exit /b 1
)

REM Install required packages
echo 📦 Installing required packages...
pip install pandas numpy

REM Change to dimensions directory
cd dimensions

REM Run the data generator
echo 💾 Generating dimension data...
python dimension_data_generator.py

REM Check if files were created
echo 📋 Checking generated files...
if exist "colors.csv" (
    echo ✅ colors.csv - OK
) else (
    echo ❌ colors.csv - Missing
)

if exist "branches.csv" (
    echo ✅ branches.csv - OK
) else (
    echo ❌ branches.csv - Missing
)

if exist "employees.csv" (
    echo ✅ employees.csv - OK
) else (
    echo ❌ employees.csv - Missing
)

if exist "customers.csv" (
    echo ✅ customers.csv - OK
) else (
    echo ❌ customers.csv - Missing
)

if exist "treatments.csv" (
    echo ✅ treatments.csv - OK
) else (
    echo ❌ treatments.csv - Missing
)

if exist "date_dim.csv" (
    echo ✅ date_dim.csv - OK
) else (
    echo ❌ date_dim.csv - Missing
)

echo.
echo 🎉 CSV generation complete!
echo 📁 Files are in: %cd%
echo.
echo Next steps:
echo 1. Run: docker-compose up -d
echo 2. Check MinIO at: http://localhost:9001
echo 3. Check logs: docker logs dimension-data-copier
echo.
pause 