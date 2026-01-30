@echo off
REM Build and check package for PyPI upload (Windows)
REM Usage: scripts\build_and_check.bat

echo ======================================
echo GeoFlow Package Build ^& Check
echo ======================================

REM Step 1: Clean old builds
echo.
echo [1/6] Cleaning old builds...
if exist dist rmdir /s /q dist
if exist build rmdir /s /q build
if exist geoflow.egg-info rmdir /s /q geoflow.egg-info

REM Step 2: Run tests
echo.
echo [2/6] Running tests...
python -m pytest tests/unit -v --tb=short
if errorlevel 1 (
    echo ERROR: Tests failed! Fix tests before building.
    exit /b 1
)

REM Step 3: Build package
echo.
echo [3/6] Building package...
python -m build
if errorlevel 1 (
    echo ERROR: Build failed!
    exit /b 1
)

REM Step 4: Check package with twine
echo.
echo [4/6] Checking package with twine...
python -m twine check dist/*
if errorlevel 1 (
    echo ERROR: Package check failed!
    exit /b 1
)

REM Step 5: List built files
echo.
echo [5/6] Built files:
dir dist

REM Step 6: Test local install
echo.
echo [6/6] Testing local installation...
python -m venv .test_install_env
call .test_install_env\Scripts\activate.bat
pip install dist\*.whl
python -c "from geoflow import spatial_join, buffer, geo_pipeline; print('[OK] Package imports successfully!')"
if errorlevel 1 (
    call deactivate
    rmdir /s /q .test_install_env
    echo ERROR: Import test failed!
    exit /b 1
)
call deactivate
rmdir /s /q .test_install_env

echo.
echo ======================================
echo [OK] Package ready for upload!
echo ======================================
echo.
echo Next steps:
echo   1. Test upload:    python -m twine upload --repository testpypi dist/*
echo   2. Production:     python -m twine upload dist/*
echo.
