#!/bin/bash
# Build and check package for PyPI upload
# Usage: bash scripts/build_and_check.sh

set -e  # Exit on error

echo "======================================"
echo "GeoFlow Package Build & Check"
echo "======================================"

# Step 1: Clean old builds
echo ""
echo "[1/6] Cleaning old builds..."
rm -rf dist/ build/ *.egg-info geoflow.egg-info

# Step 2: Run tests
echo ""
echo "[2/6] Running tests..."
python -m pytest tests/unit -v --tb=short || {
    echo "ERROR: Tests failed! Fix tests before building."
    exit 1
}

# Step 3: Build package
echo ""
echo "[3/6] Building package..."
python -m build || {
    echo "ERROR: Build failed!"
    exit 1
}

# Step 4: Check package with twine
echo ""
echo "[4/6] Checking package with twine..."
python -m twine check dist/* || {
    echo "ERROR: Package check failed!"
    exit 1
}

# Step 5: List built files
echo ""
echo "[5/6] Built files:"
ls -lh dist/

# Step 6: Test local install
echo ""
echo "[6/6] Testing local installation..."
python -m venv .test_install_env
source .test_install_env/bin/activate || .test_install_env\Scripts\activate
pip install dist/*.whl
python -c "from geoflow import spatial_join, buffer, geo_pipeline; print('[OK] Package imports successfully!')" || {
    deactivate
    rm -rf .test_install_env
    echo "ERROR: Import test failed!"
    exit 1
}
deactivate
rm -rf .test_install_env

echo ""
echo "======================================"
echo "[OK] Package ready for upload!"
echo "======================================"
echo ""
echo "Next steps:"
echo "  1. Test upload:    python -m twine upload --repository testpypi dist/*"
echo "  2. Production:     python -m twine upload dist/*"
echo ""
