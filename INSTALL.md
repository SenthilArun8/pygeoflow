# Installation Guide

## From PyPI (Recommended)

Once published, install GeoFlow using pip:

```bash
pip install geoflow
```

### With Development Tools

```bash
pip install geoflow[dev]
```

This includes pytest, pytest-cov, pytest-benchmark, and other development tools.

## From Source

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/geoflow.git
cd geoflow
```

### 2. Create Virtual Environment (Recommended)

```bash
# Using venv
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Or using conda
conda create -n geoflow python=3.11
conda activate geoflow
```

### 3. Install in Development Mode

```bash
# Install with dev dependencies
pip install -e ".[dev]"

# Or just the package
pip install -e .
```

## System Requirements

- **Python**: 3.9 or higher
- **Operating System**: Windows, macOS, Linux
- **Memory**: 512MB minimum (more for large datasets)

## Dependencies

GeoFlow automatically installs these dependencies:

- **geopandas** >= 0.14.0 - Geospatial data handling
- **shapely** >= 2.0.0 - Geometric operations
- **pyproj** >= 3.5.0 - Coordinate system transformations
- **fiona** >= 1.9.0 - File I/O
- **networkx** >= 3.0 - Graph operations for pipelines
- **pydantic** >= 2.0.0 - Data validation
- **loguru** >= 0.7.0 - Logging

## Verify Installation

```python
# Test import
from geoflow import spatial_join, buffer, geo_pipeline

# Run simple test
import geopandas as gpd
from shapely.geometry import Point

gdf = gpd.GeoDataFrame(
    {'name': ['A']},
    geometry=[Point(0, 0)],
    crs='EPSG:4326'
)

result = buffer(gdf.to_crs('EPSG:32610'), distance=100)
print(f"Buffer created: {len(result)} features")
```

Expected output:
```
Buffer created: 1 features
```

## Run Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=geoflow

# Run benchmarks
pytest tests/benchmarks/
```

## Troubleshooting

### Import Error: No module named 'geoflow'

Make sure you're in the virtual environment where you installed it:
```bash
source venv/bin/activate  # or venv\Scripts\activate on Windows
pip list | grep geoflow
```

### Dependency Conflicts

If you have conflicts with existing geopandas installation:
```bash
pip install --upgrade geopandas shapely
pip install geoflow
```

### GDAL/Fiona Installation Issues

On Windows, use conda for easier GDAL management:
```bash
conda install -c conda-forge geopandas
pip install geoflow
```

On Linux:
```bash
sudo apt-get install gdal-bin libgdal-dev
pip install geoflow
```

On macOS:
```bash
brew install gdal
pip install geoflow
```

### Permission Errors

Use `--user` flag:
```bash
pip install --user geoflow
```

## Updating

```bash
# Update to latest version
pip install --upgrade geoflow

# Update to specific version
pip install geoflow==0.2.0
```

## Uninstall

```bash
pip uninstall geoflow
```

## Next Steps

- Read the [README](README.md) for usage examples
- Check [examples/](examples/) for real-world case studies
- Run [examples/case_study_real_errors.py](examples/case_study_real_errors.py) to see GeoFlow in action
