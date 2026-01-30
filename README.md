# ProvFlow

A reproducibility framework for geospatial analysis. Prevents common GIS errors and tracks everything you do automatically.

> **Note:** This package provides a safety wrapper around GeoPandas with automatic provenance tracking. Unrelated to other "geoflow" packages on PyPI.

[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## The Problem

GeoPandas is powerful but easy to mess up:
- Buffer in WGS84? **110,000x error** (degrees vs meters)
- CRS mismatch in spatial join? **Silent failures**
- Invalid geometries? **Crashes or wrong results**
- Reproducibility? **Good luck explaining what you did 6 months later**

## The Solution

ProvFlow wraps GeoPandas operations with automatic safety checks and provenance tracking.

```python
from provflow import load, save, buffer, spatial_join, geo_pipeline

@geo_pipeline(name="my_analysis", track_provenance=True)
def analyze_flood_risk(parcels_path, zones_path):
    parcels = load(parcels_path, validate=True, auto_fix=True)
    zones = load(zones_path)

    # Automatic CRS alignment, no silent errors
    at_risk = spatial_join(parcels, zones, target_crs='EPSG:32610')

    return at_risk

result = analyze_flood_risk.run("parcels.gpkg", "zones.gpkg")
save(result.result, "output.gpkg", provenance=result.provenance.to_dict())
```

**What you get:**
- CRS mismatches caught before they cause errors
- Invalid geometries detected and fixed
- Complete provenance metadata embedded in output file
- Publication-ready documentation of what you did

## Installation

```bash
pip install provflow
```

Or from source:
```bash
git clone https://github.com/SenthilArun8/pyGeoflow.git
cd pyGeoflow
pip install -e .
```

## Quick Examples

### CRS Safety
```python
from provflow import spatial_join

# This FAILS with helpful error:
result = spatial_join(gdf_wgs84, gdf_utm)
# ValueError: CRS mismatch. You must specify target_crs.

# This works:
result = spatial_join(gdf_wgs84, gdf_utm, target_crs='EPSG:32610')
```

### Geographic CRS Warnings
```python
from provflow import buffer

buffered = buffer(gdf_wgs84, distance=0.01)
# ⚠️  WARNING: Buffering in geographic CRS (EPSG:4326).
#     Results will be in degrees, not meters!
#     Consider reprojecting to a projected CRS first.
```

### Geometry Validation
```python
from provflow import load, validate_geometry

gdf = load("messy_data.shp", validate=True, auto_fix=True)
# Found 12 invalid geometries
# Fixing 12 invalid geometries using 'make_valid' method...
# Successfully fixed all 12 invalid geometries
```

### Provenance Tracking
```python
from provflow import geo_pipeline, spatial_task, load, buffer

@geo_pipeline(name="buffer_analysis", track_provenance=True)
def analyze(roads_path):
    roads = load(roads_path).to_crs('EPSG:32610')
    return buffer(roads, distance=100)

result = analyze.run("roads.gpkg")
result.save_provenance("analysis.json")
# Saves: operations, timing, environment, data signatures
```

## Core Functions

**I/O:**
- `load(filepath, validate=False, auto_fix=False)` - Load GeoJSON/Shapefile/GeoPackage with validation
- `save(gdf, filepath, provenance=None)` - Save with embedded provenance metadata

**Spatial Operations (CRS-safe):**
- `spatial_join(left, right, target_crs=None)` - Join with automatic CRS alignment
- `buffer(gdf, distance)` - Buffer with geographic CRS warnings
- `overlay(gdf1, gdf2, how='intersection', target_crs=None)` - Overlay operations
- `clip(gdf, mask, target_crs=None)` - Clip to boundary

**Validation:**
- `validate_geometry(gdf, auto_fix=False)` - Find and fix invalid geometries

**Decorators:**
- `@geo_pipeline(name, track_provenance=True)` - Track complete workflow
- `@spatial_task(name, validate_crs=True)` - Add validation to functions

## Real Data Test Results

Tested on messy real-world data with self-intersections, bow-ties, CRS mismatches:
- ✅ Fixed 100% of invalid geometries (6/6)
- ✅ Prevented CRS mismatch errors
- ✅ Detected 97.7x buffering error (geographic vs projected)
- ✅ Full provenance tracking

See [REAL_DATA_TEST_RESULTS.md](REAL_DATA_TEST_RESULTS.md) for details.

## Examples

Check out [examples/](examples/) for working code:
- `simple_example.py` - Basic operations
- `pipeline_example.py` - Complete workflow with provenance
- `case_study_real_errors.py` - Real-world error handling
- `reproducible_pipeline_complete.py` - Full closed-loop example

## Testing

```bash
pytest tests/ -v                    # Run all tests
pytest tests/ --cov=provflow        # With coverage (tests use 'geoflow' internally)
pytest tests/benchmarks/            # Performance tests
```

**Status:** 75 tests passing, 91.36% coverage

## How It Works

ProvFlow is a thin safety wrapper around GeoPandas:
1. **CRS Manager** - Forces explicit target_crs on mismatches
2. **Geometry Validator** - Uses Shapely's make_valid to fix invalid geometries
3. **Provenance Tracker** - Logs operations with timestamps and metadata
4. **Pipeline Decorator** - Orchestrates tasks and captures execution history

No magic, just sensible defaults and forced explicitness where it matters.

## Why I Built This

Got tired of:
- Debugging spatial joins that silently reprojected to the first CRS
- Buffering in WGS84 and getting wildly wrong results
- Trying to remember what operations I ran for a paper 6 months ago
- Invalid geometries causing cryptic crashes

Built ProvFlow to solve these problems for my own research. Open sourced in case it helps others.

## Contributing

Found a bug or want to add a feature? Open an issue or PR. This is a solo project so response times may vary.

## License

MIT License - see [LICENSE](LICENSE) file.

## Citation

If you use ProvFlow in research:

```bibtex
@software{provflow2025,
  title = {ProvFlow: Reproducible Geospatial Pipeline Framework with Provenance Tracking},
  author = {Senthil Kirthieswar},
  year = {2025},
  url = {https://github.com/SenthilArun8/pyGeoflow}
}
```

---

**Note:** This is research software. It works for my use cases and passes tests, but your mileage may vary. If something breaks, open an issue with a reproducible example.
