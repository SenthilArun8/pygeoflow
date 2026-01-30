# Changelog

All notable changes to GeoFlow will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2025-01-07

### Added
- Initial release of GeoFlow
- CRS safety: Explicit `target_crs` required for CRS mismatches
- Geometry validation with automatic fixing
- Safe spatial operations: `spatial_join()`, `buffer()`, `overlay()`, `clip()`
- Pipeline orchestration with `@geo_pipeline` decorator
- Task tracking with `@spatial_task` decorator
- Provenance tracking for reproducibility
- Comprehensive test suite (82 tests, 100% pass rate)
- Performance benchmarks showing 15.9% overhead for spatial joins
- Real-world error case studies

### Safety Features
- Prevents buffering in degrees (110,000x error prevention)
- Detects CRS mismatches and requires explicit resolution
- Validates and fixes invalid geometries automatically
- Warns when using geographic CRS for distance operations

### Documentation
- Complete README with installation and usage examples
- API documentation for all public functions
- Real-world case study examples
- Performance benchmark results

[Unreleased]: https://github.com/yourusername/geoflow/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/yourusername/geoflow/releases/tag/v0.1.0
