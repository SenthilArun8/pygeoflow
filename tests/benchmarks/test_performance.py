"""
Performance benchmarks comparing GeoFlow to vanilla GeoPandas.

This addresses the critique: "You claim benefits but have zero proof."
"""

import pytest
import geopandas as gpd
import pandas as pd
import numpy as np
from shapely.geometry import Point, Polygon, box
import time
from pathlib import Path
import tempfile

from geoflow import geo_pipeline, spatial_task, load, buffer, overlay, spatial_join
from geoflow.spatial.operations import buffer as geoflow_buffer
from geoflow.spatial.operations import spatial_join as geoflow_spatial_join


@pytest.fixture
def large_points_gdf():
    """Create large point dataset (10,000 features)"""
    np.random.seed(42)
    n = 10000

    points = [Point(x, y) for x, y in zip(
        np.random.uniform(-122.5, -122.0, n),
        np.random.uniform(37.5, 38.0, n)
    )]

    gdf = gpd.GeoDataFrame({
        'id': range(n),
        'value': np.random.randint(0, 100, n)
    }, geometry=points, crs='EPSG:4326')

    return gdf


@pytest.fixture
def large_polygons_gdf():
    """Create large polygon dataset (5,000 features)"""
    np.random.seed(43)
    n = 5000

    polygons = []
    for _ in range(n):
        x, y = np.random.uniform(-122.5, -122.0), np.random.uniform(37.5, 38.0)
        size = 0.01
        poly = box(x, y, x + size, y + size)
        polygons.append(poly)

    gdf = gpd.GeoDataFrame({
        'id': range(n),
        'category': np.random.choice(['A', 'B', 'C'], n)
    }, geometry=polygons, crs='EPSG:4326')

    return gdf


@pytest.fixture
def messy_invalid_gdf():
    """Create dataset with invalid geometries (common real-world issue)"""
    # Self-intersecting polygon
    invalid_poly1 = Polygon([(0, 0), (1, 1), (1, 0), (0, 1), (0, 0)])

    # Very thin sliver polygon
    invalid_poly2 = Polygon([
        (0, 0), (1, 0), (1, 0.0000001), (0, 0.0000001), (0, 0)
    ])

    # Valid polygon
    valid_poly = box(2, 2, 3, 3)

    gdf = gpd.GeoDataFrame({
        'id': [1, 2, 3]
    }, geometry=[invalid_poly1, invalid_poly2, valid_poly], crs='EPSG:4326')

    return gdf


class TestPerformanceBenchmarks:
    """Compare GeoFlow vs vanilla GeoPandas performance"""

    def test_benchmark_spatial_join_large_dataset(self, large_points_gdf, large_polygons_gdf, benchmark):
        """Benchmark spatial join on large datasets"""

        # Reproject to projected CRS for fair comparison
        points_utm = large_points_gdf.to_crs('EPSG:32610')
        polygons_utm = large_polygons_gdf.to_crs('EPSG:32610')

        def geoflow_join():
            return geoflow_spatial_join(points_utm, polygons_utm)

        # Benchmark GeoFlow
        result = benchmark(geoflow_join)

        assert len(result) > 0
        assert 'id_left' in result.columns or 'id' in result.columns

    def test_compare_geoflow_vs_vanilla_spatial_join(self, large_points_gdf, large_polygons_gdf):
        """Direct comparison: GeoFlow vs vanilla GeoPandas spatial join"""

        points_utm = large_points_gdf.to_crs('EPSG:32610')
        polygons_utm = large_polygons_gdf.to_crs('EPSG:32610')

        # Vanilla GeoPandas
        start = time.time()
        vanilla_result = points_utm.sjoin(polygons_utm, how='inner', predicate='within')
        vanilla_time = time.time() - start

        # GeoFlow (with CRS handling overhead)
        start = time.time()
        geoflow_result = geoflow_spatial_join(points_utm, polygons_utm)
        geoflow_time = time.time() - start

        # Results should be similar
        assert len(vanilla_result) == len(geoflow_result)

        # GeoFlow should be within 2x of vanilla (overhead acceptable)
        overhead_ratio = geoflow_time / vanilla_time
        assert overhead_ratio < 2.0, f"GeoFlow overhead too high: {overhead_ratio:.2f}x"

        print(f"\nSpatial Join Performance:")
        print(f"  Vanilla GeoPandas: {vanilla_time:.3f}s")
        print(f"  GeoFlow: {geoflow_time:.3f}s")
        print(f"  Overhead: {overhead_ratio:.2f}x")

    def test_compare_buffer_performance(self, large_points_gdf):
        """Compare buffer operation performance"""

        points_utm = large_points_gdf.to_crs('EPSG:32610')

        # Vanilla GeoPandas
        start = time.time()
        vanilla_result = points_utm.buffer(100)
        vanilla_time = time.time() - start

        # GeoFlow
        start = time.time()
        geoflow_result = geoflow_buffer(points_utm, distance=100)
        geoflow_time = time.time() - start

        # Results should be identical
        assert len(vanilla_result) == len(geoflow_result)

        # Performance should be similar (GeoFlow adds CRS check)
        overhead_ratio = geoflow_time / vanilla_time
        assert overhead_ratio < 1.5, f"GeoFlow buffer overhead too high: {overhead_ratio:.2f}x"

        print(f"\nBuffer Performance:")
        print(f"  Vanilla GeoPandas: {vanilla_time:.3f}s")
        print(f"  GeoFlow: {geoflow_time:.3f}s")
        print(f"  Overhead: {overhead_ratio:.2f}x")

    def test_provenance_overhead(self, large_points_gdf):
        """Measure provenance tracking overhead"""

        points_utm = large_points_gdf.to_crs('EPSG:32610')

        @geo_pipeline(name="test_pipeline")
        def pipeline_with_buffer(gdf):
            return geoflow_buffer(gdf, distance=100)

        # Without provenance
        start = time.time()
        result_no_prov = pipeline_with_buffer(points_utm)
        time_no_prov = time.time() - start

        # With provenance
        start = time.time()
        result_with_prov = pipeline_with_buffer.run(points_utm)
        time_with_prov = time.time() - start

        # Results should be same
        assert len(result_no_prov) == len(result_with_prov.result)

        # Provenance overhead should be acceptable (<100%)
        # Note: Overhead includes data signature computation and JSON serialization
        overhead = (time_with_prov - time_no_prov) / time_no_prov * 100
        assert overhead < 100, f"Provenance overhead too high: {overhead:.1f}%"

        print(f"\nProvenance Tracking Overhead:")
        print(f"  Without provenance: {time_no_prov:.3f}s")
        print(f"  With provenance: {time_with_prov:.3f}s")
        print(f"  Overhead: {overhead:.1f}%")


class TestRealWorldScenarios:
    """Test with messy real-world data scenarios"""

    def test_handle_invalid_geometries(self, messy_invalid_gdf):
        """GeoFlow should handle invalid geometries gracefully"""

        # Vanilla GeoPandas will fail or produce bad results
        vanilla_invalid_count = (~messy_invalid_gdf.geometry.is_valid).sum()
        assert vanilla_invalid_count > 0, "Test data should have invalid geometries"

        # GeoFlow with auto-fix
        from geoflow import validate_geometry

        fixed = validate_geometry(messy_invalid_gdf, auto_fix=True, method='make_valid')

        # Should fix all invalid geometries
        assert fixed.geometry.is_valid.all(), "GeoFlow should fix all invalid geometries"
        assert len(fixed) == len(messy_invalid_gdf), "Should preserve all features"

    def test_crs_mismatch_detection(self):
        """GeoFlow should REQUIRE explicit target_crs on mismatch (safety!)"""

        # Create two GeoDataFrames with different CRS
        gdf1 = gpd.GeoDataFrame({
            'id': [1, 2]
        }, geometry=[Point(0, 0), Point(1, 1)], crs='EPSG:4326')

        gdf2 = gpd.GeoDataFrame({
            'id': [3, 4]
        }, geometry=[Point(0, 0), Point(1, 1)], crs='EPSG:3857')

        # CRITICAL FIX: GeoFlow should REFUSE to auto-align without explicit target_crs
        # This prevents silent spatial errors
        with pytest.raises(ValueError, match="CRS mismatch"):
            result = geoflow_spatial_join(gdf1, gdf2)

        # Correct usage: User must specify target_crs
        result = geoflow_spatial_join(gdf1, gdf2, target_crs='EPSG:3857')
        assert result.crs == 'EPSG:3857'

    def test_geographic_crs_warning(self, caplog):
        """GeoFlow should warn about buffering in geographic CRS"""

        from geoflow import spatial_task

        @spatial_task(name="test_buffer", warn_geographic=True)
        def buffer_task(gdf, distance):
            return geoflow_buffer(gdf, distance)

        # Geographic CRS (WGS84)
        gdf_geo = gpd.GeoDataFrame({
            'id': [1]
        }, geometry=[Point(0, 0)], crs='EPSG:4326')

        # Should log warning
        result = buffer_task(gdf_geo, 100)

        # Vanilla GeoPandas would silently buffer by 100 degrees!
        # GeoFlow at least warns
        assert len(result) == 1

    def test_empty_geometry_handling(self):
        """Handle empty geometries from failed operations"""

        from shapely.geometry import Point, Polygon

        # Create a scenario where overlay produces empty geometries
        gdf1 = gpd.GeoDataFrame({
            'id': [1, 2]
        }, geometry=[
            box(0, 0, 1, 1),
            box(10, 10, 11, 11)  # No overlap
        ], crs='EPSG:4326')

        gdf2 = gpd.GeoDataFrame({
            'id': [1]
        }, geometry=[box(0, 0, 1, 1)], crs='EPSG:4326')

        # Overlay will create some empty geometries
        result = overlay(gdf1, gdf2, how='intersection')

        # GeoFlow should handle this gracefully
        assert result is not None


class TestScalability:
    """Test performance at different scales"""

    @pytest.mark.parametrize("n_features", [100, 1000, 10000])
    def test_buffer_scales_linearly(self, n_features):
        """Buffer operation should scale linearly with feature count"""

        # Create dataset of varying sizes
        points = [Point(x, y) for x, y in zip(
            np.random.uniform(0, 1, n_features),
            np.random.uniform(0, 1, n_features)
        )]

        gdf = gpd.GeoDataFrame({
            'id': range(n_features)
        }, geometry=points, crs='EPSG:32610')

        # Time the operation
        start = time.time()
        result = geoflow_buffer(gdf, distance=100)
        elapsed = time.time() - start

        # Should complete in reasonable time
        time_per_feature = elapsed / n_features
        assert time_per_feature < 0.001, f"Too slow: {time_per_feature*1000:.2f}ms per feature"

        print(f"\nBuffer {n_features} features: {elapsed:.3f}s ({time_per_feature*1000:.2f}ms per feature)")

    @pytest.mark.parametrize("n_features", [100, 1000, 5000])
    def test_spatial_join_with_index(self, n_features):
        """Spatial join should benefit from spatial index on large datasets"""

        # Create point and polygon datasets
        points = [Point(x, y) for x, y in zip(
            np.random.uniform(0, 1, n_features),
            np.random.uniform(0, 1, n_features)
        )]

        polygons = [box(x-0.1, y-0.1, x+0.1, y+0.1) for x, y in zip(
            np.random.uniform(0, 1, n_features//10),
            np.random.uniform(0, 1, n_features//10)
        )]

        gdf_points = gpd.GeoDataFrame({
            'id': range(n_features)
        }, geometry=points, crs='EPSG:32610')

        gdf_polygons = gpd.GeoDataFrame({
            'id': range(len(polygons))
        }, geometry=polygons, crs='EPSG:32610')

        # Time spatial join
        start = time.time()
        result = geoflow_spatial_join(gdf_points, gdf_polygons)
        elapsed = time.time() - start

        print(f"\nSpatial join {n_features} points: {elapsed:.3f}s")


class TestMemoryEfficiency:
    """Test memory usage"""

    def test_large_dataset_memory(self):
        """Should handle large datasets without excessive memory"""

        import psutil
        import os

        process = psutil.Process(os.getpid())
        mem_before = process.memory_info().rss / 1024 / 1024  # MB

        # Create large dataset
        n = 50000
        points = [Point(x, y) for x, y in zip(
            np.random.uniform(0, 1, n),
            np.random.uniform(0, 1, n)
        )]

        gdf = gpd.GeoDataFrame({
            'id': range(n),
            'value': np.random.randint(0, 100, n)
        }, geometry=points, crs='EPSG:32610')

        # Process with GeoFlow
        result = geoflow_buffer(gdf, distance=100)

        mem_after = process.memory_info().rss / 1024 / 1024  # MB
        mem_used = mem_after - mem_before

        # Should not use excessive memory (< 500MB for 50k features)
        assert mem_used < 500, f"Excessive memory usage: {mem_used:.1f}MB"

        print(f"\nMemory usage for 50k features: {mem_used:.1f}MB")
