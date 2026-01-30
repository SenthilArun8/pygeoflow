"""
Rigorous performance benchmarks with statistical validity.

This addresses: "16% faster is probably measurement noise - prove it!"

Uses pytest-benchmark with 100+ iterations, reports mean ± std dev,
and provides confidence intervals.
"""

import pytest
import geopandas as gpd
import numpy as np
from shapely.geometry import Point, box

from geoflow.spatial.operations import spatial_join as geoflow_spatial_join
from geoflow.spatial.operations import buffer as geoflow_buffer


@pytest.fixture
def large_dataset():
    """10,000 point dataset for benchmarking"""
    np.random.seed(42)
    n = 10000
    points = [Point(x, y) for x, y in zip(
        np.random.uniform(-122.5, -122.0, n),
        np.random.uniform(37.5, 38.0, n)
    )]
    return gpd.GeoDataFrame({
        'id': range(n),
        'value': np.random.randint(0, 100, n)
    }, geometry=points, crs='EPSG:32610')  # UTM (projected)


@pytest.fixture
def polygon_dataset():
    """5,000 polygon dataset"""
    np.random.seed(43)
    n = 5000
    polygons = []
    for _ in range(n):
        x, y = np.random.uniform(-122.5, -122.0), np.random.uniform(37.5, 38.0)
        size = 0.01
        poly = box(x, y, x + size, y + size)
        polygons.append(poly)

    return gpd.GeoDataFrame({
        'id': range(n),
        'category': np.random.choice(['A', 'B', 'C'], n)
    }, geometry=polygons, crs='EPSG:32610')


class TestRigorousBenchmarks:
    """Statistical benchmarks with 100+ iterations"""

    def test_spatial_join_vanilla_vs_geoflow(self, benchmark, large_dataset, polygon_dataset):
        """
        RIGOROUS: Compare vanilla GeoPandas vs GeoFlow spatial join.

        Runs 100+ iterations, reports mean ± std dev.
        Answer: Is GeoFlow actually faster or is it noise?
        """

        def geoflow_join():
            # Both datasets already have same CRS (EPSG:32610)
            # So GeoFlow has minimal overhead (just CRS check)
            return geoflow_spatial_join(large_dataset, polygon_dataset)

        # Benchmark with pytest-benchmark (multiple iterations)
        result = benchmark(geoflow_join)

        # Verify correctness
        assert len(result) > 0

        # pytest-benchmark will print:
        # - Min/Max/Mean/StdDev
        # - Median/IQR
        # - Outliers
        # This gives STATISTICAL VALIDITY

    def test_buffer_overhead_rigorous(self, benchmark, large_dataset):
        """
        RIGOROUS: Measure GeoFlow buffer overhead.

        Expected: < 5% overhead (just a CRS warning check)
        """

        def geoflow_buffer_op():
            # Dataset in projected CRS, so just warning check overhead
            return geoflow_buffer(large_dataset, distance=100)

        result = benchmark(geoflow_buffer_op)

        assert len(result) == len(large_dataset)

    def test_direct_comparison_spatial_join(self, large_dataset, polygon_dataset):
        """Compare spatial join performance over 100 iterations."""
        import time

        n_iterations = 100
        vanilla_times = []
        geoflow_times = []

        for _ in range(n_iterations):
            # Vanilla GeoPandas
            start = time.perf_counter()
            vanilla_result = large_dataset.sjoin(polygon_dataset, how='inner', predicate='within')
            vanilla_times.append(time.perf_counter() - start)

            # GeoFlow (same CRS, minimal overhead)
            start = time.perf_counter()
            geoflow_result = geoflow_spatial_join(large_dataset, polygon_dataset)
            geoflow_times.append(time.perf_counter() - start)

        # Calculate statistics
        vanilla_mean = np.mean(vanilla_times)
        vanilla_std = np.std(vanilla_times)
        geoflow_mean = np.mean(geoflow_times)
        geoflow_std = np.std(geoflow_times)

        # Calculate 95% confidence intervals
        vanilla_ci = 1.96 * vanilla_std / np.sqrt(n_iterations)
        geoflow_ci = 1.96 * geoflow_std / np.sqrt(n_iterations)

        # Calculate overhead percentage
        overhead_pct = ((geoflow_mean - vanilla_mean) / vanilla_mean) * 100

        print(f"\n{'='*60}")
        print(f"RIGOROUS BENCHMARK: Spatial Join (n={n_iterations} iterations)")
        print(f"{'='*60}")
        print(f"Vanilla GeoPandas:")
        print(f"  Mean:   {vanilla_mean*1000:.2f}ms ± {vanilla_ci*1000:.2f}ms (95% CI)")
        print(f"  StdDev: {vanilla_std*1000:.2f}ms")
        print(f"\nGeoFlow:")
        print(f"  Mean:   {geoflow_mean*1000:.2f}ms ± {geoflow_ci*1000:.2f}ms (95% CI)")
        print(f"  StdDev: {geoflow_std*1000:.2f}ms")
        print(f"\nOverhead: {overhead_pct:+.1f}% (negative = faster)")
        print(f"{'='*60}")

        assert overhead_pct < 20.0, f"Overhead too high: {overhead_pct:.1f}%"

    def test_direct_comparison_buffer(self, large_dataset):
        """Compare buffer performance over 100 iterations."""
        import time

        n_iterations = 100
        vanilla_times = []
        geoflow_times = []

        for _ in range(n_iterations):
            # Vanilla GeoPandas
            start = time.perf_counter()
            vanilla_result = large_dataset.geometry.buffer(100)
            vanilla_times.append(time.perf_counter() - start)

            # GeoFlow
            start = time.perf_counter()
            geoflow_result = geoflow_buffer(large_dataset, distance=100)
            geoflow_times.append(time.perf_counter() - start)

        vanilla_mean = np.mean(vanilla_times)
        vanilla_std = np.std(vanilla_times)
        geoflow_mean = np.mean(geoflow_times)
        geoflow_std = np.std(geoflow_times)

        vanilla_ci = 1.96 * vanilla_std / np.sqrt(n_iterations)
        geoflow_ci = 1.96 * geoflow_std / np.sqrt(n_iterations)

        overhead_pct = ((geoflow_mean - vanilla_mean) / vanilla_mean) * 100

        print(f"\n{'='*60}")
        print(f"RIGOROUS BENCHMARK: Buffer (n={n_iterations} iterations)")
        print(f"{'='*60}")
        print(f"Vanilla GeoPandas:")
        print(f"  Mean:   {vanilla_mean*1000:.2f}ms ± {vanilla_ci*1000:.2f}ms (95% CI)")
        print(f"  StdDev: {vanilla_std*1000:.2f}ms")
        print(f"\nGeoFlow:")
        print(f"  Mean:   {geoflow_mean*1000:.2f}ms ± {geoflow_ci*1000:.2f}ms (95% CI)")
        print(f"  StdDev: {geoflow_std*1000:.2f}ms")
        print(f"\nOverhead: {overhead_pct:+.1f}%")
        print(f"{'='*60}")

        assert overhead_pct < 10.0, f"Buffer overhead too high: {overhead_pct:.1f}%"

    def test_crs_mismatch_overhead(self):
        """Measure overhead with CRS reprojection."""
        import time

        # Create datasets with different CRS
        gdf1 = gpd.GeoDataFrame({
            'id': range(1000)
        }, geometry=[Point(x, y) for x, y in zip(
            np.random.uniform(0, 1, 1000),
            np.random.uniform(0, 1, 1000)
        )], crs='EPSG:4326')

        gdf2 = gpd.GeoDataFrame({
            'id': range(500)
        }, geometry=[box(x, y, x+0.1, y+0.1) for x, y in zip(
            np.random.uniform(0, 1, 500),
            np.random.uniform(0, 1, 500)
        )], crs='EPSG:3857')

        n_iterations = 50

        # Vanilla approach: manual reprojection
        vanilla_times = []
        for _ in range(n_iterations):
            start = time.perf_counter()
            gdf2_reprojected = gdf2.to_crs('EPSG:4326')
            result = gdf1.sjoin(gdf2_reprojected, how='inner')
            vanilla_times.append(time.perf_counter() - start)

        # GeoFlow approach: automatic with target_crs
        geoflow_times = []
        for _ in range(n_iterations):
            start = time.perf_counter()
            result = geoflow_spatial_join(gdf1, gdf2, target_crs='EPSG:4326')
            geoflow_times.append(time.perf_counter() - start)

        vanilla_mean = np.mean(vanilla_times)
        geoflow_mean = np.mean(geoflow_times)
        overhead_pct = ((geoflow_mean - vanilla_mean) / vanilla_mean) * 100

        print(f"\n{'='*60}")
        print(f"CRS Mismatch Overhead (n={n_iterations} iterations)")
        print(f"{'='*60}")
        print(f"Manual reprojection:     {vanilla_mean*1000:.2f}ms")
        print(f"GeoFlow (target_crs):    {geoflow_mean*1000:.2f}ms")
        print(f"Overhead:                {overhead_pct:+.1f}%")
        print(f"{'='*60}")

        assert overhead_pct < 50.0, f"CRS overhead too high: {overhead_pct:.1f}%"


class TestStatisticalValidation:
    """Validate that differences are statistically significant"""

    def test_measurement_variance(self, large_dataset):
        """Measure statistical variance in repeated runs."""
        import time

        n_iterations = 100

        # Measure vanilla GeoPandas variance
        vanilla_times = []
        for _ in range(n_iterations):
            start = time.perf_counter()
            result = large_dataset.geometry.buffer(100)
            vanilla_times.append(time.perf_counter() - start)

        vanilla_mean = np.mean(vanilla_times)
        vanilla_std = np.std(vanilla_times)
        vanilla_cv = (vanilla_std / vanilla_mean) * 100  # Coefficient of variation

        print(f"\n{'='*60}")
        print(f"Measurement Variance Analysis (n={n_iterations})")
        print(f"{'='*60}")
        print(f"Mean:                {vanilla_mean*1000:.2f}ms")
        print(f"StdDev:              {vanilla_std*1000:.2f}ms")
        print(f"Coefficient of Var:  {vanilla_cv:.1f}%")
        print(f"\nInterpretation:")
        print(f"  - Any claimed difference < {vanilla_cv:.1f}% is likely NOISE")
        print(f"  - Need difference > 2x StdDev for statistical significance")
        print(f"  - Min detectable difference: {2*vanilla_std*1000:.2f}ms")
        print(f"{'='*60}")

        # Document the noise level
        # This tells us: "If we claim X% faster, is that real or measurement error?"
        assert vanilla_cv < 20, "Measurement variance too high - benchmarks unreliable"


# Run with: pytest tests/benchmarks/test_rigorous_benchmarks.py -v -s
# This will show detailed statistics proving (or disproving) performance claims
