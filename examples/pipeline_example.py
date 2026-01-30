"""
Demonstration of @geo_pipeline decorator with provenance tracking.

This is the DEMO - showing what makes GeoFlow unique.
"""

from pathlib import Path
import geopandas as gpd
from shapely.geometry import Point, Polygon

# Import GeoFlow decorators and operations
from geoflow import (
    geo_pipeline,
    spatial_task,
    load,
    buffer,
    overlay,
    clip,
    validate_geometry
)


# Example 1: Simple Pipeline with Provenance
@geo_pipeline(
    name="buffer_analysis",
    description="Create 100m buffers around road features",
    auto_save_provenance=True,
    provenance_dir="outputs/provenance"
)
def analyze_road_buffers(roads_path: str, output_path: str):
    """
    Simple pipeline demonstrating automatic provenance tracking.
    """

    @spatial_task(name="load_roads", validate_geometries=True)
    def load_data(path):
        return load(path, validate=True, auto_fix=True)

    @spatial_task(name="reproject_utm", warn_geographic=True)
    def reproject(roads):
        return roads.to_crs('EPSG:32610')  # UTM Zone 10N

    @spatial_task(name="create_buffer", strict_crs=True)
    def create_buffer(roads):
        # This will ERROR if roads are in geographic CRS (safety!)
        return buffer(roads, distance=100)

    # Execute pipeline steps
    print("Loading roads...")
    roads = load_data(roads_path)
    print(f"  Loaded {len(roads)} features")

    print("Reprojecting to UTM...")
    roads_utm = reproject(roads)
    print(f"  Reprojected to {roads_utm.crs}")

    print("Creating 100m buffer...")
    buffered = create_buffer(roads_utm)
    print(f"  Created {len(buffered)} buffers")

    # Save results
    buffered.to_file(output_path)
    print(f"Saved to {output_path}")

    return buffered


# Example 2: Complex Multi-Dataset Pipeline
@geo_pipeline(
    name="parcel_zoning_analysis",
    description="Analyze parcels within residential zones using overlay operations",
    auto_save_provenance=True
)
def analyze_parcel_zoning(
    parcels_path: str,
    zoning_path: str,
    boundary_path: str,
    output_path: str
):
    """
    Complex pipeline showing multiple operations with semantic checks.
    """

    @spatial_task(name="load_datasets", validate_geometries=True)
    def load_all_data():
        parcels = load(parcels_path, validate=True, auto_fix=True)
        zoning = load(zoning_path, validate=True, auto_fix=True)
        boundary = load(boundary_path)
        return parcels, zoning, boundary

    @spatial_task(name="standardize_crs", warn_geographic=True)
    def standardize_crs(parcels, zoning, boundary):
        # Standardize to UTM Zone 10N
        target_crs = 'EPSG:32610'
        return (
            parcels.to_crs(target_crs),
            zoning.to_crs(target_crs),
            boundary.to_crs(target_crs)
        )

    @spatial_task(name="overlay_parcels_zoning")
    def perform_overlay(parcels, zoning):
        # Find parcels overlapping residential zones
        residential = zoning[zoning['type'] == 'Residential'].copy()
        return overlay(parcels, residential, how='intersection')

    @spatial_task(name="clip_to_boundary")
    def clip_to_study_area(parcels, boundary):
        return clip(parcels, boundary)

    # Execute
    print("Loading datasets...")
    parcels, zoning, boundary = load_all_data()
    print(f"  Parcels: {len(parcels)}")
    print(f"  Zoning: {len(zoning)}")
    print(f"  Boundary: {len(boundary)}")

    print("Standardizing CRS...")
    parcels, zoning, boundary = standardize_crs(parcels, zoning, boundary)
    print(f"  All datasets in {parcels.crs}")

    print("Finding residential parcels...")
    residential_parcels = perform_overlay(parcels, zoning)
    print(f"  Found {len(residential_parcels)} parcels in residential zones")

    print("Clipping to study area...")
    final_result = clip_to_study_area(residential_parcels, boundary)
    print(f"  Final result: {len(final_result)} parcels")

    # Save
    final_result.to_file(output_path)
    print(f"Saved to {output_path}")

    return final_result


# Example 3: Provenance Export
def demonstrate_provenance():
    """
    Show how provenance tracking works
    """
    import tempfile
    import json

    print("\n" + "="*60)
    print("PROVENANCE TRACKING DEMO")
    print("="*60)

    # Create sample data
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # Create sample roads
        roads_gdf = gpd.GeoDataFrame({
            'id': [1, 2, 3],
            'name': ['Main St', 'Oak Ave', 'Pine Rd']
        }, geometry=[
            Point(0, 0).buffer(0.001),
            Point(0.01, 0.01).buffer(0.001),
            Point(0.02, 0.02).buffer(0.001)
        ], crs='EPSG:4326')

        roads_path = tmpdir / "roads.gpkg"
        roads_gdf.to_file(roads_path)

        output_path = tmpdir / "buffered_roads.gpkg"

        # Execute pipeline WITH provenance
        print("\nExecuting pipeline with provenance tracking...")
        result = analyze_road_buffers.run(str(roads_path), str(output_path))

        print("\nPipeline Result:")
        print(result)

        # Get summary
        summary = result.get_summary()
        print("\nExecution Summary:")
        print(f"  Pipeline: {summary['pipeline_name']}")
        print(f"  Total operations: {summary['total_operations']}")
        print(f"  Total time: {summary['total_execution_time']:.3f}s")
        print(f"  Status: {'SUCCESS' if summary['failed_operations'] == 0 else 'FAILED'}")

        print("\nOperations executed:")
        for op in summary['operations']:
            status_symbol = "[OK]" if op['status'] == 'success' else "[FAIL]"
            print(f"  {status_symbol} {op['name']} ({op['time']:.3f}s)")

        # Save provenance manually
        provenance_file = tmpdir / "manual_provenance.json"
        result.save_provenance(provenance_file)
        print(f"\nProvenance saved to: {provenance_file}")

        # Show provenance content
        with open(provenance_file) as f:
            prov_data = json.load(f)

        print("\nProvenance metadata includes:")
        print(f"  - Pipeline name: {prov_data['pipeline_name']}")
        print(f"  - Start time: {prov_data['start_time']}")
        print(f"  - Python version: {prov_data['environment']['python_version']}")
        print(f"  - GeoPandas version: {prov_data['environment']['geopandas_version']}")
        print(f"  - Platform: {prov_data['environment']['platform']}")
        print(f"  - Operations: {len(prov_data['operations'])}")

        print("\nFirst operation details:")
        first_op = prov_data['operations'][0]
        print(f"  Name: {first_op['operation_name']}")
        print(f"  Type: {first_op['operation_type']}")
        print(f"  Inputs: {len(first_op['inputs'])}")
        print(f"  Outputs: {len(first_op['outputs'])}")

        print("\n" + "="*60)
        print("This provenance file can be:")
        print("  1. Archived with your publication")
        print("  2. Used to recreate the exact workflow")
        print("  3. Shared for reproducible research")
        print("="*60)


# Example 4: Compare WITH and WITHOUT provenance
def compare_execution_modes():
    """
    Show the difference between regular execution and tracked execution.
    """
    import tempfile

    print("\n" + "="*60)
    print("EXECUTION MODE COMPARISON")
    print("="*60)

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # Create sample data
        roads_gdf = gpd.GeoDataFrame({
            'id': [1, 2]
        }, geometry=[Point(0, 0).buffer(0.001), Point(0.01, 0.01).buffer(0.001)],
        crs='EPSG:4326')

        roads_path = tmpdir / "roads.gpkg"
        roads_gdf.to_file(roads_path)
        output_path = tmpdir / "output.gpkg"

        # Mode 1: Without provenance (fast)
        print("\nMode 1: Regular execution (NO provenance)")
        print("  Use when: Prototyping, testing, quick analysis")
        result1 = analyze_road_buffers(str(roads_path), str(output_path))
        print(f"  Result: {type(result1).__name__} with {len(result1)} features")

        # Mode 2: With provenance (tracked)
        print("\nMode 2: Tracked execution (WITH provenance)")
        print("  Use when: Production runs, publishable research, auditing")
        result2 = analyze_road_buffers.run(str(roads_path), str(output_path))
        print(f"  Result: {type(result2).__name__}")
        print(f"  Provenance: {len(result2.provenance.records)} operations tracked")
        print(f"  Data: {len(result2.result)} features")

        print("\n" + "="*60)
        print("KEY INSIGHT:")
        print("  - Same pipeline, same code")
        print("  - Just call .run() to enable provenance")
        print("  - No code changes needed for reproducibility!")
        print("="*60)


if __name__ == "__main__":
    print(
    ("=" * 67) + "\n" +
    "GeoFlow @geo_pipeline Decorator Demonstration\n" +
    "The CORE feature that makes workflows reproducible\n" +
    ("=" * 67) + "\n"
    )

    # Run demonstrations
    demonstrate_provenance()
    compare_execution_modes()

    print("\n" + "="*60)
    print("WHAT MAKES THIS USEFUL:")
    print("="*60)
    print("[*] Automatic provenance tracking")
    print("[*] Semantic validation (warns about anti-patterns)")
    print("[*] CRS safety checks")
    print("[*] Reproducible by design")
    print("[*] Publication-ready metadata")
    print("[*] Same code works with/without tracking")
    print("="*60)
