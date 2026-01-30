"""
Complete Reproducible Pipeline Example

This demonstrates the CLOSED LOOP:
  load -> transform -> save (WITH PROVENANCE)

The provenance chain is maintained from input to output, making your
results truly reproducible and publication-ready.
"""

import geopandas as gpd
from shapely.geometry import Point
from pathlib import Path
import tempfile

from geoflow import load, save, geo_pipeline, spatial_task, buffer, spatial_join


def create_sample_data(temp_dir):
    """Create sample data for the example"""

    # Create sample parcels
    parcels = gpd.GeoDataFrame({
        'parcel_id': ['P1', 'P2', 'P3', 'P4'],
        'owner': ['Alice', 'Bob', 'Carol', 'Dave'],
        'value': [100000, 150000, 200000, 180000]
    }, geometry=[
        Point(-122.45, 37.75).buffer(0.01),
        Point(-122.46, 37.76).buffer(0.01),
        Point(-122.44, 37.74).buffer(0.01),
        Point(-122.47, 37.77).buffer(0.01),
    ], crs='EPSG:4326')

    # Create sample flood zones
    zones = gpd.GeoDataFrame({
        'zone_id': ['FZ1', 'FZ2'],
        'risk': ['High', 'Medium']
    }, geometry=[
        Point(-122.45, 37.75).buffer(0.02),
        Point(-122.47, 37.77).buffer(0.015),
    ], crs='EPSG:4326')

    # Save input data
    parcels_path = Path(temp_dir) / "input_parcels.gpkg"
    zones_path = Path(temp_dir) / "input_zones.gpkg"

    parcels.to_file(parcels_path)
    zones.to_file(zones_path)

    return parcels_path, zones_path


@geo_pipeline(
    name="flood_risk_analysis",
    track_provenance=True,
    auto_save_provenance=False  # We'll save it with the data
)
def analyze_flood_risk(parcels_path, zones_path, output_path):
    """
    Analyze parcels at risk of flooding.

    This pipeline:
    1. Loads parcels and flood zones
    2. Reprojects to UTM for accurate spatial operations
    3. Creates 50m buffer around flood zones
    4. Identifies parcels intersecting buffered zones
    5. Saves results WITH PROVENANCE
    """

    @spatial_task(name="load_data", validate_geometries=True)
    def load_datasets():
        print("\n[1/5] Loading datasets...")
        parcels = load(parcels_path, validate=True)
        zones = load(zones_path, validate=True)
        return parcels, zones

    @spatial_task(name="reproject", strict_crs=False)
    def reproject_to_utm(parcels, zones):
        print("[2/5] Reprojecting to UTM Zone 10N...")
        # Reproject to projected CRS for accurate distance measurements
        parcels_utm = parcels.to_crs('EPSG:32610')
        zones_utm = zones.to_crs('EPSG:32610')
        return parcels_utm, zones_utm

    @spatial_task(name="buffer_zones", warn_geographic=True)
    def create_buffer_zones(zones_utm):
        print("[3/5] Creating 50m buffer around flood zones...")
        # Safe buffer in meters (projected CRS)
        buffered = buffer(zones_utm, distance=50)
        return buffered

    @spatial_task(name="identify_at_risk", validate_crs=True)
    def identify_at_risk_parcels(parcels_utm, buffered_zones):
        print("[4/5] Identifying parcels at risk...")
        # Spatial join with automatic CRS checks
        at_risk = spatial_join(
            parcels_utm,
            buffered_zones,
            how='inner',
            predicate='intersects',
            target_crs='EPSG:32610'
        )
        return at_risk

    # Execute pipeline steps
    parcels, zones = load_datasets()
    parcels_utm, zones_utm = reproject_to_utm(parcels, zones)
    buffered_zones = create_buffer_zones(zones_utm)
    at_risk_parcels = identify_at_risk_parcels(parcels_utm, buffered_zones)

    print(f"\n[5/5] Saving results to {output_path}...")
    return at_risk_parcels


def main():
    """Run the complete reproducible pipeline"""

    print("="*70)
    print("CLOSED-LOOP REPRODUCIBLE PIPELINE EXAMPLE")
    print("="*70)

    # Create temporary directory for example
    with tempfile.TemporaryDirectory() as temp_dir:

        # Create sample data
        print("\n[SETUP] Creating sample data...")
        parcels_path, zones_path = create_sample_data(temp_dir)
        output_path = Path(temp_dir) / "at_risk_parcels.gpkg"

        # Run pipeline
        print("\n[PIPELINE] Running flood risk analysis...")
        result = analyze_flood_risk.run(parcels_path, zones_path, output_path)

        # Access the result data
        at_risk = result.result
        print(f"\n[OK] Found {len(at_risk)} parcels at risk")

        # Get provenance summary
        summary = result.get_summary()
        print(f"\n[PROVENANCE] Pipeline executed:")
        print(f"  - Pipeline: {summary['pipeline_name']}")
        print(f"  - Operations: {summary['total_operations']}")
        print(f"  - Duration: {summary['total_execution_time']:.3f}s")

        print(f"\n[SAVE] Saving results with provenance...")
        save(
            at_risk,
            output_path,
            provenance=result.provenance.to_dict()
        )

        print(f"\n[OK] Saved to: {output_path}")
        print(f"[OK] Provenance embedded in GeoPackage metadata!")

        # Demonstrate provenance retrieval
        print("\n[VERIFY] Reading provenance back from file...")
        import sqlite3
        import json

        conn = sqlite3.connect(str(output_path))
        cursor = conn.cursor()
        cursor.execute("SELECT provenance_json FROM geoflow_provenance LIMIT 1")
        row = cursor.fetchone()

        if row:
            saved_provenance = json.loads(row[0])
            print(f"[OK] Retrieved provenance from GeoPackage")
            print(f"  - Pipeline: {saved_provenance['pipeline_name']}")
            print(f"  - Operations: {len(saved_provenance['operations'])}")

            print("\n  Operations performed:")
            for op in saved_provenance['operations']:
                print(f"    {op['operation_name']}: {op['execution_time']:.3f}s")

        conn.close()

        print("\n" + "="*70)
        print("RESULT: DOCUMENTED ASSET CREATED [OK]")
        print("="*70)
        print("""
This output file is now a "Documented Asset" because it contains:

1. [OK] The actual analysis results (GeoDataFrame)
2. [OK] Complete provenance metadata
3. [OK] Full operation history
4. [OK] Input parameters
5. [OK] Execution timing
6. [OK] System environment

Anyone can now:
- Reproduce your analysis
- Understand what operations were performed
- Verify the data lineage
- Audit the workflow

This is TRUE reproducibility!
        """)


if __name__ == "__main__":
    main()
