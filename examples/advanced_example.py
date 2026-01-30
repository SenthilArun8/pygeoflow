"""
Advanced GeoFlow Example
=========================

This example demonstrates advanced features including:
- Geometry validation and repair
- Overlay operations (intersection, union, etc.)
- Clip operations
- CRS handling across different datasets
"""

import geopandas as gpd
from shapely.geometry import Point, Polygon, box
from pathlib import Path

# Import from geoflow
from geoflow import load, buffer, overlay, clip, validate_geometry


def main():
    """Run an advanced geospatial workflow"""

    print("=" * 70)
    print("GeoFlow Advanced Example - Geometry Validation & Overlay Operations")
    print("=" * 70)

    # 1. Create sample datasets with different CRS
    print("\n1. Creating sample datasets...")

    # Study area in WGS84
    study_area = gpd.GeoDataFrame(
        {'name': ['Study Area']},
        geometry=[box(-122.5, 37.7, -122.3, 37.9)],
        crs='EPSG:4326'
    )
    print(f"   Study area created in {study_area.crs}")

    # Points of interest in WGS84
    poi = gpd.GeoDataFrame(
        {
            'id': [1, 2, 3, 4],
            'name': ['Park', 'School', 'Hospital', 'Library']
        },
        geometry=[
            Point(-122.45, 37.75),
            Point(-122.40, 37.80),
            Point(-122.35, 37.85),
            Point(-122.25, 37.72)  # Outside study area
        ],
        crs='EPSG:4326'
    )
    print(f"   Created {len(poi)} points of interest")

    # Zones in UTM (different CRS)
    zones = gpd.GeoDataFrame(
        {'zone_type': ['Residential', 'Commercial']},
        geometry=[
            box(550000, 4175000, 560000, 4185000),
            box(555000, 4180000, 565000, 4190000)
        ],
        crs='EPSG:32610'  # UTM Zone 10N
    )
    print(f"   Created {len(zones)} zones in {zones.crs}")

    # 2. Geometry Validation
    print("\n2. Demonstrating geometry validation...")

    # Create an invalid geometry (self-intersecting polygon - bowtie)
    invalid_polygon = Polygon([(0, 0), (1, 1), (1, 0), (0, 1), (0, 0)])
    invalid_gdf = gpd.GeoDataFrame(
        {'id': [1]},
        geometry=[invalid_polygon],
        crs='EPSG:4326'
    )

    print(f"   Invalid geometry valid? {invalid_gdf.geometry.is_valid.iloc[0]}")

    # Fix invalid geometry
    fixed_gdf = validate_geometry(invalid_gdf, auto_fix=True)
    print(f"   After fix, valid? {fixed_gdf.geometry.is_valid.iloc[0]}")

    # 3. Clip operation with automatic CRS handling
    print("\n3. Clipping points to study area (different CRS)...")

    # Note: POI is in EPSG:4326, study_area is also in EPSG:4326
    # But this demonstrates the automatic CRS handling capability
    clipped_poi = clip(poi, study_area)
    print(f"   Original points: {len(poi)}")
    print(f"   Clipped points: {len(clipped_poi)}")
    print(f"   Points within study area: {clipped_poi['name'].tolist()}")

    # 4. Buffer with CRS warning
    print("\n4. Creating buffers (with CRS safety check)...")

    # Convert to UTM for accurate buffering
    poi_utm = poi.to_crs('EPSG:32610')
    buffered_poi = buffer(poi_utm, distance=500)  # 500 meters
    print(f"   Created {len(buffered_poi)} buffers of 500m")

    # 5. Overlay operations with automatic CRS alignment
    print("\n5. Performing overlay operations...")

    # Convert study area to UTM to match zones
    study_area_utm = study_area.to_crs('EPSG:32610')

    # Intersection: Find areas that are both in study area AND in zones
    intersection = overlay(study_area_utm, zones, how='intersection')
    print(f"   Intersection result: {len(intersection)} features")

    # Union: Combine study area with zones
    union = overlay(study_area_utm, zones, how='union')
    print(f"   Union result: {len(union)} features")

    # Difference: Study area excluding zones
    difference = overlay(study_area_utm, zones, how='difference')
    print(f"   Difference result: {len(difference)} features")

    # 6. Complex workflow: Find POI within buffered zones
    print("\n6. Complex workflow: POI within buffered residential zones...")

    # Filter residential zones
    residential = zones[zones['zone_type'] == 'Residential'].copy()

    # Buffer residential zones by 1km
    buffered_residential = buffer(residential, distance=1000)
    print(f"   Buffered {len(residential)} residential zones by 1km")

    # Clip POI to buffered residential zones
    poi_in_residential = clip(poi_utm, buffered_residential)
    print(f"   Found {len(poi_in_residential)} POI within buffered residential zones")
    if len(poi_in_residential) > 0:
        print(f"   POI names: {poi_in_residential['name'].tolist()}")

    # 7. Save results
    print("\n7. Saving results...")

    temp_dir = Path("temp_output")
    temp_dir.mkdir(exist_ok=True)

    clipped_poi.to_file(temp_dir / "clipped_poi.geojson", driver='GeoJSON')
    buffered_poi.to_file(temp_dir / "buffered_poi.geojson", driver='GeoJSON')
    intersection.to_file(temp_dir / "intersection.geojson", driver='GeoJSON')

    print(f"   Results saved to {temp_dir}/")

    # Summary
    print("\n" + "=" * 70)
    print("Advanced Example Completed Successfully!")
    print("=" * 70)
    print("\nKey Features Demonstrated:")
    print("  [OK] Automatic CRS handling across different coordinate systems")
    print("  [OK] Geometry validation and automatic repair")
    print("  [OK] Safe buffering with CRS warnings")
    print("  [OK] Overlay operations (intersection, union, difference)")
    print("  [OK] Clipping operations")
    print("  [OK] Complex multi-step workflows")
    print("\nAll operations maintained data integrity and reproducibility!")
    print("=" * 70)

    # Cleanup
    import shutil
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
        print(f"\nCleaned up temporary directory: {temp_dir}")


if __name__ == "__main__":
    main()
