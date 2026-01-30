"""
Simple GeoFlow Example
======================

This example demonstrates basic usage of the GeoFlow framework.
"""

import geopandas as gpd
from shapely.geometry import Point
from pathlib import Path

# Import from geoflow
from geoflow import load, buffer, spatial_join


def main():
    """Run a simple geospatial workflow"""

    print("=" * 60)
    print("GeoFlow Simple Example")
    print("=" * 60)

    # Create sample data
    print("\n1. Creating sample data...")
    points = gpd.GeoDataFrame(
        {
            'id': [1, 2, 3],
            'name': ['Point A', 'Point B', 'Point C']
        },
        geometry=[Point(0, 0), Point(1, 1), Point(2, 2)],
        crs='EPSG:4326'
    )
    print(f"   Created {len(points)} points in WGS84 (EPSG:4326)")

    # Save to temporary file
    temp_file = Path("temp_points.geojson")
    points.to_file(temp_file, driver='GeoJSON')
    print(f"   Saved to {temp_file}")

    # Load using GeoFlow
    print("\n2. Loading data with GeoFlow...")
    loaded_points = load(temp_file)
    print(f"   Loaded {len(loaded_points)} features")
    print(f"   CRS: {loaded_points.crs}")

    # Buffer the points (will warn about geographic CRS)
    print("\n3. Buffering points...")
    print("   (Note: Buffering in geographic CRS will trigger a warning)")
    buffered = buffer(loaded_points, distance=0.5)
    print(f"   Created {len(buffered)} buffers")
    print(f"   Geometry type: {buffered.geometry.geom_type.iloc[0]}")

    # Display results
    print("\n4. Results:")
    print(buffered[['id', 'name', 'geometry']].head())

    # Clean up
    temp_file.unlink()
    print(f"\n5. Cleaned up temporary file: {temp_file}")

    print("\n" + "=" * 60)
    print("Example completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    main()
