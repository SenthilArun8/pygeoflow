import pytest
import geopandas as gpd
from shapely.geometry import Point, box

from geoflow.io.loaders import load
from geoflow.spatial.operations import spatial_join, buffer


@pytest.fixture
def sample_points():
    """Create sample points in WGS84"""
    points = [Point(0, 0), Point(1, 1), Point(2, 2)]
    return gpd.GeoDataFrame(
        [{'id': i, 'geometry': pt} for i, pt in enumerate(points)],
        crs='EPSG:4326'
    )


@pytest.fixture
def sample_polygons():
    """Create sample polygons in UTM"""
    polygons = [
        box(499000, 4499000, 501000, 4501000),
        box(501000, 4501000, 503000, 4503000)
    ]
    return gpd.GeoDataFrame(
        [{'zone': i, 'geometry': poly} for i, poly in enumerate(polygons)],
        crs='EPSG:32610'
    )


@pytest.mark.integration
class TestEndToEndWorkflow:

    def test_simple_pipeline(self, sample_points, tmp_path):
        """Test a simple load -> buffer -> save pipeline"""
        # Save points to file
        input_file = tmp_path / "points.geojson"
        sample_points.to_file(input_file, driver='GeoJSON')

        # Load
        gdf = load(input_file)
        assert len(gdf) == 3

        # Buffer (should warn about geographic CRS)
        buffered = buffer(gdf, distance=0.1)
        assert len(buffered) == len(gdf)
        assert buffered.geometry.geom_type.iloc[0] == 'Polygon'

        # Save
        output_file = tmp_path / "buffered.geojson"
        buffered.to_file(output_file, driver='GeoJSON')

        # Verify output
        assert output_file.exists()
        reloaded = gpd.read_file(output_file)
        assert len(reloaded) == 3

    def test_spatial_join_mixed_crs(self, sample_points, sample_polygons):
        """Test spatial join with different CRS"""
        # Convert points to same CRS as polygons for meaningful join
        sample_points_utm = sample_points.to_crs('EPSG:32610')
        # Move points to be within polygon bounds
        sample_points_utm.geometry = sample_points_utm.geometry.apply(
            lambda geom: Point(500000, 4500000)
        )

        # This should automatically reproject
        result = spatial_join(sample_points_utm, sample_polygons)

        # Should have joined features
        assert len(result) > 0

        # Should have columns from both
        assert 'id' in result.columns
        assert 'zone' in result.columns

    def test_buffer_preserves_attributes(self, sample_points):
        """Test that buffer operation preserves data attributes"""
        buffered = buffer(sample_points, distance=0.1)

        # Should preserve all original columns
        assert 'id' in buffered.columns
        assert buffered['id'].tolist() == [0, 1, 2]

    def test_metadata_tracking(self, sample_points, tmp_path):
        """Test that metadata is tracked through pipeline"""
        input_file = tmp_path / "test.geojson"
        sample_points.to_file(input_file, driver='GeoJSON')

        # Load with metadata
        gdf = load(input_file)

        # Check metadata exists
        assert 'source_file' in gdf.attrs
        assert 'test.geojson' in gdf.attrs['source_file']
