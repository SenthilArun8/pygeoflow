import pytest
import geopandas as gpd
from shapely.geometry import Point
from pathlib import Path
import tempfile
import json

from geoflow.io.loaders import load, DataLoader


@pytest.fixture
def temp_geojson(tmp_path):
    """Create a temporary GeoJSON file"""
    geojson = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [0, 0]},
                "properties": {"name": "Point 1"}
            },
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [1, 1]},
                "properties": {"name": "Point 2"}
            }
        ]
    }

    filepath = tmp_path / "test.geojson"
    with open(filepath, 'w') as f:
        json.dump(geojson, f)

    return filepath


class TestDataLoader:

    def test_load_geojson(self, temp_geojson):
        """Test loading a GeoJSON file"""
        gdf = load(temp_geojson)

        assert len(gdf) == 2
        assert 'name' in gdf.columns
        assert gdf['name'].tolist() == ['Point 1', 'Point 2']

    def test_load_missing_file(self):
        """Test loading non-existent file raises error"""
        with pytest.raises(FileNotFoundError):
            load("nonexistent.geojson")

    def test_load_unsupported_format(self, tmp_path):
        """Test loading unsupported format raises error"""
        bad_file = tmp_path / "test.txt"
        bad_file.touch()

        with pytest.raises(ValueError, match="Unsupported format"):
            load(bad_file)

    def test_metadata_attached(self, temp_geojson):
        """Test that metadata is attached to GeoDataFrame"""
        gdf = load(temp_geojson)

        assert 'source_file' in gdf.attrs
        assert str(temp_geojson) in gdf.attrs['source_file']

    def test_load_with_path_object(self, temp_geojson):
        """Test loading with Path object"""
        gdf = load(Path(temp_geojson))
        assert len(gdf) == 2

    def test_load_with_string_path(self, temp_geojson):
        """Test loading with string path"""
        gdf = load(str(temp_geojson))
        assert len(gdf) == 2
