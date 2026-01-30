"""Tests for save functionality with provenance tracking"""

import pytest
import geopandas as gpd
from shapely.geometry import Point
from pathlib import Path
import json
import sqlite3
import tempfile
import shutil

from geoflow import load, save
from geoflow.core.provenance import ProvenanceTracker


@pytest.fixture
def sample_gdf():
    """Create a sample GeoDataFrame"""
    return gpd.GeoDataFrame(
        {
            'name': ['A', 'B', 'C'],
            'value': [1, 2, 3]
        },
        geometry=[Point(0, 0), Point(1, 1), Point(2, 2)],
        crs='EPSG:4326'
    )


@pytest.fixture
def sample_provenance():
    """Create sample provenance metadata"""
    tracker = ProvenanceTracker(pipeline_name="test_pipeline")
    record = tracker.start_operation("test_op", operation_type="task")
    tracker.complete_operation(record, execution_time=0.123)
    tracker.finalize()
    return tracker.to_dict()


@pytest.fixture
def temp_dir():
    """Create temporary directory for test outputs"""
    temp = tempfile.mkdtemp()
    yield Path(temp)
    shutil.rmtree(temp)


class TestSaveBasic:
    """Test basic save functionality without provenance"""

    def test_save_geojson(self, sample_gdf, temp_dir):
        """Test saving to GeoJSON"""
        output_path = temp_dir / "output.geojson"

        result_path = save(sample_gdf, output_path)

        assert result_path.exists()
        assert result_path == output_path

        # Verify data
        loaded = gpd.read_file(result_path)
        assert len(loaded) == 3
        assert loaded.crs == sample_gdf.crs

    def test_save_geopackage(self, sample_gdf, temp_dir):
        """Test saving to GeoPackage"""
        output_path = temp_dir / "output.gpkg"

        result_path = save(sample_gdf, output_path)

        assert result_path.exists()
        loaded = gpd.read_file(result_path)
        assert len(loaded) == 3

    def test_save_shapefile(self, sample_gdf, temp_dir):
        """Test saving to Shapefile"""
        output_path = temp_dir / "output.shp"

        result_path = save(sample_gdf, output_path)

        assert result_path.exists()
        loaded = gpd.read_file(result_path)
        assert len(loaded) == 3

    def test_save_unsupported_format(self, sample_gdf, temp_dir):
        """Test that unsupported formats raise ValueError"""
        output_path = temp_dir / "output.csv"

        with pytest.raises(ValueError, match="Unsupported format"):
            save(sample_gdf, output_path)


class TestSaveWithProvenance:
    """Test save functionality with provenance tracking"""

    def test_save_geojson_with_provenance_sidecar(self, sample_gdf, sample_provenance, temp_dir):
        """Test GeoJSON creates sidecar provenance file"""
        output_path = temp_dir / "output.geojson"

        save(sample_gdf, output_path, provenance=sample_provenance)

        # Check data file exists
        assert output_path.exists()

        # Check sidecar provenance file exists
        provenance_path = temp_dir / "output.geojson.provenance.json"
        assert provenance_path.exists()

        # Verify provenance content
        with open(provenance_path) as f:
            saved_prov = json.load(f)

        assert 'data_file' in saved_prov
        assert saved_prov['data_file'] == 'output.geojson'
        assert 'saved_at' in saved_prov
        assert 'provenance' in saved_prov
        assert saved_prov['provenance']['pipeline_name'] == 'test_pipeline'

    def test_save_shapefile_with_provenance_sidecar(self, sample_gdf, sample_provenance, temp_dir):
        """Test Shapefile creates sidecar provenance file"""
        output_path = temp_dir / "output.shp"

        save(sample_gdf, output_path, provenance=sample_provenance)

        # Check provenance sidecar
        provenance_path = temp_dir / "output.shp.provenance.json"
        assert provenance_path.exists()

    def test_save_geopackage_embedded_provenance(self, sample_gdf, sample_provenance, temp_dir):
        """Test GeoPackage embeds provenance in metadata table"""
        output_path = temp_dir / "output.gpkg"

        save(sample_gdf, output_path, provenance=sample_provenance)

        # Check data file exists
        assert output_path.exists()

        # Verify provenance embedded in GeoPackage
        conn = sqlite3.connect(str(output_path))
        cursor = conn.cursor()

        # Check provenance table exists
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='geoflow_provenance'"
        )
        assert cursor.fetchone() is not None

        # Check provenance data
        cursor.execute("SELECT timestamp, provenance_json FROM geoflow_provenance")
        row = cursor.fetchone()
        assert row is not None

        timestamp, provenance_json = row
        assert timestamp is not None

        saved_prov = json.loads(provenance_json)
        assert saved_prov['pipeline_name'] == 'test_pipeline'

        conn.close()

    def test_save_without_provenance(self, sample_gdf, temp_dir):
        """Test save without provenance doesn't create sidecar"""
        output_path = temp_dir / "output.geojson"

        save(sample_gdf, output_path)

        assert output_path.exists()

        # No sidecar should be created
        provenance_path = temp_dir / "output.geojson.provenance.json"
        assert not provenance_path.exists()

    def test_save_provenance_none(self, sample_gdf, temp_dir):
        """Test save with provenance=None doesn't create sidecar"""
        output_path = temp_dir / "output.geojson"

        save(sample_gdf, output_path, provenance=None)

        provenance_path = temp_dir / "output.geojson.provenance.json"
        assert not provenance_path.exists()


class TestSaveIntegration:
    """Test save integration with load and pipelines"""

    def test_load_save_roundtrip(self, sample_gdf, temp_dir):
        """Test saving and loading back"""
        output_path = temp_dir / "roundtrip.geojson"

        # Save
        save(sample_gdf, output_path)

        # Load back
        loaded = load(output_path)

        assert len(loaded) == len(sample_gdf)
        assert loaded.crs == sample_gdf.crs
        assert list(loaded['name']) == list(sample_gdf['name'])

    def test_save_with_path_object(self, sample_gdf, temp_dir):
        """Test save accepts Path objects"""
        output_path = Path(temp_dir) / "output.gpkg"

        result = save(sample_gdf, output_path)

        assert isinstance(result, Path)
        assert result.exists()

    def test_save_with_string_path(self, sample_gdf, temp_dir):
        """Test save accepts string paths"""
        output_path = str(temp_dir / "output.gpkg")

        result = save(sample_gdf, output_path)

        assert isinstance(result, Path)
        assert result.exists()
