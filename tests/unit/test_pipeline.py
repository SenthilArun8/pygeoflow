"""Tests for pipeline orchestration and provenance tracking"""

import pytest
import tempfile
from pathlib import Path
import json
import geopandas as gpd
from shapely.geometry import Point

from geoflow import geo_pipeline, spatial_task, load, buffer
from geoflow.core.pipeline import PipelineResult
from geoflow.core.provenance import ProvenanceTracker


@pytest.fixture
def sample_roads_file(tmp_path):
    """Create a sample roads GeoPackage for testing"""
    roads = gpd.GeoDataFrame({
        'id': [1, 2, 3],
        'name': ['Main St', 'Oak Ave', 'Pine Rd']
    }, geometry=[
        Point(0, 0).buffer(0.001),
        Point(0.01, 0.01).buffer(0.001),
        Point(0.02, 0.02).buffer(0.001)
    ], crs='EPSG:4326')

    filepath = tmp_path / "roads.gpkg"
    roads.to_file(filepath)
    return filepath


class TestGeoPipeline:
    """Test @geo_pipeline decorator"""

    def test_pipeline_without_provenance(self, sample_roads_file, tmp_path):
        """Pipeline should work in regular mode without provenance"""

        @geo_pipeline(name="test_pipeline")
        def simple_pipeline(input_path):
            gdf = load(input_path)
            return gdf

        # Call without .run() - no provenance
        result = simple_pipeline(str(sample_roads_file))

        assert isinstance(result, gpd.GeoDataFrame)
        assert len(result) == 3

    def test_pipeline_with_provenance(self, sample_roads_file, tmp_path):
        """Pipeline should track provenance when using .run()"""

        @geo_pipeline(name="tracked_pipeline")
        def simple_pipeline(input_path):
            gdf = load(input_path)
            return gdf

        # Call with .run() - provenance tracked
        result = simple_pipeline.run(str(sample_roads_file))

        assert isinstance(result, PipelineResult)
        assert isinstance(result.result, gpd.GeoDataFrame)
        assert isinstance(result.provenance, ProvenanceTracker)
        assert len(result.result) == 3

    def test_pipeline_provenance_metadata(self, sample_roads_file, tmp_path):
        """Provenance should capture pipeline metadata"""

        @geo_pipeline(name="metadata_test")
        def simple_pipeline(input_path, buffer_dist=100):
            gdf = load(input_path)
            return gdf

        result = simple_pipeline.run(str(sample_roads_file), buffer_dist=50)

        # Check provenance metadata
        prov_dict = result.provenance.to_dict()

        assert prov_dict['pipeline_name'] == 'metadata_test'
        assert 'start_time' in prov_dict
        assert 'end_time' in prov_dict
        assert 'total_execution_time' in prov_dict
        assert prov_dict['total_execution_time'] > 0

        # Check environment capture
        env = prov_dict['environment']
        assert 'python_version' in env
        assert 'geopandas_version' in env
        assert 'platform' in env

    def test_pipeline_operations_recorded(self, sample_roads_file, tmp_path):
        """Provenance should record all operations"""

        @geo_pipeline(name="ops_test")
        def pipeline_with_ops(input_path):
            gdf = load(input_path)
            gdf_utm = gdf.to_crs('EPSG:32610')
            return gdf_utm

        result = pipeline_with_ops.run(str(sample_roads_file))

        # Should have at least the pipeline operation
        assert len(result.provenance.records) >= 1
        assert result.provenance.records[0].operation_name == 'ops_test'

    def test_pipeline_auto_save_provenance(self, sample_roads_file, tmp_path):
        """Pipeline should auto-save provenance when enabled"""

        provenance_dir = tmp_path / "provenance"

        @geo_pipeline(
            name="auto_save_test",
            auto_save_provenance=True,
            provenance_dir=provenance_dir
        )
        def auto_save_pipeline(input_path):
            return load(input_path)

        result = auto_save_pipeline.run(str(sample_roads_file))

        # Check that provenance file was created
        prov_files = list(provenance_dir.glob("auto_save_test_*_provenance.json"))
        assert len(prov_files) == 1

        # Verify file content
        with open(prov_files[0]) as f:
            prov_data = json.load(f)

        assert prov_data['pipeline_name'] == 'auto_save_test'

    def test_pipeline_error_handling(self, sample_roads_file, tmp_path):
        """Pipeline should capture errors in provenance"""

        @geo_pipeline(name="error_test")
        def failing_pipeline(input_path):
            gdf = load(input_path)
            raise ValueError("Intentional error for testing")

        with pytest.raises(ValueError, match="Intentional error"):
            result = failing_pipeline.run(str(sample_roads_file))

    def test_pipeline_result_save_provenance(self, sample_roads_file, tmp_path):
        """PipelineResult should save provenance manually"""

        @geo_pipeline(name="manual_save_test")
        def simple_pipeline(input_path):
            return load(input_path)

        result = simple_pipeline.run(str(sample_roads_file))

        # Manually save provenance
        prov_file = tmp_path / "manual_prov.json"
        result.save_provenance(prov_file)

        assert prov_file.exists()

        # Verify content
        with open(prov_file) as f:
            prov_data = json.load(f)

        assert prov_data['pipeline_name'] == 'manual_save_test'

    def test_pipeline_get_summary(self, sample_roads_file, tmp_path):
        """PipelineResult should provide execution summary"""

        @geo_pipeline(name="summary_test")
        def simple_pipeline(input_path):
            return load(input_path)

        result = simple_pipeline.run(str(sample_roads_file))
        summary = result.get_summary()

        assert summary['pipeline_name'] == 'summary_test'
        assert summary['total_operations'] >= 1
        assert summary['failed_operations'] == 0
        assert summary['total_execution_time'] > 0
        assert 'operations' in summary


class TestSpatialTask:
    """Test @spatial_task decorator"""

    def test_spatial_task_basic(self):
        """Spatial task should execute normally"""

        @spatial_task(name="test_task")
        def simple_task(gdf):
            return gdf.copy()

        gdf = gpd.GeoDataFrame({
            'id': [1, 2]
        }, geometry=[Point(0, 0), Point(1, 1)], crs='EPSG:4326')

        result = simple_task(gdf)

        assert isinstance(result, gpd.GeoDataFrame)
        assert len(result) == 2

    def test_spatial_task_warn_geographic(self, caplog):
        """Spatial task should warn about geographic CRS for buffers"""

        @spatial_task(name="buffer_task", warn_geographic=True)
        def buffer_task(gdf, distance):
            return gdf.buffer(distance)

        gdf = gpd.GeoDataFrame({
            'id': [1]
        }, geometry=[Point(0, 0)], crs='EPSG:4326')

        # Should log a warning about geographic CRS
        result = buffer_task(gdf, 100)

        assert len(result) == 1

    def test_spatial_task_strict_crs(self):
        """Spatial task should raise error in strict mode"""

        @spatial_task(name="strict_buffer", strict_crs=True)
        def buffer_task(gdf, distance):
            return gdf.buffer(distance)

        gdf = gpd.GeoDataFrame({
            'id': [1]
        }, geometry=[Point(0, 0)], crs='EPSG:4326')

        # Should raise error for geographic CRS
        with pytest.raises(ValueError, match="Cannot perform.*geographic CRS"):
            buffer_task(gdf, 100)

    def test_spatial_task_validate_geometries(self, caplog):
        """Spatial task should validate geometries when enabled"""
        from shapely.geometry import Polygon

        @spatial_task(name="validate_task", validate_geometries=True)
        def process_task(gdf):
            return gdf.copy()

        # Create invalid polygon (self-intersecting)
        invalid_poly = Polygon([(0, 0), (1, 1), (1, 0), (0, 1), (0, 0)])
        gdf = gpd.GeoDataFrame({
            'id': [1]
        }, geometry=[invalid_poly], crs='EPSG:4326')

        # Should log warning about invalid geometries
        result = process_task(gdf)

        assert len(result) == 1


class TestProvenanceTracker:
    """Test ProvenanceTracker class"""

    def test_provenance_tracker_creation(self):
        """ProvenanceTracker should initialize correctly"""
        tracker = ProvenanceTracker("test_pipeline")

        assert tracker.pipeline_name == "test_pipeline"
        assert len(tracker.records) == 0
        assert tracker.start_time is not None
        assert tracker.end_time is None
        assert 'python_version' in tracker.environment

    def test_provenance_start_operation(self):
        """Should track new operations"""
        tracker = ProvenanceTracker("test_pipeline")

        record = tracker.start_operation("load_data", "task", {'path': 'data.gpkg'})

        assert record.operation_name == "load_data"
        assert record.operation_type == "task"
        assert record.parameters == {'path': 'data.gpkg'}
        assert len(tracker.records) == 1

    def test_provenance_complete_operation(self):
        """Should complete operations with execution time"""
        tracker = ProvenanceTracker("test_pipeline")

        record = tracker.start_operation("process")
        tracker.complete_operation(record, 1.5)

        assert record.execution_time == 1.5

    def test_provenance_record_error(self):
        """Should record errors"""
        tracker = ProvenanceTracker("test_pipeline")

        record = tracker.start_operation("failing_op")
        error = ValueError("Test error")
        tracker.record_error(record, error)

        assert record.error == "Test error"

    def test_provenance_finalize(self):
        """Should finalize tracking"""
        tracker = ProvenanceTracker("test_pipeline")
        tracker.finalize()

        assert tracker.end_time is not None

    def test_provenance_save_and_load(self, tmp_path):
        """Should save and load provenance"""
        tracker = ProvenanceTracker("test_pipeline")
        record = tracker.start_operation("op1")
        tracker.complete_operation(record, 1.0)
        tracker.finalize()

        # Save
        prov_file = tmp_path / "provenance.json"
        tracker.save(prov_file)

        assert prov_file.exists()

        # Load
        loaded = ProvenanceTracker.load(prov_file)

        assert loaded.pipeline_name == "test_pipeline"

    def test_provenance_get_summary(self):
        """Should generate execution summary"""
        tracker = ProvenanceTracker("test_pipeline")

        record1 = tracker.start_operation("op1")
        tracker.complete_operation(record1, 1.0)

        record2 = tracker.start_operation("op2")
        tracker.record_error(record2, ValueError("Error"))
        tracker.complete_operation(record2, 0.5)

        tracker.finalize()

        summary = tracker.get_summary()

        assert summary['pipeline_name'] == "test_pipeline"
        assert summary['total_operations'] == 2
        assert summary['failed_operations'] == 1
        assert len(summary['operations']) == 2
