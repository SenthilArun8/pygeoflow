import pytest
import geopandas as gpd
from shapely.geometry import Point, Polygon, LineString
from shapely import wkt
import logging

from geoflow.validation.geometry import GeometryValidator, validate_geometry


@pytest.fixture
def valid_gdf():
    """Create a GeoDataFrame with valid geometries"""
    return gpd.GeoDataFrame(
        {'id': [1, 2, 3]},
        geometry=[
            Point(0, 0),
            Point(1, 1),
            Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)])
        ],
        crs='EPSG:4326'
    )


@pytest.fixture
def invalid_gdf():
    """Create a GeoDataFrame with invalid geometries (self-intersecting polygon)"""
    # Self-intersecting polygon (bowtie)
    invalid_poly = Polygon([(0, 0), (1, 1), (1, 0), (0, 1), (0, 0)])

    return gpd.GeoDataFrame(
        {'id': [1, 2, 3]},
        geometry=[
            Point(0, 0),  # Valid
            invalid_poly,  # Invalid
            Point(2, 2)   # Valid
        ],
        crs='EPSG:4326'
    )


@pytest.fixture
def gdf_with_empty():
    """Create a GeoDataFrame with empty geometries"""
    return gpd.GeoDataFrame(
        {'id': [1, 2, 3]},
        geometry=[
            Point(0, 0),
            Point(),  # Empty point
            Point(2, 2)
        ],
        crs='EPSG:4326'
    )


@pytest.fixture
def gdf_with_null():
    """Create a GeoDataFrame with null geometries"""
    gdf = gpd.GeoDataFrame(
        {'id': [1, 2, 3]},
        geometry=[Point(0, 0), Point(1, 1), Point(2, 2)],
        crs='EPSG:4326'
    )
    gdf.loc[1, 'geometry'] = None
    return gdf


class TestGeometryValidator:

    def test_find_invalid_all_valid(self, valid_gdf):
        """Test finding invalid geometries when all are valid"""
        validator = GeometryValidator()
        invalid = validator.find_invalid(valid_gdf)

        assert len(invalid) == 0

    def test_find_invalid_with_invalid(self, invalid_gdf):
        """Test finding invalid geometries"""
        validator = GeometryValidator()
        invalid = validator.find_invalid(invalid_gdf)

        assert len(invalid) == 1
        assert 'validity_issue' in invalid.columns
        assert invalid.iloc[0]['id'] == 2

    def test_fix_invalid_buffer_method(self, invalid_gdf):
        """Test fixing invalid geometries with buffer method"""
        validator = GeometryValidator()
        fixed = validator.fix_invalid(invalid_gdf, method='buffer')

        # All should be valid after fix
        assert fixed.geometry.is_valid.all()
        assert len(fixed) == len(invalid_gdf)

    def test_fix_invalid_make_valid_method(self, invalid_gdf):
        """Test fixing invalid geometries with make_valid method"""
        validator = GeometryValidator()
        fixed = validator.fix_invalid(invalid_gdf, method='make_valid')

        # All should be valid after fix
        assert fixed.geometry.is_valid.all()
        assert len(fixed) == len(invalid_gdf)

    def test_fix_invalid_unknown_method(self, invalid_gdf):
        """Test that unknown method raises error"""
        validator = GeometryValidator()

        with pytest.raises(ValueError, match="Unknown repair method"):
            validator.fix_invalid(invalid_gdf, method='unknown')

    def test_fix_invalid_no_invalid(self, valid_gdf):
        """Test fixing when no invalid geometries exist"""
        validator = GeometryValidator()
        fixed = validator.fix_invalid(valid_gdf)

        assert len(fixed) == len(valid_gdf)
        assert fixed.geometry.is_valid.all()

    def test_validate_or_raise_valid(self, valid_gdf):
        """Test validate_or_raise with valid geometries"""
        validator = GeometryValidator()
        # Should not raise
        validator.validate_or_raise(valid_gdf)

    def test_validate_or_raise_invalid(self, invalid_gdf):
        """Test validate_or_raise with invalid geometries"""
        validator = GeometryValidator()

        with pytest.raises(ValueError, match="invalid geometries"):
            validator.validate_or_raise(invalid_gdf)

    def test_check_empty_geometries(self, gdf_with_empty):
        """Test checking for empty geometries"""
        validator = GeometryValidator()
        empty_mask = validator.check_empty_geometries(gdf_with_empty)

        assert empty_mask.sum() == 1
        assert empty_mask.iloc[1] == True

    def test_check_null_geometries(self, gdf_with_null):
        """Test checking for null geometries"""
        validator = GeometryValidator()
        null_mask = validator.check_null_geometries(gdf_with_null)

        assert null_mask.sum() == 1
        assert null_mask.iloc[1] == True

    def test_get_validation_report_valid(self, valid_gdf):
        """Test validation report for valid geometries"""
        validator = GeometryValidator()
        report = validator.get_validation_report(valid_gdf)

        assert report['total_features'] == 3
        assert report['invalid_count'] == 0
        assert report['valid_count'] == 3
        assert report['invalid_percentage'] == 0
        assert report['issues'] == {}

    def test_get_validation_report_invalid(self, invalid_gdf):
        """Test validation report for invalid geometries"""
        validator = GeometryValidator()
        report = validator.get_validation_report(invalid_gdf)

        assert report['total_features'] == 3
        assert report['invalid_count'] == 1
        assert report['valid_count'] == 2
        assert report['invalid_percentage'] == pytest.approx(33.33, rel=0.1)
        assert len(report['issues']) > 0

    def test_get_validation_report_empty(self, gdf_with_empty):
        """Test validation report with empty geometries"""
        validator = GeometryValidator()
        report = validator.get_validation_report(gdf_with_empty)

        assert report['empty_count'] == 1

    def test_get_validation_report_null(self, gdf_with_null):
        """Test validation report with null geometries"""
        validator = GeometryValidator()
        report = validator.get_validation_report(gdf_with_null)

        assert report['null_count'] == 1


class TestValidateGeometryFunction:

    def test_validate_geometry_no_fix(self, invalid_gdf):
        """Test validate_geometry without auto-fix"""
        result = validate_geometry(invalid_gdf, auto_fix=False)

        # Should return unchanged
        assert len(result) == len(invalid_gdf)
        # Still has invalid geometry
        assert not result.geometry.is_valid.all()

    def test_validate_geometry_with_fix(self, invalid_gdf):
        """Test validate_geometry with auto-fix"""
        result = validate_geometry(invalid_gdf, auto_fix=True)

        # Should fix invalid geometries
        assert result.geometry.is_valid.all()

    def test_validate_geometry_buffer_method(self, invalid_gdf):
        """Test validate_geometry with buffer method"""
        result = validate_geometry(invalid_gdf, auto_fix=True, method='buffer')

        assert result.geometry.is_valid.all()

    def test_validate_geometry_raise_on_invalid(self, invalid_gdf):
        """Test validate_geometry with raise_on_invalid"""
        with pytest.raises(ValueError, match="invalid geometries"):
            validate_geometry(invalid_gdf, raise_on_invalid=True)

    def test_validate_geometry_raise_on_invalid_valid(self, valid_gdf):
        """Test validate_geometry with raise_on_invalid on valid data"""
        # Should not raise
        result = validate_geometry(valid_gdf, raise_on_invalid=True)
        assert len(result) == len(valid_gdf)

    def test_validate_geometry_preserves_attributes(self, invalid_gdf):
        """Test that validation preserves data attributes"""
        result = validate_geometry(invalid_gdf, auto_fix=True)

        assert 'id' in result.columns
        assert result['id'].tolist() == [1, 2, 3]
        assert result.crs == invalid_gdf.crs
