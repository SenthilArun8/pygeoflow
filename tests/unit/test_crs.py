import pytest
import geopandas as gpd
from shapely.geometry import Point
import logging

from geoflow.crs.manager import CRSManager


@pytest.fixture
def gdf_wgs84():
    """GeoDataFrame in WGS84"""
    return gpd.GeoDataFrame(
        [{'geometry': Point(0, 0)}],
        crs='EPSG:4326'
    )


@pytest.fixture
def gdf_utm():
    """GeoDataFrame in UTM Zone 10N"""
    return gpd.GeoDataFrame(
        [{'geometry': Point(500000, 4500000)}],
        crs='EPSG:32610'
    )


class TestCRSManager:

    def test_matching_crs_unchanged(self, gdf_wgs84):
        """Test that matching CRS doesn't trigger reprojection"""
        manager = CRSManager()
        gdf1 = gdf_wgs84
        gdf2 = gdf_wgs84.copy()

        result1, result2 = manager.ensure_common_crs(gdf1, gdf2)

        assert result1.crs == gdf1.crs
        assert result2.crs == gdf2.crs

    def test_different_crs_reprojected(self, gdf_wgs84, gdf_utm):
        """Test that CRS mismatch requires explicit target_crs."""
        manager = CRSManager()

        with pytest.raises(ValueError, match="CRS mismatch"):
            manager.ensure_common_crs(gdf_wgs84, gdf_utm)

        result1, result2 = manager.ensure_common_crs(
            gdf_wgs84, gdf_utm, target_crs='EPSG:32610'
        )

        assert result1.crs == result2.crs
        assert result2.crs.to_string() == 'EPSG:32610'

    def test_missing_crs_raises_error(self):
        """Test that missing CRS raises ValueError"""
        manager = CRSManager()
        gdf_no_crs = gpd.GeoDataFrame([{'geometry': Point(0, 0)}])
        gdf_with_crs = gpd.GeoDataFrame(
            [{'geometry': Point(0, 0)}],
            crs='EPSG:4326'
        )

        with pytest.raises(ValueError, match="no CRS"):
            manager.ensure_common_crs(gdf_no_crs, gdf_with_crs)

    def test_is_geographic(self, gdf_wgs84, gdf_utm):
        """Test geographic CRS detection"""
        manager = CRSManager()

        assert manager.is_geographic(gdf_wgs84.crs) is True
        assert manager.is_geographic(gdf_utm.crs) is False

    def test_warn_if_geographic(self, gdf_wgs84, caplog):
        """Test warning for geographic CRS operations"""
        manager = CRSManager()

        with caplog.at_level(logging.WARNING):
            manager.warn_if_geographic(gdf_wgs84, 'buffer')

        assert 'geographic CRS' in caplog.text
        assert 'buffer' in caplog.text

    def test_target_crs_specified(self, gdf_wgs84, gdf_utm):
        """Test reprojection to a specified target CRS"""
        manager = CRSManager()
        target = 'EPSG:3857'  # Web Mercator

        result1, result2 = manager.ensure_common_crs(
            gdf_wgs84, gdf_utm, target_crs=target
        )

        assert result1.crs.to_string() == target
        assert result2.crs.to_string() == target
