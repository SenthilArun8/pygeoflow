"""Tests for spatial operations (overlay, clip, etc.)"""

import pytest
import geopandas as gpd
from shapely.geometry import Point, Polygon, box

from geoflow.spatial.operations import overlay, clip


@pytest.fixture
def polygons1():
    """Create first set of test polygons"""
    return gpd.GeoDataFrame(
        {'id': [1, 2]},
        geometry=[
            box(0, 0, 2, 2),
            box(1, 1, 3, 3)
        ],
        crs='EPSG:4326'
    )


@pytest.fixture
def polygons2():
    """Create second set of test polygons"""
    return gpd.GeoDataFrame(
        {'id': [10, 20]},
        geometry=[
            box(1, 0, 3, 2),
            box(0, 1, 2, 3)
        ],
        crs='EPSG:4326'
    )


@pytest.fixture
def points_to_clip():
    """Create points for clipping"""
    points = [
        Point(0.5, 0.5),  # Inside
        Point(1.5, 1.5),  # Inside
        Point(5, 5)       # Outside
    ]
    return gpd.GeoDataFrame(
        {'id': [1, 2, 3]},
        geometry=points,
        crs='EPSG:4326'
    )


@pytest.fixture
def clip_boundary():
    """Create boundary for clipping"""
    return gpd.GeoDataFrame(
        {'id': [1]},
        geometry=[box(0, 0, 2, 2)],
        crs='EPSG:4326'
    )


class TestOverlay:

    def test_overlay_intersection(self, polygons1, polygons2):
        """Test intersection overlay"""
        result = overlay(polygons1, polygons2, how='intersection')

        assert len(result) > 0
        assert 'id_1' in result.columns
        assert 'id_2' in result.columns
        # All resulting geometries should be valid
        assert result.geometry.is_valid.all()

    def test_overlay_union(self, polygons1, polygons2):
        """Test union overlay"""
        result = overlay(polygons1, polygons2, how='union')

        assert len(result) > 0
        assert result.geometry.is_valid.all()

    def test_overlay_difference(self, polygons1, polygons2):
        """Test difference overlay"""
        result = overlay(polygons1, polygons2, how='difference')

        assert len(result) >= 0  # Could be empty
        assert result.geometry.is_valid.all()

    def test_overlay_symmetric_difference(self, polygons1, polygons2):
        """Test symmetric difference overlay"""
        result = overlay(polygons1, polygons2, how='symmetric_difference')

        assert len(result) >= 0
        assert result.geometry.is_valid.all()

    def test_overlay_crs_alignment(self):
        """Test that overlay requires explicit target_crs (SAFETY FIX!)"""
        gdf1 = gpd.GeoDataFrame(
            {'id': [1]},
            geometry=[box(0, 0, 2, 2)],
            crs='EPSG:4326'
        )
        gdf2 = gpd.GeoDataFrame(
            {'id': [2]},
            geometry=[box(500000, 4500000, 502000, 4502000)],
            crs='EPSG:32610'  # Different CRS
        )

        # CRITICAL: Should require explicit target_crs
        with pytest.raises(ValueError, match="CRS mismatch"):
            overlay(gdf1, gdf2, how='intersection')

        # Correct usage: specify target_crs
        result = overlay(gdf1, gdf2, how='intersection', target_crs='EPSG:32610')
        assert result.crs.to_string() == 'EPSG:32610'

    def test_overlay_preserves_attributes(self, polygons1, polygons2):
        """Test that overlay preserves data attributes"""
        result = overlay(polygons1, polygons2, how='intersection')

        # Should have columns from both input GeoDataFrames
        assert 'id_1' in result.columns
        assert 'id_2' in result.columns


class TestClip:

    def test_clip_points_to_polygon(self, points_to_clip, clip_boundary):
        """Test clipping points to a polygon"""
        result = clip(points_to_clip, clip_boundary)

        # Only points inside boundary should remain
        assert len(result) == 2
        assert result['id'].tolist() == [1, 2]

    def test_clip_crs_alignment(self):
        """Test that clip requires explicit target_crs (SAFETY FIX!)"""
        points = gpd.GeoDataFrame(
            {'id': [1, 2]},
            geometry=[Point(0, 0), Point(1, 1)],
            crs='EPSG:4326'
        )
        boundary = gpd.GeoDataFrame(
            {'id': [1]},
            geometry=[box(499000, 4499000, 501000, 4501000)],
            crs='EPSG:32610'  # Different CRS
        )

        # CRITICAL: Should require explicit target_crs
        with pytest.raises(ValueError, match="CRS mismatch"):
            clip(points, boundary)

        # Correct usage: specify target_crs
        result = clip(points, boundary, target_crs='EPSG:32610')

        # Result should have a CRS
        assert result.crs is not None

    def test_clip_preserves_attributes(self, points_to_clip, clip_boundary):
        """Test that clip preserves data attributes"""
        result = clip(points_to_clip, clip_boundary)

        # Should preserve original attributes
        assert 'id' in result.columns
        assert result.crs == points_to_clip.crs

    def test_clip_polygons(self):
        """Test clipping polygons to a boundary"""
        polygons = gpd.GeoDataFrame(
            {'id': [1, 2, 3]},
            geometry=[
                box(0, 0, 2, 2),
                box(1.5, 1.5, 3, 3),
                box(5, 5, 7, 7)  # Outside
            ],
            crs='EPSG:4326'
        )
        boundary = gpd.GeoDataFrame(
            {'id': [1]},
            geometry=[box(0, 0, 2.5, 2.5)],
            crs='EPSG:4326'
        )

        result = clip(polygons, boundary)

        # Should have clipped geometries
        assert len(result) > 0
        # All geometries should be within or intersecting boundary
        assert result.geometry.is_valid.all()
