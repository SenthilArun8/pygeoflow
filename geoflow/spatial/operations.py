"""Safe spatial operations with CRS handling"""

import geopandas as gpd
import logging
from typing import Literal

from geoflow.crs.manager import CRSManager

logger = logging.getLogger(__name__)
crs_manager = CRSManager()


def spatial_join(
    left: gpd.GeoDataFrame,
    right: gpd.GeoDataFrame,
    how: str = 'inner',
    predicate: str = 'intersects',
    target_crs=None,
    **kwargs
) -> gpd.GeoDataFrame:
    '''
    Perform spatial join between two GeoDataFrames with automatic CRS safety checks.
    If CRS mismatch detected, raises error requiring explicit target_crs parameter.
    Prevents silent spatial errors from arbitrary CRS choices. Returns joined
    GeoDataFrame.

    spatial_join: left: gpd.GeoDataFrame, right: gpd.GeoDataFrame, how: str = 'inner',
                  predicate: str = 'intersects', target_crs = None -> gpd.GeoDataFrame

    Examples:
        spatial_join(parcels, zones) -> GeoDataFrame with 45 joined features
        spatial_join(gdf1, gdf2, target_crs='EPSG:32610') -> GeoDataFrame reprojected to UTM
        spatial_join(left, right, how='left', predicate='within') -> Left join result
    '''
    left, right = crs_manager.ensure_common_crs(left, right, target_crs)
    result = gpd.sjoin(left, right, how=how, predicate=predicate, **kwargs)
    logger.info(f"Spatial join completed: {len(result)} features")
    return result


def buffer(
    gdf: gpd.GeoDataFrame,
    distance: float,
    **kwargs
) -> gpd.GeoDataFrame:
    '''
    Create buffer around geometries with CRS safety warnings. If geographic CRS
    detected, warns user that distance is in degrees not meters to prevent
    massive errors. Returns new GeoDataFrame with buffered geometries.

    buffer: gdf: gpd.GeoDataFrame, distance: float -> gpd.GeoDataFrame

    Examples:
        buffer(gdf_utm, distance=100) -> GeoDataFrame with 100m buffers
        buffer(gdf_wgs84, distance=0.01) -> Warning: degrees not meters, returns buffered GeoDataFrame
    '''
    crs_manager.warn_if_geographic(gdf, 'buffer')
    result = gdf.copy()
    result.geometry = result.geometry.buffer(distance, **kwargs)
    logger.info(f"Buffered {len(gdf)} features by {distance} units")
    return result


def overlay(
    gdf1: gpd.GeoDataFrame,
    gdf2: gpd.GeoDataFrame,
    how: Literal['intersection', 'union', 'difference', 'symmetric_difference'] = 'intersection',
    keep_geom_type: bool = False,
    target_crs=None,
    **kwargs
) -> gpd.GeoDataFrame:
    '''
    Perform geometric overlay operation with CRS safety. Supports intersection, union,
    difference, and symmetric_difference. Requires explicit target_crs if CRS mismatch
    detected. Returns GeoDataFrame with overlay result.

    overlay: gdf1: gpd.GeoDataFrame, gdf2: gpd.GeoDataFrame,
             how: Literal['intersection', 'union', 'difference', 'symmetric_difference'] = 'intersection',
             target_crs = None -> gpd.GeoDataFrame

    Examples:
        overlay(parcels, zones, how='intersection') -> GeoDataFrame with intersected areas
        overlay(gdf1, gdf2, how='union', target_crs='EPSG:32610') -> Union result in UTM
    '''
    gdf1, gdf2 = crs_manager.ensure_common_crs(gdf1, gdf2, target_crs)
    result = gpd.overlay(gdf1, gdf2, how=how, keep_geom_type=keep_geom_type, **kwargs)
    logger.info(f"Overlay ({how}) completed: {len(result)} features")
    return result


def clip(
    gdf: gpd.GeoDataFrame,
    mask: gpd.GeoDataFrame,
    target_crs=None,
    **kwargs
) -> gpd.GeoDataFrame:
    '''
    Clip geometries to mask extent with CRS safety. Requires explicit target_crs if
    CRS mismatch detected between gdf and mask. Returns GeoDataFrame with clipped
    geometries only.

    clip: gdf: gpd.GeoDataFrame, mask: gpd.GeoDataFrame, target_crs = None -> gpd.GeoDataFrame

    Examples:
        clip(parcels, boundary) -> GeoDataFrame with 120 features clipped to boundary
        clip(gdf, mask, target_crs='EPSG:32610') -> Clipped result in UTM
    '''
    gdf, mask = crs_manager.ensure_common_crs(gdf, mask, target_crs)
    result = gpd.clip(gdf, mask, **kwargs)
    logger.info(f"Clipped to {len(result)} features (from {len(gdf)})")
    return result
