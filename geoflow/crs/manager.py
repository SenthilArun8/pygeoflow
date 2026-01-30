"""CRS management and safety checks"""

import geopandas as gpd
from pyproj import CRS
import logging
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


class CRSManager:
    """Manage CRS operations and ensure spatial safety"""

    def ensure_common_crs(
        self,
        gdf1: gpd.GeoDataFrame,
        gdf2: gpd.GeoDataFrame,
        target_crs: Optional[CRS] = None
    ) -> Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
        '''
        Ensure two GeoDataFrames share same CRS by reprojecting to target_crs if needed.
        If CRS mismatch detected and no target_crs provided, raises ValueError forcing
        explicit CRS choice. This prevents silent spatial errors. Returns tuple of both
        GeoDataFrames in common CRS.

        ensure_common_crs: gdf1: gpd.GeoDataFrame, gdf2: gpd.GeoDataFrame,
                          target_crs: Optional[CRS] = None -> Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]

        Examples:
            ensure_common_crs(gdf1, gdf2, target_crs='EPSG:32610') -> (gdf1_utm, gdf2_utm)
            ensure_common_crs(gdf_wgs84, gdf_utm) -> Raises ValueError: must specify target_crs
        '''
        if gdf1.crs is None:
            raise ValueError("gdf1 has no CRS defined")
        if gdf2.crs is None:
            raise ValueError("gdf2 has no CRS defined")

        if gdf1.crs == gdf2.crs:
            logger.debug("CRS already match")
            return gdf1, gdf2

        # CRS MISMATCH DETECTED
        logger.warning(
            f"⚠️  CRS mismatch detected! "
            f"gdf1: {gdf1.crs}, gdf2: {gdf2.crs}"
        )

        if target_crs is None:
            raise ValueError(
                f"CRS mismatch: gdf1 has {gdf1.crs}, gdf2 has {gdf2.crs}.\n"
                f"You must specify target_crs to avoid spatial errors.\n"
                f"Suggestions:\n"
                f"  - If both are geographic (EPSG:4326), reproject to appropriate UTM zone\n"
                f"  - If one is projected, consider using that CRS\n"
                f"  - For global analysis, consider EPSG:3857 (Web Mercator)\n"
                f"Example: spatial_join(gdf1, gdf2, target_crs='EPSG:32610')  # UTM Zone 10N"
            )

        # User explicitly specified target_crs - proceed safely
        logger.info(f"Reprojecting both to {target_crs}")
        gdf1 = gdf1.to_crs(target_crs)
        gdf2 = gdf2.to_crs(target_crs)

        return gdf1, gdf2

    def is_geographic(self, crs: CRS) -> bool:
        """Check if CRS is geographic (lat/lon)"""
        return crs.is_geographic

    def warn_if_geographic(self, gdf: gpd.GeoDataFrame, operation: str):
        """Warn if performing metric operation in geographic CRS"""
        if gdf.crs and self.is_geographic(gdf.crs):
            logger.warning(
                f"⚠️  Performing '{operation}' in geographic CRS ({gdf.crs}). "
                f"Results will be in degrees, not meters! "
                f"Consider reprojecting to a projected CRS."
            )
