"""Task decorators for spatial operations with semantic validation"""

import functools
import time
from typing import Any, Callable, List, Optional, Union
import geopandas as gpd
import logging

from geoflow.core.provenance import ProvenanceRecord

logger = logging.getLogger(__name__)


class SpatialTask:
    """
    Wrapper for a spatial task with automatic validation and semantic checks.

    Created by @spatial_task decorator. Provides:
    - CRS validation and warnings
    - Geometry validation
    - Semantic anti-pattern detection
    - Operation metadata capture
    """

    def __init__(
        self,
        func: Callable,
        name: Optional[str] = None,
        validate_crs: bool = True,
        validate_geometries: bool = False,
        warn_geographic: bool = True,
        strict_crs: bool = False
    ):
        self.func = func
        self.name = name or func.__name__
        self.validate_crs = validate_crs
        self.validate_geometries = validate_geometries
        self.warn_geographic = warn_geographic
        self.strict_crs = strict_crs

        functools.update_wrapper(self, func)

        # Track if we're inside a pipeline (set by pipeline context)
        self._provenance_record: Optional[ProvenanceRecord] = None

    def _check_geographic_crs(self, gdf: gpd.GeoDataFrame, operation: str) -> None:
        """Check if operation is being performed in geographic CRS"""
        if gdf.crs and gdf.crs.is_geographic:
            message = (
                f"⚠️  WARNING: {operation} in geographic CRS ({gdf.crs}).\n"
                f"   Results will be in degrees, not meters!\n"
                f"   Consider reprojecting to a projected CRS first (e.g., UTM)."
            )

            if self.strict_crs:
                raise ValueError(
                    f"Cannot perform {operation} in geographic CRS {gdf.crs}. "
                    f"Reproject to a projected CRS first.\n"
                    f"Example: gdf.to_crs('EPSG:32610')  # UTM Zone 10N"
                )
            else:
                logger.warning(message)

    def _validate_geometries(self, gdf: gpd.GeoDataFrame) -> None:
        """Validate geometries before operation"""
        invalid_count = (~gdf.geometry.is_valid).sum()
        if invalid_count > 0:
            logger.warning(
                f"Found {invalid_count} invalid geometries. "
                f"Consider using validate_geometry() to fix them."
            )

    def _detect_semantic_issues(
        self,
        args: tuple,
        kwargs: dict,
        operation_type: str
    ) -> List[str]:
        """Detect common GIS anti-patterns"""
        warnings = []

        # Check for buffer operations
        if 'buffer' in self.name.lower() or operation_type == 'buffer':
            for arg in args:
                if isinstance(arg, gpd.GeoDataFrame):
                    if arg.crs and arg.crs.is_geographic:
                        warnings.append(
                            f"ANTI-PATTERN: Buffering in geographic CRS. "
                            f"Reproject to projected CRS first."
                        )

        # Check for distance-based operations with large datasets
        if 'distance' in kwargs:
            for arg in args:
                if isinstance(arg, gpd.GeoDataFrame) and len(arg) > 10000:
                    if not hasattr(arg, '_sindex'):
                        warnings.append(
                            f"PERFORMANCE: Large dataset ({len(arg)} features) without spatial index. "
                            f"Consider creating index for faster queries."
                        )

        # Check for CRS mismatches in joins/overlays
        if any(op in self.name.lower() for op in ['join', 'overlay', 'intersection']):
            gdfs = [arg for arg in args if isinstance(arg, gpd.GeoDataFrame)]
            if len(gdfs) >= 2:
                crs_list = [gdf.crs for gdf in gdfs if gdf.crs]
                if len(set(str(crs) for crs in crs_list)) > 1:
                    warnings.append(
                        f"CRS MISMATCH: Multiple CRS detected. "
                        f"GeoFlow will auto-align but consider standardizing beforehand."
                    )

        return warnings

    def __call__(self, *args, **kwargs) -> Any:
        """Execute the task with validation and semantic checks"""
        logger.debug(f"Executing spatial task '{self.name}'")

        # Semantic checks
        semantic_warnings = self._detect_semantic_issues(args, kwargs, self.name)
        for warning in semantic_warnings:
            logger.warning(f"[SEMANTIC CHECK] {warning}")

        # Pre-execution validations
        for arg in args:
            if isinstance(arg, gpd.GeoDataFrame):
                if self.warn_geographic and 'buffer' in self.name.lower():
                    self._check_geographic_crs(arg, "Buffering")

                if self.validate_geometries:
                    self._validate_geometries(arg)

        # Execute function
        start_time = time.time()
        try:
            result = self.func(*args, **kwargs)
            execution_time = time.time() - start_time

            logger.debug(f"Task '{self.name}' completed in {execution_time:.3f}s")
            return result

        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Task '{self.name}' failed after {execution_time:.3f}s: {e}")
            raise


def spatial_task(
    name: Optional[str] = None,
    validate_crs: bool = True,
    validate_geometries: bool = False,
    warn_geographic: bool = True,
    strict_crs: bool = False
):
    '''
    Decorator that adds safety checks and anti-pattern detection to spatial operations.
    Validates CRS compatibility, checks for invalid geometries, detects geographic CRS
    operations (degrees vs meters), identifies missing spatial indexes on large datasets,
    and catches CRS mismatches in joins and overlays. Use strict_crs=True to error on
    geographic operations instead of warning.

    spatial_task: name: Optional[str] = None, validate_crs: bool = True,
                  validate_geometries: bool = False, warn_geographic: bool = True,
                  strict_crs: bool = False -> Callable[[Callable], SpatialTask]

    Examples:
        @spatial_task(name="buffer_roads")
        def buffer_features(gdf, distance):
            return buffer(gdf, distance)
        buffer_features(gdf_utm, 100) -> Buffered GeoDataFrame with validation checks
        @spatial_task(name="safe_buffer", strict_crs=True)
        def buffer_features(gdf, distance):
            return buffer(gdf, distance)
        buffer_features(gdf_wgs84, 0.01) -> Raises ValueError: Cannot buffer in geographic CRS
    '''

    def decorator(func: Callable) -> SpatialTask:
        return SpatialTask(
            func,
            name=name,
            validate_crs=validate_crs,
            validate_geometries=validate_geometries,
            warn_geographic=warn_geographic,
            strict_crs=strict_crs
        )

    return decorator
