"""
GeoFlow: Reproducible Geospatial Pipeline Framework

A Python framework for building reproducible, error-resistant spatial analysis workflows.
"""

from geoflow.io.loaders import load, save
from geoflow.spatial.operations import spatial_join, buffer, overlay, clip
from geoflow.validation.geometry import validate_geometry
from geoflow.core.pipeline import geo_pipeline
from geoflow.core.task import spatial_task

__version__ = "0.1.0"
__all__ = [
    # Data I/O
    "load",
    "save",
    # Spatial operations
    "spatial_join",
    "buffer",
    "overlay",
    "clip",
    # Validation
    "validate_geometry",
    # Pipeline orchestration (CORE FEATURES)
    "geo_pipeline",
    "spatial_task",
]
