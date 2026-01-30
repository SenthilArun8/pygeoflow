"""
Data loading and saving with automatic format detection and provenance tracking
"""

import geopandas as gpd
from pathlib import Path
from typing import Optional, Union, Literal, Dict, Any
import logging
import json
from datetime import datetime
import sqlite3

from geoflow.validation.geometry import GeometryValidator

logger = logging.getLogger(__name__)

# Ingestion
class DataLoader:
    """Load geospatial data with format auto-detection"""

    # Include Raster and other vector formats
    SUPPORTED_FORMATS = {
        '.geojson': 'GeoJSON',
        '.shp': 'Shapefile',
        '.gpkg': 'GeoPackage',
        '.json': 'GeoJSON',
    }

    # default constructor
    def __init__(self):
        self.validator = GeometryValidator()

    def load(self,
             filepath: Union[str, Path],
             validate: bool = False,
             auto_fix: bool = False,
             fix_method: Literal['buffer', 'make_valid'] = 'make_valid',
             **kwargs) -> gpd.GeoDataFrame:
        '''
        Load geospatial data with automatic format detection and optional geometry validation.

        load: filepath: Union[str, Path], validate: bool = False, auto_fix: bool = False,
              fix_method: Literal['buffer', 'make_valid'] = 'make_valid' -> gpd.GeoDataFrame

        Examples:
            loader.load("data.geojson") -> GeoDataFrame with 100 features
            loader.load("messy.shp", validate=True, auto_fix=True) -> GeoDataFrame with fixed geometries
        '''
        filepath = Path(filepath)

        if not filepath.exists():
            raise FileNotFoundError(f"File not found: {filepath}")

        # Detect format
        suffix = filepath.suffix.lower()
        if suffix not in self.SUPPORTED_FORMATS:
            raise ValueError(
                f"Unsupported format: {suffix}. "
                f"Supported: {list(self.SUPPORTED_FORMATS.keys())}"
            )

        format_name = self.SUPPORTED_FORMATS[suffix]
        logger.info(f"Loading {format_name} file: {filepath.name}")

        # Load data
        gdf = gpd.read_file(filepath, **kwargs)

        # Log basic info
        logger.info(f"Loaded {len(gdf)} features")
        if gdf.crs:
            logger.info(f"CRS: {gdf.crs}")
        else:
            logger.warning("⚠️  No CRS detected!")

        # Validate geometries if requested
        if validate:
            invalid = self.validator.find_invalid(gdf)
            if len(invalid) > 0:
                logger.warning(f"Found {len(invalid)} invalid geometries")

                if auto_fix:
                    logger.info("Attempting to fix invalid geometries...")
                    gdf = self.validator.fix_invalid(gdf, method=fix_method)
                    logger.info("Geometry validation and repair completed")

        # Add metadata
        gdf.attrs['source_file'] = str(filepath)

        return gdf


class DataWriter:
    """Save geospatial data with provenance tracking"""

    SUPPORTED_FORMATS = {
        '.geojson': 'GeoJSON',
        '.shp': 'Shapefile',
        '.gpkg': 'GeoPackage',
        '.json': 'GeoJSON',
    }

    def save(
        self,
        gdf: gpd.GeoDataFrame,
        filepath: Union[str, Path],
        provenance: Optional[Dict[str, Any]] = None,
        embed_provenance: bool = True,
        driver: Optional[str] = None,
        **kwargs
    ) -> Path:
        '''
        Save geospatial data with optional provenance metadata embedded in file or as sidecar JSON.
        For GeoPackage, embeds provenance in metadata table. For GeoJSON and Shapefile, creates
        sidecar .provenance.json file.

        save: gdf: gpd.GeoDataFrame, filepath: Union[str, Path], provenance: Optional[Dict[str, Any]] = None,
              embed_provenance: bool = True, driver: Optional[str] = None -> Path

        Examples:
            writer.save(gdf, "output.gpkg") -> Path("output.gpkg")
            writer.save(gdf, "output.gpkg", provenance=result.provenance.to_dict()) -> Path("output.gpkg") with embedded metadata
        '''
        filepath = Path(filepath)
        suffix = filepath.suffix.lower()

        if suffix not in self.SUPPORTED_FORMATS:
            raise ValueError(
                f"Unsupported format: {suffix}. "
                f"Supported: {list(self.SUPPORTED_FORMATS.keys())}"
            )

        format_name = self.SUPPORTED_FORMATS[suffix]
        logger.info(f"Saving {format_name} file: {filepath.name}")

        # Save the geodata
        gdf.to_file(filepath, driver=driver, **kwargs)
        logger.info(f"Saved {len(gdf)} features to {filepath}")

        # Handle provenance if provided
        if provenance and embed_provenance:
            if suffix == '.gpkg':
                self._embed_provenance_gpkg(filepath, provenance)
            else:
                self._save_provenance_sidecar(filepath, provenance)

        return filepath

    def _embed_provenance_gpkg(self, gpkg_path: Path, provenance: Dict[str, Any]) -> None:
        """Embed provenance metadata inside GeoPackage"""
        try:
            conn = sqlite3.connect(str(gpkg_path))
            cursor = conn.cursor()

            # Create provenance table if not exists
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS geoflow_provenance (
                    id INTEGER PRIMARY KEY,
                    timestamp TEXT NOT NULL,
                    provenance_json TEXT NOT NULL
                )
            """)

            # Insert provenance
            timestamp = datetime.now().isoformat()
            provenance_json = json.dumps(provenance, indent=2)

            cursor.execute(
                "INSERT INTO geoflow_provenance (timestamp, provenance_json) VALUES (?, ?)",
                (timestamp, provenance_json)
            )

            conn.commit()
            conn.close()

            logger.info(f"✓ Embedded provenance in GeoPackage metadata table")

        except Exception as e:
            logger.warning(f"Could not embed provenance in GeoPackage: {e}")
            # Fallback to sidecar
            self._save_provenance_sidecar(gpkg_path, provenance)

    def _save_provenance_sidecar(self, data_path: Path, provenance: Dict[str, Any]) -> None:
        """Save provenance as sidecar JSON file"""
        provenance_path = data_path.with_suffix(data_path.suffix + '.provenance.json')

        # Add metadata about the data file
        provenance_with_meta = {
            'data_file': str(data_path.name),
            'saved_at': datetime.now().isoformat(),
            'provenance': provenance
        }

        with open(provenance_path, 'w') as f:
            json.dump(provenance_with_meta, f, indent=2)

        logger.info(f"✓ Saved provenance to sidecar: {provenance_path.name}")


# Global instances
_loader = DataLoader()
_writer = DataWriter()


def load(filepath: Union[str, Path], **kwargs) -> gpd.GeoDataFrame:
    '''
    Load geospatial data with automatic format detection and optional validation.
    Supports GeoJSON, Shapefile, and GeoPackage formats. Can validate and
    auto-fix invalid geometries during load. Returns GeoDataFrame with source
    file metadata attached.

    load: filepath: Union[str, Path], validate: bool = False, auto_fix: bool = False,
          fix_method: Literal['buffer', 'make_valid'] = 'make_valid' -> gpd.GeoDataFrame

    Examples:
        load("data.geojson") -> GeoDataFrame with 100 features
        load("messy.shp", validate=True, auto_fix=True) -> GeoDataFrame with fixed geometries
    '''
    return _loader.load(filepath, **kwargs)


def save(
    gdf: gpd.GeoDataFrame,
    filepath: Union[str, Path],
    provenance: Optional[Dict[str, Any]] = None,
    **kwargs
) -> Path:
    '''
    Save geospatial data with optional provenance metadata embedded in the output file.
    For GeoPackage format, provenance is stored in a metadata table. For GeoJSON and
    Shapefile formats, provenance is saved as a sidecar .provenance.json file. This
    closes the reproducibility loop by making outputs self-documenting. Returns path
    to saved file.

    save: gdf: gpd.GeoDataFrame, filepath: Union[str, Path],
          provenance: Optional[Dict[str, Any]] = None -> Path

    Examples:
        save(gdf, "output.gpkg") -> Path("output.gpkg")
        save(gdf, "output.gpkg", provenance=result.provenance.to_dict()) ->
            Path("output.gpkg") with embedded provenance metadata table
    '''
    return _writer.save(gdf, filepath, provenance=provenance, **kwargs)
