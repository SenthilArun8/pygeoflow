"""Geometry validation and repair"""

import geopandas as gpd
import pandas as pd
import logging
from shapely.validation import explain_validity, make_valid
from typing import Literal

logger = logging.getLogger(__name__)


class GeometryValidator:
    """Validates and repairs geometries"""

    def find_invalid(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        '''
        Find invalid geometries and return them with validity issue explanations.

        find_invalid: gdf: gpd.GeoDataFrame -> gpd.GeoDataFrame

        Examples:
            validator.find_invalid(gdf) -> GeoDataFrame with 3 invalid features and 'validity_issue' column
        '''
        invalid_mask = ~gdf.geometry.is_valid
        invalid = gdf[invalid_mask].copy()

        if len(invalid) > 0:
            # Add explanation of why invalid
            invalid['validity_issue'] = invalid.geometry.apply(
                lambda geom: explain_validity(geom)
            )
            logger.warning(f"Found {len(invalid)} invalid geometries")
        else:
            logger.debug("All geometries are valid")

        return invalid

    def fix_invalid(
        self,
        gdf: gpd.GeoDataFrame,
        method: Literal['buffer', 'make_valid'] = 'make_valid'
    ) -> gpd.GeoDataFrame:
        '''
        Attempt to repair invalid geometries using buffer(0) or make_valid methods.

        fix_invalid: gdf: gpd.GeoDataFrame, method: Literal['buffer', 'make_valid'] = 'make_valid' -> gpd.GeoDataFrame

        Examples:
            validator.fix_invalid(gdf) -> GeoDataFrame with 2 geometries repaired using make_valid
            validator.fix_invalid(gdf, method='buffer') -> GeoDataFrame with geometries repaired using buffer(0)
        '''
        result = gdf.copy()
        invalid_mask = ~result.geometry.is_valid

        if not invalid_mask.any():
            logger.debug("No invalid geometries to fix")
            return result

        logger.info(f"Fixing {invalid_mask.sum()} invalid geometries using '{method}' method...")

        if method == 'buffer':
            # Classic buffer(0) trick
            result.loc[invalid_mask, 'geometry'] = result.loc[invalid_mask, 'geometry'].buffer(0)
        elif method == 'make_valid':
            # Modern Shapely 2.0+ approach
            result.loc[invalid_mask, 'geometry'] = result.loc[invalid_mask, 'geometry'].apply(
                make_valid
            )
        else:
            raise ValueError(f"Unknown repair method: {method}")

        # Check if fix worked
        still_invalid = ~result.geometry.is_valid
        if still_invalid.any():
            logger.warning(
                f"{still_invalid.sum()} geometries could not be fixed. "
                f"Consider manual inspection."
            )
        else:
            logger.info(f"Successfully fixed all {invalid_mask.sum()} invalid geometries")

        return result

    def validate_or_raise(self, gdf: gpd.GeoDataFrame):
        """
        Validate geometries and raise error if any are invalid.

        Args:
            gdf: GeoDataFrame to validate

        Raises:
            ValueError: If any geometries are invalid
        """
        invalid = self.find_invalid(gdf)
        if len(invalid) > 0:
            issues = invalid['validity_issue'].unique()
            raise ValueError(
                f"Found {len(invalid)} invalid geometries. "
                f"Issues: {', '.join(issues[:3])}"  # Show first 3 issues
            )

    def check_empty_geometries(self, gdf: gpd.GeoDataFrame) -> pd.Series:
        """
        Check for empty geometries.

        Args:
            gdf: GeoDataFrame to check

        Returns:
            Boolean series indicating which geometries are empty
        """
        empty_mask = gdf.geometry.is_empty
        if empty_mask.any():
            logger.warning(f"Found {empty_mask.sum()} empty geometries")
        return empty_mask

    def check_null_geometries(self, gdf: gpd.GeoDataFrame) -> pd.Series:
        """
        Check for null geometries.

        Args:
            gdf: GeoDataFrame to check

        Returns:
            Boolean series indicating which geometries are null
        """
        null_mask = gdf.geometry.isna()
        if null_mask.any():
            logger.warning(f"Found {null_mask.sum()} null geometries")
        return null_mask

    def get_validation_report(self, gdf: gpd.GeoDataFrame) -> dict:
        '''
        Generate comprehensive validation report with statistics on invalid, empty, and null geometries.

        get_validation_report: gdf: gpd.GeoDataFrame -> dict

        Examples:
            validator.get_validation_report(gdf) -> {'total_features': 100, 'invalid_count': 3, 'empty_count': 1, 'null_count': 0, 'invalid_percentage': 3.0}
        '''
        total = len(gdf)
        invalid = self.find_invalid(gdf)
        empty = self.check_empty_geometries(gdf)
        null = self.check_null_geometries(gdf)

        report = {
            'total_features': total,
            'invalid_count': len(invalid),
            'empty_count': empty.sum(),
            'null_count': null.sum(),
            'valid_count': total - len(invalid),
            'invalid_percentage': (len(invalid) / total * 100) if total > 0 else 0,
            'issues': invalid['validity_issue'].value_counts().to_dict() if len(invalid) > 0 else {}
        }

        return report


def validate_geometry(
    gdf: gpd.GeoDataFrame,
    auto_fix: bool = False,
    method: Literal['buffer', 'make_valid'] = 'make_valid',
    raise_on_invalid: bool = False
) -> gpd.GeoDataFrame:
    '''
    Validate geometries and optionally fix invalid ones. Detects self-intersections,
    bow-ties, and topology errors. Can auto-fix using buffer(0) or make_valid methods.
    Returns validated GeoDataFrame, optionally with repairs applied.

    validate_geometry: gdf: gpd.GeoDataFrame, auto_fix: bool = False,
                      method: Literal['buffer', 'make_valid'] = 'make_valid',
                      raise_on_invalid: bool = False -> gpd.GeoDataFrame

    Examples:
        validate_geometry(gdf) -> GeoDataFrame, warns about 3 invalid geometries
        validate_geometry(gdf, auto_fix=True) -> GeoDataFrame with all geometries fixed
        validate_geometry(gdf, raise_on_invalid=True) -> Raises ValueError if invalid found
    '''
    validator = GeometryValidator()

    if raise_on_invalid:
        validator.validate_or_raise(gdf)
        return gdf

    if auto_fix:
        return validator.fix_invalid(gdf, method=method)
    else:
        invalid = validator.find_invalid(gdf)
        if len(invalid) > 0:
            logger.warning(f"Found {len(invalid)} invalid geometries (not auto-fixing)")
        return gdf
