# common/usgs/nhdplus_h/core.py
import logging
import pathlib
from os import PathLike

import geopandas as gpd

from common.utils.gis import get_clipped_vector_data  # Ensure this path is correct

logger = logging.getLogger(__name__)

# --- Default Paths ---
DEFAULT_NHDPLUS_CACHE_DIR = (
    pathlib.Path(__file__).parent.parent.parent.parent
    / "data/pickles/usgs/nhdplus_h/flowlines"
)
DEFAULT_NHDPLUS_FLOWLINES_LAYER_NAME = "NetworkNHDFlowline"
DEFAULT_NHDPLUS_FLOWLINES_DATA_VERSION = "0.1.1"
DEFAULT_NHDPLUS_FLOWLINES_VERSION_ATTR = "nhdplus_flowlines_data_version"


def get_clipped_nhdplus_flowlines(
    nhdplus_path: PathLike,  # Changed from _zip_path in doc to match param
    buffered_region_gdf: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """
    Retrieves NHDPlus flowlines clipped to a buffered region,
    with 'permanent_identifier' set as a unique index.

    This core domain function is oblivious to workflow orchestration frameworks.
    It uses a generic vector clipping utility and applies NHDPlus-specific
    configurations.

    Args:
        nhdplus_gdb_path: Path to the NHDPlus H GDB file or collection.
        buffered_region_gdf: GeoDataFrame representing the area to clip against.

    Returns:
        A GeoDataFrame containing the clipped NHDPlus flowlines, indexed by
        'permanent_identifier'.

    Raises:
        ValueError: If the 'permanent_identifier' column does not exist or
                    if its values are not unique.
    """
    logger.info(
        f"Core: Requesting clipped NHDPlus flowlines from '{nhdplus_path}' "
        f"using layer '{DEFAULT_NHDPLUS_FLOWLINES_LAYER_NAME}'."
    )

    clipped_flowlines_gdf = get_clipped_vector_data(
        vector_data_path=nhdplus_path,
        mask_gdf=buffered_region_gdf,
        cache_dir=DEFAULT_NHDPLUS_CACHE_DIR,
        gdf_version=DEFAULT_NHDPLUS_FLOWLINES_DATA_VERSION,
        gdf_version_attr_name=DEFAULT_NHDPLUS_FLOWLINES_VERSION_ATTR,
        layer_name=DEFAULT_NHDPLUS_FLOWLINES_LAYER_NAME,
    )

    if clipped_flowlines_gdf.empty:
        logger.warning(
            "Core: Retrieved GeoDataFrame is empty. No features to process or index."
        )

        return clipped_flowlines_gdf

    # --- Set 'permanent_identifier' as index ---
    primary_key_column = "permanent_identifier"

    # 1. Verify the column exists
    if primary_key_column not in clipped_flowlines_gdf.columns:
        logger.error(
            f"Core: Critical - Primary key column '{primary_key_column}' "
            "not found in the retrieved data."
        )
        raise ValueError(f"Primary key column '{primary_key_column}' not found.")

    # 2. Verify the column values are unique before setting as index
    if not clipped_flowlines_gdf[primary_key_column].is_unique:
        # Count duplicates for a more informative error message
        num_duplicates = clipped_flowlines_gdf[primary_key_column].duplicated().sum()
        logger.error(
            f"Core: Critical - Values in '{primary_key_column}' column are not unique. "
            f"Found {num_duplicates} duplicate value(s). "
            "Cannot set as a unique primary key index."
        )
        raise ValueError(
            f"Primary key column '{primary_key_column}' contains duplicate values. "
            "Cannot reliably set as index."
        )

    # 3. Set the index
    # Using inplace=False (default) and reassigning is often preferred
    # for clarity and to avoid SettingWithCopyWarning in more complex scenarios.
    clipped_flowlines_gdf = clipped_flowlines_gdf.set_index(primary_key_column)
    logger.info(f"Core: Successfully set '{primary_key_column}' as the index.")

    logger.info(
        "Core: Successfully retrieved/generated and indexed clipped NHDPlus flowlines. "
        f"Features: {len(clipped_flowlines_gdf)}"
    )
    return clipped_flowlines_gdf
