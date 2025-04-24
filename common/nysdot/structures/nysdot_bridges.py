import logging
import pathlib
import re
from typing import Any, Dict, Optional, Union

import geopandas as gpd
import pandas as pd
import pyogrio

# --- Logger Setup ---
# Configure logging in the calling script/orchestrator (e.g., Prefect flow, worker script)
# Example configuration:
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_clipped_nysdot_bridges_data(
    nysdot_bridges_source: Union[str, pathlib.Path],
    buffered_region_gdf: gpd.GeoDataFrame,
    layer_name: Optional[str] = None,
    filter_out_rail_bridges: bool = True,
    rail_filter_column: str = "PrimaryOwn",
    rail_pattern: str = r".*RAILROAD.*",
) -> gpd.GeoDataFrame:
    """
    Reads NYSDOT bridge point data, clips it to a buffered region,
    optionally filters out railroad bridges, and returns the result.

    Uses Python's logging module for status updates. Configure the logger
    level (e.g., logging.INFO) externally to see messages.

    Args:
        nysdot_bridges_source: Path to the NYSDOT bridges data source.
        buffered_region_gdf: GeoDataFrame representing the region of interest.
                             Must have a CRS defined.
        layer_name: Specific layer name within the data source (optional).
        filter_out_rail_bridges: If True, filter railroad bridges. Defaults to True.
        rail_filter_column: Column name for railroad filtering. Defaults to 'PrimaryOwn'.
        rail_pattern: Regex pattern for railroad filtering. Defaults to r'.*RAILROAD.*'.

    Returns:
        gpd.GeoDataFrame:
            A GeoDataFrame containing the NYSDOT bridge points that fall
            within the `buffered_region_gdf`.
            The schema includes columns like:
             - ObjectID (int): Unique object identifier.
             - BIN (str): Bridge Identification Number.
             - Carried (str, optional): Roadway carried by the bridge.
             - Crossed (str, optional): Feature crossed by the bridge.
             - PrimaryOwn (str, optional): Owning agency.
             - PrimaryMai (str, optional): Maintaining agency.
             - County (str, optional): County name.
             - Region (str, optional): NYSDOT Region number/name.
             - GTMSStruct (str, optional): Structure type description.
             - GTMSMateri (str, optional): Material type description.
             - NumberOfSp (int, optional): Number of spans.
             - ConditionR (float, optional): Condition rating.
             - LastInspec (datetime, optional): Last inspection date.
             - BridgeLeng (float, optional): Bridge length (units as per source).
             - DeckAreaSq (float, optional): Deck area (units as per source).
             - AADT (int, optional): Average Annual Daily Traffic.
             - YearBuilt (int, optional): Year the bridge was built.
             - PostedLoad (float, optional): Posted load limit (often NaN if none).
             - RPosted (str, optional): Roadway posted flag ('YES'/'NO').
             - Other (float, optional): Other posting value (often NaN).
             - REDC (str, optional): Regional Economic Development Council.
             - NBI_DeckCo (str, optional): NBI Deck Condition Code.
             - NBI_Substr (str, optional): NBI Substructure Condition Code.
             - NBI_Supers (str, optional): NBI Superstructure Condition Code.
             - LocationLa (datetime, optional): Unknown purpose date field.
             - FHWA_Condi (str, optional): FHWA Condition ('Good', 'Fair', 'Poor').
             - GEOID (str, optional): Associated Census GEOID.
             - geometry (Point): The point geometry of the bridge location.
            The CRS of the returned GeoDataFrame will match the CRS of the
            input `buffered_region_gdf`.

    Raises:
        ValueError: If inputs are invalid (e.g., empty region, missing CRS/column).
        FileNotFoundError: If `nysdot_bridges_source` is not found.
        RuntimeError: For CRS, projection, file reading, or spatial join errors.
    """
    # --- Input Validation ---
    # (Error raising logic remains the same, no logging needed for these)
    if buffered_region_gdf.empty:
        raise ValueError("Input 'buffered_region_gdf' cannot be empty.")
    # ... (other validations for CRS, file existence, rail_filter_column) ...
    source_path = pathlib.Path(nysdot_bridges_source)  # Validate path earlier maybe
    if not source_path.exists():
        raise FileNotFoundError(f"NYSDOT bridges source not found at: {source_path}")
    if buffered_region_gdf.crs is None:
        raise ValueError("Input 'buffered_region_gdf' must have a CRS defined.")

    # --- Processing Steps ---

    logger.info(f"Processing NYSDOT bridges from: {source_path}")
    if layer_name:
        logger.info(f"Using layer: {layer_name}")

    # Get Data CRS and Prepare Mask
    try:
        source_info: Dict[str, Any] = pyogrio.read_info(
            source_path, layer=layer_name, encoding="utf-8"
        )
        source_crs = source_info["crs"]
        if source_crs is None:
            raise ValueError("Could not determine CRS from data source.")
        logger.info(f"Source data CRS detected: {source_crs}")

        logger.debug("Projecting region mask to match data source CRS...")
        mask_gdf: gpd.GeoDataFrame = buffered_region_gdf.to_crs(source_crs)
        logger.debug("Region mask projected successfully.")

    except Exception as e:
        logger.error(f"Failed during CRS handling or mask projection. Error: {e}")
        raise RuntimeError(f"Failed during CRS handling or mask projection: {e}")

    # Read Data using BBox Filter
    logger.info("Reading bridge features using bounding box filter...")
    try:
        points_in_bbox_gdf: gpd.GeoDataFrame = gpd.read_file(
            source_path,
            engine="pyogrio",
            layer=layer_name,
            bbox=tuple(mask_gdf.total_bounds),
            encoding="utf-8",
        )
        logger.info(f"Read {len(points_in_bbox_gdf)} features within bounding box.")
    except Exception as e:
        logger.error(f"Failed to read data file '{source_path}'. Error: {e}")
        raise RuntimeError(f"Failed to read data file '{source_path}': {e}")

    if points_in_bbox_gdf.empty:
        logger.info(
            "No bridge features found within the bounding box. Returning empty GeoDataFrame."
        )
        return gpd.GeoDataFrame([], geometry=[], crs=buffered_region_gdf.crs)

    # Clip using Spatial Join ('within')
    logger.info(
        "Performing spatial join to clip points precisely within region boundary..."
    )
    try:
        # Keep only columns from the left side (points)
        clipped_gdf: gpd.GeoDataFrame = gpd.sjoin(
            points_in_bbox_gdf,
            mask_gdf,
            how="inner",
            predicate="within",
            lsuffix="data",  # Use a generic suffix to easily identify and drop mask cols
            rsuffix="mask",
        )
        cols_to_drop = [col for col in clipped_gdf.columns if col.endswith("_mask")]
        clipped_gdf = clipped_gdf.drop(columns=cols_to_drop)
        logger.info(
            f"Retained {len(clipped_gdf)} features within the precise region boundary."
        )

    except Exception as e:
        logger.error(f"Spatial join (clipping) failed. Error: {e}")
        raise RuntimeError(f"Spatial join (clipping) failed: {e}")

    # Optional Filtering
    if filter_out_rail_bridges:
        logger.info(
            f"Filtering railroad bridges using column '{rail_filter_column}'..."
        )
        if rail_filter_column not in clipped_gdf.columns:
            # Log error before raising
            logger.error(
                f"Column '{rail_filter_column}' not found for filtering railroads."
            )
            raise ValueError(
                f"Column '{rail_filter_column}' not found for filtering railroads."
            )

        try:
            is_railroad: pd.Series = (
                clipped_gdf[rail_filter_column]
                .fillna("")
                .astype(str)
                .str.match(rail_pattern, flags=re.IGNORECASE, na=False)
            )
            original_count = len(clipped_gdf)
            clipped_gdf = clipped_gdf[~is_railroad].copy()
            removed_count = original_count - len(clipped_gdf)
            logger.info(f"Removed {removed_count} railroad bridge(s).")
        except Exception as e:
            # Log as warning, don't raise, proceed with unfiltered
            logger.warning(
                f"Failed to filter railroad bridges. Proceeding with potentially unfiltered data. Error: {e}",
                exc_info=False,  # Optionally set exc_info=True for full traceback in log
            )
    # Final Projection
    logger.info(f"Projecting results to final CRS: {buffered_region_gdf.crs}...")
    try:
        final_gdf: gpd.GeoDataFrame = clipped_gdf.to_crs(buffered_region_gdf.crs)
        final_gdf = final_gdf.reset_index(drop=True)
    except Exception as e:
        logger.error(
            f"Failed to project final result to '{buffered_region_gdf.crs}'. Error: {e}"
        )
        raise RuntimeError(f"Failed to project final result: {e}")

    logger.info(f"Processing complete. Returning {len(final_gdf)} bridge features.")
    return final_gdf
