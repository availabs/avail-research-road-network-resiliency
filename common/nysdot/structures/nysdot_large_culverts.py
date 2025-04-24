import logging
import pathlib
from typing import Any, Dict, Optional, Union

import geopandas as gpd
import pyogrio

# --- Logger Setup ---
# Configure logging in the calling script/orchestrator (e.g., Prefect flow, worker script)
# Example configuration:
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_clipped_large_culvert_data(
    gis_source: Union[str, pathlib.Path],
    buffered_region_gdf: gpd.GeoDataFrame,
    layer_name: Optional[str] = None,
    sjoin_predicate: str = "intersects",
) -> gpd.GeoDataFrame:
    # --- Input Validation ---
    # (Error raising logic remains the same, no logging needed for these)
    if buffered_region_gdf.empty:
        raise ValueError("Input 'buffered_region_gdf' cannot be empty.")
    # ... (other validations for CRS, file existence, rail_filter_column) ...
    source_path = pathlib.Path(gis_source)  # Validate path earlier maybe
    if not source_path.exists():
        raise FileNotFoundError(f"GIS dataset source not found at: {source_path}")
    if buffered_region_gdf.crs is None:
        raise ValueError("Input 'buffered_region_gdf' must have a CRS defined.")

    # --- Processing Steps ---

    logger.debug(f"Processing GIS dataset from: {source_path}")
    if layer_name:
        logger.debug(f"Using layer: {layer_name}")

    # Get Data CRS and Prepare Mask
    try:
        source_info: Dict[str, Any] = pyogrio.read_info(
            source_path, layer=layer_name, encoding="utf-8"
        )
        source_crs = source_info["crs"]
        if source_crs is None:
            raise ValueError("Could not determine CRS from data source.")
        logger.debug(f"Source data CRS detected: {source_crs}")

        logger.debug("Projecting region mask to match data source CRS...")
        mask_gdf: gpd.GeoDataFrame = buffered_region_gdf.to_crs(source_crs)
        logger.debug("Region mask projected successfully.")

    except Exception as e:
        logger.error(f"Failed during CRS handling or mask projection. Error: {e}")
        raise RuntimeError(f"Failed during CRS handling or mask projection: {e}")

    # Read Data using BBox Filter
    logger.debug("Reading large_culverts features using bounding box filter...")
    try:
        points_in_bbox_gdf: gpd.GeoDataFrame = gpd.read_file(
            source_path,
            engine="pyogrio",
            layer=layer_name,
            bbox=tuple(mask_gdf.total_bounds),
            encoding="utf-8",
        )
        logger.debug(f"Read {len(points_in_bbox_gdf)} features within bounding box.")
    except Exception as e:
        logger.error(f"Failed to read data file '{source_path}'. Error: {e}")
        raise RuntimeError(f"Failed to read data file '{source_path}': {e}")

    if points_in_bbox_gdf.empty:
        logger.debug("No found within the bounding box. Returning empty GeoDataFrame.")
        return gpd.GeoDataFrame([], geometry=[], crs=buffered_region_gdf.crs)

    # Clip using Spatial Join ('within')
    logger.debug(
        "Performing spatial join to clip points precisely within region boundary..."
    )
    try:
        # Keep only columns from the left side (points)
        clipped_gdf: gpd.GeoDataFrame = gpd.sjoin(
            points_in_bbox_gdf,
            mask_gdf,
            how="inner",
            predicate=sjoin_predicate,
            lsuffix="data",  # Use a generic suffix to easily identify and drop mask cols
            rsuffix="mask",
        )
        cols_to_drop = [col for col in clipped_gdf.columns if col.endswith("_mask")]
        clipped_gdf = clipped_gdf.drop(columns=cols_to_drop)
        logger.debug(
            f"Retained {len(clipped_gdf)} features within the precise region boundary."
        )

    except Exception as e:
        logger.error(f"Spatial join (clipping) failed. Error: {e}")
        raise RuntimeError(f"Spatial join (clipping) failed: {e}")

    # Final Projection
    logger.debug(f"Projecting results to final CRS: {buffered_region_gdf.crs}...")
    try:
        final_gdf: gpd.GeoDataFrame = clipped_gdf.to_crs(buffered_region_gdf.crs)
        final_gdf = final_gdf.reset_index(drop=True)
    except Exception as e:
        logger.error(
            f"Failed to project final result to '{buffered_region_gdf.crs}'. Error: {e}"
        )
        raise RuntimeError(f"Failed to project final result: {e}")

    logger.debug(
        f"Processing complete. Returning {len(final_gdf)} large_culverts features."
    )
    return final_gdf
