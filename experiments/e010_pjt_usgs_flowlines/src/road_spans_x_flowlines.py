import logging
from dataclasses import dataclass
from typing import Callable, List, Tuple  # Keep List and Tuple

import geopandas as gpd

logger = logging.getLogger(__name__)

# --- Constants for Conventions ---
# Name for the index level created when exploding multi-part geometries
DEFAULT_PART_INDEX_NAME = "intxn_idx"


def _ensure_compatible_crs(
    gdf_left: gpd.GeoDataFrame,
    gdf_right: gpd.GeoDataFrame,
    gdf_right_name: str = "gdf_right",
) -> gpd.GeoDataFrame:
    """
    Ensures gdf_right is in the same CRS as gdf_left.
    Returns the (potentially reprojected) gdf_right. Logs warnings if CRS is None.
    """
    if gdf_left.crs is None or gdf_right.crs is None:
        logger.warning(
            f"CRS for '{gdf_left.geometry.name}' or '{gdf_right_name}' is not set. "
            "Assuming they are compatible. Results may be incorrect if CRSs differ."
        )
        return gdf_right
    if gdf_left.crs != gdf_right.crs:
        logger.info(
            f"Reprojecting '{gdf_right_name}' from CRS '{gdf_right.crs}' to '{gdf_left.crs}'..."
        )
        gdf_right = gdf_right.to_crs(gdf_left.crs)
    return gdf_right


@dataclass
class PreparedGeoDataFrames:
    road_spans_overlay_gdf: gpd.GeoDataFrame
    flowlines_overlay_gdf: gpd.GeoDataFrame
    road_span_original_index_names: List[str]
    flowline_original_index_names: List[str]


def _prepare_geodataframes_for_overlay(
    road_spans_gdf: gpd.GeoDataFrame,  #
    nhd_flowlines_gdf: gpd.GeoDataFrame,
) -> PreparedGeoDataFrames:
    """
    Prepares input GeoDataFrames for the overlay operation.
    - road_spans_gdf: Keeps only its index (as columns) and geometry.
    - nhd_flowlines_gdf: Keeps its index (as columns) and all data columns.
    Returns prepared GDFs and their original index names.
    """
    logger.debug("Preparing GeoDataFrames for overlay.")

    # Prepare road_spans_gdf: keep only index (as columns) + geometry
    road_span_original_index_names = list(road_spans_gdf.index.names)

    cols_to_drop_from_roads = [
        c
        for c in road_spans_gdf.columns
        if c not in [road_spans_gdf.geometry.name, "_road_span_type_"]
    ]

    road_spans_overlay_gdf = road_spans_gdf.drop(
        columns=cols_to_drop_from_roads
    ).reset_index()

    # Prepare nhd_flowlines_gdf: keep index (as columns) + all data columns
    flowline_original_index_names = list(nhd_flowlines_gdf.index.names)

    # Handle unnamed single index for nhd_flowlines_gdf
    if flowline_original_index_names == [None] and nhd_flowlines_gdf.index.nlevels == 1:
        nhd_flowlines_gdf.index.name = "flowline_index_level_0"  # Ensure it has a name
        flowline_original_index_names = [nhd_flowlines_gdf.index.name]

    flowlines_overlay_gdf = nhd_flowlines_gdf.reset_index()

    return PreparedGeoDataFrames(
        road_spans_overlay_gdf=road_spans_overlay_gdf,
        flowlines_overlay_gdf=flowlines_overlay_gdf,
        road_span_original_index_names=road_span_original_index_names,
        flowline_original_index_names=flowline_original_index_names,
    )


def _perform_geometric_intersection(
    prepared_geodataframes: PreparedGeoDataFrames,
) -> gpd.GeoDataFrame:
    """
    Performs the geometric intersection overlay.
    """
    logger.info("Performing geometric overlay (intersection)...")

    intersections_gdf = gpd.overlay(
        df1=prepared_geodataframes.road_spans_overlay_gdf,
        df2=prepared_geodataframes.flowlines_overlay_gdf,
        how="intersection",
        keep_geom_type=False,
        make_valid=True,
    )

    logger.info(
        f"Overlay resulted in {len(intersections_gdf)} initial intersection geometries."
    )

    return intersections_gdf


def _set_initial_composite_index(
    intersections_gdf: gpd.GeoDataFrame,
    prepared_geodataframes: PreparedGeoDataFrames,
) -> gpd.GeoDataFrame:
    """
    Sets the initial composite index from original GDF identifiers.
    """
    if intersections_gdf.empty:
        return intersections_gdf  # No index to set

    logger.debug("Setting initial composite index.")
    initial_composite_index_levels = (
        prepared_geodataframes.road_span_original_index_names
        + prepared_geodataframes.flowline_original_index_names
    )

    missing_cols = [
        col
        for col in initial_composite_index_levels
        if col not in intersections_gdf.columns
    ]

    if missing_cols:
        raise ValueError(
            f"Columns for initial composite index missing: {missing_cols}. "
            f"Available: {intersections_gdf.columns.tolist()}"
        )

    gdf_indexed = intersections_gdf.set_index(initial_composite_index_levels)

    return gdf_indexed.sort_index()


def _explode_multi_geometries(
    gdf: gpd.GeoDataFrame,  #
    part_index_name: str = DEFAULT_PART_INDEX_NAME,
) -> gpd.GeoDataFrame:
    """
    Explodes multi-part geometries and adds a part index level.
    """
    if gdf.empty:
        return gdf

    logger.info(
        f"Exploding multi-part geometries and adding '{part_index_name}' to index..."
    )

    exploded_gdf = gdf.explode(index_parts=True)

    logger.info(f"Exploded to {len(exploded_gdf)} individual geometries.")

    # Rename the new index level
    original_index_names = list(gdf.index.names)

    exploded_gdf.index.names = original_index_names + [part_index_name]

    return exploded_gdf


def _convert_linestrings_to_midpoints(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Converts LineString geometries in the GeoDataFrame to their midpoints.
    """
    if gdf.empty:
        return gdf

    logger.debug("Converting LineString intersection parts to their midpoints...")

    is_line = gdf.geom_type == "LineString"

    if is_line.any():
        num_lines_to_convert = is_line.sum()

        logger.info(f"Found {num_lines_to_convert} LineString(s) to convert to Points.")

        gdf.loc[is_line, "geometry"] = gdf.loc[is_line, "geometry"].apply(
            lambda geom: geom.interpolate(0.5, normalized=True)
        )
    else:
        logger.info(
            "No LineString geometries found after exploding; no interpolation needed."
        )
    return gdf


# --- Strategy Dataclass Definition ---
@dataclass
class IntersectionStrategy:
    """
    A strategy object holding callable tasks for the intersection process.
    """

    ensure_compatible_crs: Callable = _ensure_compatible_crs
    prepare_geodataframes_for_overlay: Callable = _prepare_geodataframes_for_overlay
    perform_geometric_intersection: Callable = _perform_geometric_intersection
    set_initial_composite_index: Callable = _set_initial_composite_index
    explode_multi_geometries: Callable = _explode_multi_geometries
    convert_linestrings_to_midpoints: Callable = _convert_linestrings_to_midpoints


INTERSECTION_DEFAULT_STRATEGY = IntersectionStrategy()


def process_road_flowline_intersections(
    road_spans_gdf: gpd.GeoDataFrame,  #
    nhd_flowlines_gdf: gpd.GeoDataFrame,
    strategy: IntersectionStrategy = INTERSECTION_DEFAULT_STRATEGY,
) -> gpd.GeoDataFrame:
    """
    Calculates and processes intersection points between road spans and NHD flowlines.

    This orchestrating function follows established conventions:
    - Uses existing index names from input GDFs.
    - Drops non-index/non-geometry columns from `road_spans_gdf` before overlay.
    - Names the intersection part index level 'intxn_idx'.

    Args:
        road_spans_gdf: GeoDataFrame of road spans with its index correctly set.
        nhd_flowlines_gdf: GeoDataFrame of NHD flowlines with its index correctly set.
        strategy: An IntersectionStrategy object containing the callable tasks.

    Returns:
        A GeoDataFrame with Point geometries representing intersections,
        indexed by a composite of road span identifiers, flowline identifiers,
        and the intersection part index ('intxn_idx').
    """
    logger.info("Starting road span and NHD flowline intersection processing.")

    if road_spans_gdf.empty or nhd_flowlines_gdf.empty:
        logger.warning(
            "One or both input GeoDataFrames are empty. Returning an empty GeoDataFrame."
        )
        # Attempt to return an empty GDF with an indicative schema if possible,
        # otherwise, a minimal empty GDF.
        # For now, return minimal. Schema could be derived if needed.
        return gpd.GeoDataFrame(
            columns=["geometry"],
            geometry="geometry",
            crs=road_spans_gdf.crs or nhd_flowlines_gdf.crs,
        )

    # 0. Ensure CRSs are compatible
    nhd_flowlines_gdf = strategy.ensure_compatible_crs(
        gdf_left=road_spans_gdf,
        gdf_right=nhd_flowlines_gdf,
        gdf_right_name="NHD flowlines",
    )

    # 1. Prepare inputs for overlay
    prepared_geodataframes = strategy.prepare_geodataframes_for_overlay(
        road_spans_gdf=road_spans_gdf,  #
        nhd_flowlines_gdf=nhd_flowlines_gdf,
    )

    # 2. Perform geometric intersection
    intersections_gdf = strategy.perform_geometric_intersection(
        prepared_geodataframes=prepared_geodataframes
    )

    if intersections_gdf.empty:
        logger.info("No intersections found after overlay.")
        return intersections_gdf  # Return empty GDF (it has columns from overlay)

    # 3. Set initial composite index (e.g., 6-level)
    indexed_gdf = strategy.set_initial_composite_index(
        intersections_gdf=intersections_gdf,
        prepared_geodataframes=prepared_geodataframes,
    )

    # 4. Explode multi-geometries and add part index (e.g., 'intxn_idx')
    exploded_gdf = strategy.explode_multi_geometries(
        gdf=indexed_gdf,  #
        part_index_name=DEFAULT_PART_INDEX_NAME,
    )

    # 5. Convert any resulting LineStrings to their midpoints
    points_gdf = strategy.convert_linestrings_to_midpoints(exploded_gdf)

    # 6. Final sort for consistency
    final_gdf = points_gdf.sort_index()

    logger.info(
        f"Intersection processing complete. Returning {len(final_gdf)} Point records."
    )
    # Optional: Final check for index uniqueness could be added here if it's a strict requirement
    # if not final_gdf.index.is_unique:
    # logger.warning("The final composite index of intersection points is not unique.")
    return final_gdf
