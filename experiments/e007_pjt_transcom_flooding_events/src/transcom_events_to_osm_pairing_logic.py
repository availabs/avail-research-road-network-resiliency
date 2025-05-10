"""
Core logic for pairing TRANSCOM events to OSM roadways, mirroring the
process in the reference notebook and adhering to a 'fail fast' approach
for dependencies.
"""

import logging
from string import Template
from typing import (  # Ensure all necessary types are imported
    Any,
    List,
    Optional,
    Set,
    Tuple,
)

import duckdb
import geopandas as gpd
import numpy as np
import pandas as pd
import pyproj  # For CRS object handling

logger = logging.getLogger(__name__)

# Assume DEFAULT_MAX_DIST_METERS is defined in the scope where the main function is used.
# Example for this module:
DEFAULT_MAX_DIST_METERS = 12.5


def project_and_validate(
    gdf: gpd.GeoDataFrame,
    target_crs: pyproj.CRS | str,  # Made mandatory
) -> gpd.GeoDataFrame:
    """
    Projects a GeoDataFrame to the specified target CRS, asserts units
    are meters, and asserts index remains consistent after projection.

    Args:
        gdf: The GeoDataFrame to project (e.g., roads or events).
        target_crs: The target Coordinate Reference System (CRS) to project to.
                    Must use meter units.

    Returns:
        The projected GeoDataFrame.

    Raises:
        AssertionError: If target CRS units are not meters or index mismatches.
        Exception: If the projection fails for other reasons.
    """
    logger.info(f"Projecting {len(gdf)} features to mandatory CRS: {target_crs}")
    try:
        # Ensure target_crs is a CRS object for validation
        if isinstance(target_crs, str):
            target_crs_obj = pyproj.CRS.from_user_input(target_crs)
        else:
            target_crs_obj = target_crs  # type: ignore[assignment]

        # Assert units are meters before projection
        unit_name = target_crs_obj.axis_info[0].unit_name.lower()  # type: ignore[index, union-attr]
        assert "metre" in unit_name or "meter" in unit_name, (
            f"Target CRS units MUST be meters, found '{unit_name}'"
        )  # type: ignore[literal-required]

        # Perform the projection
        projected_gdf = gdf.to_crs(target_crs_obj)  # type: ignore[misc]

        # Assert index remains consistent (comparing against original gdf)
        assert gdf.index.equals(projected_gdf.index), (
            "Index mismatch between original and projected GeoDataFrame."
        )  # type: ignore[literal-required]

        logger.info("Projection and validation successful.")
        return projected_gdf  # type: ignore[no-any-return]

    except AssertionError as e:
        logger.error(f"Validation Failed: {e}")
        raise
    except Exception as e:
        logger.exception(f"Failed to project GeoDataFrame to {target_crs}.")
        raise e


def _validate_join_inputs(
    road_spans_gdf: gpd.GeoDataFrame,
    events_gdf: gpd.GeoDataFrame,
    expected_road_index_names: List[str],
    required_event_columns: List[str],
) -> pyproj.CRS:
    """
    Validates the input GeoDataFrames for the joining process.

    Args:
        road_spans_gdf: GeoDataFrame for road spans.
        events_gdf: GeoDataFrame for events.
        expected_road_index_names: Expected names for the road spans index levels.
        required_event_columns: List of column names that must exist in events_gdf.

    Returns:
        pyproj.CRS: The common, validated CRS of the input GeoDataFrames.

    Raises:
        ValueError: If CRS, index, or geometry validations fail.
        KeyError: If required columns are missing.
        TypeError: If geometry is not a GeoSeries.
    """
    # --- Road Spans GDF Validations ---
    actual_road_index_names = road_spans_gdf.index.names
    if list(actual_road_index_names) != expected_road_index_names:
        raise ValueError(
            f"Road spans GDF: Expected index names {expected_road_index_names}, "
            f"got {list(actual_road_index_names)}."
        )
    if not road_spans_gdf.index.is_unique:  # As per your provided code
        raise ValueError("Road spans GDF: Index is not unique.")
    if not isinstance(road_spans_gdf.geometry, gpd.GeoSeries):
        raise TypeError("Road spans GDF: Geometry column is not a GeoSeries.")
    if road_spans_gdf.geometry.isna().any():
        raise ValueError(
            "Road spans GDF: Contains None/NaN values in its geometry column."
        )
    if road_spans_gdf.geometry.is_empty.any():
        logger.warning(
            f"'road_spans_laea_gdf' contains {road_spans_gdf.geometry.is_empty.sum()} empty geometries."
        )

    # --- Events GDF Validations ---
    for col in required_event_columns:
        if col not in events_gdf.columns:
            raise KeyError(
                f"Events GDF: Required column '{col}' not found. "
                f"Available columns: {events_gdf.columns.tolist()}"
            )
    if not isinstance(events_gdf.geometry, gpd.GeoSeries):
        raise TypeError("Events GDF: Geometry column is not a GeoSeries.")
    if events_gdf.geometry.isna().any():
        raise ValueError("Events GDF: Contains None/NaN values in its geometry column.")
    if events_gdf.geometry.is_empty.any():
        logger.warning(
            f"'transcom_events_laea_gdf' contains {events_gdf.geometry.is_empty.sum()} empty geometries."
        )

    # --- CRS Validations ---
    crs_roads = road_spans_gdf.crs
    crs_events = events_gdf.crs

    if not (crs_roads and crs_events and crs_roads.equals(crs_events)):
        raise ValueError(
            "Both GeoDataFrames must have a defined and matching Coordinate Reference System. "
            f"Roads CRS: {str(crs_roads)}, Events CRS: {str(crs_events)}"
        )
    common_crs = crs_roads  # Confirmed they are defined and matching

    if not common_crs.is_projected:
        raise ValueError(
            f"The common CRS ('{common_crs.name}') must be a projected CRS (not {common_crs.type_name}) "
            "for metric distance operations."
        )
    expected_units = ("metre", "meter")
    try:
        axis0_unit_ok = common_crs.axis_info[0].unit_name.lower() in expected_units
        axis1_unit_ok = common_crs.axis_info[1].unit_name.lower() in expected_units
        if not (axis0_unit_ok and axis1_unit_ok):
            units_found = (
                f"Axis 0 ('{common_crs.axis_info[0].name}'): '{common_crs.axis_info[0].unit_name}', "
                f"Axis 1 ('{common_crs.axis_info[1].name}'): '{common_crs.axis_info[1].unit_name}'"
            )
            raise ValueError(
                f"The common CRS ('{common_crs.name}') first two axes units must be in meters {expected_units}. "
                f"Found: [{units_found}]"
            )
    except IndexError:
        raise ValueError(
            f"The common CRS ('{common_crs.name}') must have at least two axes defined with metric units "
            f"for 2D distance operations. Found {len(common_crs.axis_info)} axis/axes."
        )
    except AttributeError:  # Catch issues with axis_info or unit_name
        raise ValueError(
            f"Could not determine axis units for CRS ('{common_crs.name}'). "
            "Ensure it's a valid and well-defined projected CRS."
        )

    logger.info(
        f"Input GDFs passed validation. Common CRS: {common_crs.name} (projected, metric units)."
    )
    return common_crs


def _prepare_gdfs_for_sjoin(
    road_spans_gdf: gpd.GeoDataFrame,
    events_gdf: gpd.GeoDataFrame,
    input_crs: pyproj.CRS,  # Passed from validation step
) -> Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """
    Prepares lean GeoDataFrames for an efficient spatial join.

    Selects essential columns (geometry and identifiers for linking back)
    from the input GeoDataFrames and ensures the geometry column is named
    'geometry'.

    Args:
        road_spans_gdf: The original road spans GeoDataFrame, already validated.
        events_gdf: The original events GeoDataFrame, already validated.
        input_crs: The common, validated CRS of the input GeoDataFrames.

    Returns:
        Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
            A tuple containing two new GeoDataFrames:
            - `road_spans_for_join`: Contains 'original_road_span_iloc' and
              geometry from `road_spans_gdf`.
            - `events_for_join`: Contains 'original_event_iloc',
              'transcom_event_id', and geometry from `events_gdf`.
            Both are set to the `input_crs`.
    """
    logger.debug("Preparing GeoDataFrames for spatial join...")

    road_spans_for_join = gpd.GeoDataFrame(
        data={"original_road_span_iloc": np.arange(len(road_spans_gdf))},
        index=road_spans_gdf.index,
        geometry=road_spans_gdf.geometry,
        crs=input_crs,
    )
    if road_spans_for_join.geometry.name != "geometry":
        road_spans_for_join = road_spans_for_join.rename_geometry("geometry")

    events_for_join_data = {
        "original_event_iloc": np.arange(len(events_gdf)),
        "transcom_event_id": events_gdf["event_id"],
    }

    events_for_join = gpd.GeoDataFrame(
        data=events_for_join_data,
        index=events_gdf.index,
        geometry=events_gdf.geometry,
        crs=input_crs,
    )
    if events_for_join.geometry.name != "geometry":
        events_for_join = events_for_join.rename_geometry("geometry")

    return road_spans_for_join, events_for_join


def _perform_spatial_join_and_cleanup(
    road_spans_for_join: gpd.GeoDataFrame,
    events_for_join: gpd.GeoDataFrame,
    max_distance_m: float,
) -> gpd.GeoDataFrame:
    """
    Performs a spatial join (sjoin) with the 'dwithin' predicate and
    drops the 'index_right' column from the result.

    Args:
        road_spans_for_join: Prepared GeoDataFrame for the left side of the join
            (typically road spans).
        events_for_join: Prepared GeoDataFrame for the right side of the join
            (typically events).
        max_distance_m: The maximum distance for the 'dwithin' predicate,
            in the units of the input GeoDataFrames' CRS.

    Returns:
        gpd.GeoDataFrame: The result of the spatial join, with pairs of
            features that are within the specified distance. The 'index_right'
            column is removed.
    """
    logger.debug(f"Performing spatial join with max_distance: {max_distance_m}m...")
    joined_gdf = gpd.sjoin(
        left_df=road_spans_for_join,
        right_df=events_for_join,
        how="inner",
        predicate="dwithin",
        distance=max_distance_m,  # Correct parameter name
    )
    if "index_right" in joined_gdf.columns:
        joined_gdf = joined_gdf.drop(columns=["index_right"])
    elif not joined_gdf.empty:
        logger.debug(
            "Column 'index_right' not found to drop from joined_gdf (this is unusual)."
        )
    return joined_gdf


def _calculate_distances_and_shortest_lines(
    joined_gdf: gpd.GeoDataFrame,  # Contains original_road_span_iloc, original_event_iloc
    original_spans_gdf: gpd.GeoDataFrame,  # The input road_spans_laea_gdf
    original_events_gdf: gpd.GeoDataFrame,  # The input transcom_events_laea_gdf
    input_crs: pyproj.CRS,  # The common LAEA CRS of the inputs
) -> Tuple[pd.Series, gpd.GeoSeries]:
    """
    Calculates direct distances and shortest line geometries for paired features.

    Distances are calculated in the `input_crs` (e.g., LAEA).
    Shortest lines are calculated after reprojecting geometries to EPSG:4326
    for potentially more robust geometric representation, especially for visualization.

    Args:
        joined_gdf_subset: GeoDataFrame containing 'original_road_span_iloc' and
            'original_event_iloc' columns, representing the identified pairs.
        original_spans_gdf: The original, validated road spans GeoDataFrame.
        original_events_gdf: The original, validated events GeoDataFrame.
        input_crs: The common CRS of the original input GeoDataFrames (e.g., LAEA).

    Returns:
        Tuple[pd.Series, gpd.GeoSeries]:
            - distances_series: A pandas Series of calculated distances in the
              units of `input_crs`.
            - shortest_lines_geoseries_4326: A GeoPandas GeoSeries of
              `shapely.LineString` objects representing the shortest lines,
              in EPSG:4326 CRS.
    """
    logger.info("Calculating distances and shortest lines for paired features...")
    if joined_gdf.empty:  # Should be handled before calling this, but as safeguard
        return pd.Series(dtype=float), gpd.GeoSeries([], crs="EPSG:4326")

    road_ilocs = joined_gdf["original_road_span_iloc"].to_numpy()
    event_ilocs = joined_gdf["original_event_iloc"].to_numpy()

    selected_road_geoms_input_crs = original_spans_gdf.geometry.iloc[
        road_ilocs
    ].reset_index(drop=True)
    selected_event_geoms_input_crs = original_events_gdf.geometry.iloc[
        event_ilocs
    ].reset_index(drop=True)

    distances_series = selected_road_geoms_input_crs.distance(
        selected_event_geoms_input_crs, align=False
    )

    # Calculate shortest lines in EPSG:4326 for robust geometry generation
    selected_road_geoms_4326 = selected_road_geoms_input_crs.to_crs("EPSG:4326")
    selected_event_geoms_4326 = selected_event_geoms_input_crs.to_crs("EPSG:4326")
    shortest_lines_geoseries_4326 = selected_road_geoms_4326.shortest_line(
        selected_event_geoms_4326, align=False
    )

    return distances_series, shortest_lines_geoseries_4326


def _set_final_index_and_crs(
    gdf: gpd.GeoDataFrame,  # joined_gdf after distances and shortest lines are added
    input_crs: pyproj.CRS,  # Original common CRS (e.g., LAEA)
    output_crs_param: Optional[Any],
) -> gpd.GeoDataFrame:
    """
    Finalizes the joined GeoDataFrame by setting its geometry to shortest lines
    (assuming they are passed implicitly or already set on gdf_to_finalize
    before calling if this function's role is only index and CRS),
    setting a new compound MultiIndex including 'transcom_event_id',
    verifying index uniqueness, and reprojecting to the specified output CRS.

    Args:
        gdf_to_finalize: The GeoDataFrame to be finalized. It is expected
            to have 'transcom_event_id' as a data column and its geometry
            column should contain the shortest line geometries (typically in
            EPSG:4326 before this function reprojects). Its index is from the
            original road spans.
        input_crs: The original common CRS of the input data (e.g., LAEA),
            used as a fallback if `output_crs_param` is None.
        output_crs_param: The desired output CRS. If None, `input_crs` is used.

    Returns:
        gpd.GeoDataFrame: The finalized GeoDataFrame with updated index,
            geometry, and CRS.

    Raises:
        KeyError: If 'transcom_event_id' column is missing.
        ValueError: If the new compound index is not unique or if reprojection fails.
    """
    logger.debug("Setting final index and reprojecting...")

    # Set new MultiIndex (original road index + transcom_event_id)
    if "transcom_event_id" not in gdf.columns:
        raise KeyError("'transcom_event_id' column missing before setting index.")
    try:
        gdf = gdf.set_index("transcom_event_id", append=True)
    except Exception as e:
        logger.error(f"Error appending 'transcom_event_id' to index: {e}")
        raise
    if not gdf.index.is_unique:
        # ... (logging for non-unique index as before)
        raise ValueError(
            "New combined index (road_span + transcom_event_id) is not unique."
        )
    logger.debug("Compound index is unique.")

    # Reproject to final output_crs
    final_target_crs_str = (
        output_crs_param if output_crs_param is not None else input_crs.to_string()
    )
    try:
        final_target_crs_obj = pyproj.CRS.from_user_input(final_target_crs_str)
        if gdf.crs is None or not gdf.crs.equals(final_target_crs_obj):
            logger.info(
                f"Reprojecting final GDF from {gdf.crs.to_string() if gdf.crs else 'None'} to {final_target_crs_obj.to_string()}"
            )
            gdf = gdf.to_crs(final_target_crs_obj)
        else:
            logger.debug(f"Output GDF already in target CRS: {gdf.crs.to_string()}")
    except pyproj.exceptions.CRSError as e:
        raise ValueError(f"Invalid final target CRS '{final_target_crs_str}': {e}")
    except Exception as e:
        logger.error(f"Failed to reproject to final CRS '{final_target_crs_str}': {e}")
        raise ValueError(f"Failed to reproject. Error: {e}")
    return gdf


def join_road_spans_and_transcom_events(
    road_spans_laea_gdf: gpd.GeoDataFrame,
    transcom_events_laea_gdf: gpd.GeoDataFrame,
    max_distance_m: float = DEFAULT_MAX_DIST_METERS,
    output_crs: Optional[Any] = None,
) -> gpd.GeoDataFrame:
    """
    Finds all pairs of road segments (spans) and TRANSCOM events that are
    spatially within a specified maximum distance of each other.

    The process involves:
    1. Validating input GeoDataFrames (CRS, index structure, required columns).
    2. Preparing lean versions of the GeoDataFrames for efficient joining.
    3. Performing a spatial join ('dwithin') to find qualifying pairs.
    4. For each pair, calculating the metric distance and the shortest line
       geometry (shortest lines are generated in EPSG:4326 for robustness).
    5. Constructing an output GeoDataFrame with these details, indexed by a
       unique compound key (original road span index levels + 'transcom_event_id').
    6. Optionally reprojecting the output to a specified CRS.

    Args:
        road_spans_laea_gdf: Projected roadways GeoDataFrame (LineStrings)
            with units in meters (e.g., LAEA). Expected to have a unique
            MultiIndex with names: ["u", "v", "key", "span_idx"].
            Its geometry column must not contain None/NaN values.
        transcom_events_laea_gdf: Projected events GeoDataFrame (Points)
            with units in meters (e.g., LAEA). Expected to contain an
            'event_id' column. Its geometry column must not contain None/NaN values.
        max_distance_m: Maximum search distance in meters for pairing.
            Defaults to DEFAULT_MAX_DIST_METERS.
        output_crs: Optional. The target Coordinate Reference System for the
            output GeoDataFrame. If None (default), the output CRS
            matches the input CRS (e.g., LAEA, after reprojection from
            the intermediate EPSG:4326 used for shortest lines).
            Accepts any valid input for pyproj.CRS.from_user_input().

    Returns:
        gpd.GeoDataFrame:
            A GeoDataFrame where each row represents a unique pairing between a
            road span and a TRANSCOM event.
            The GeoDataFrame is indexed by a MultiIndex with the names (in order):
            ``["u", "v", "key", "span_idx", "transcom_event_id"]``.
            This combined index is verified to be unique.
            The CRS of the output GeoDataFrame is either the input CRS (e.g., LAEA)
            or the CRS specified by the `output_crs` parameter.

            Key data columns include:
            - 'original_road_span_iloc': Integer location (iloc) of the road span
                                         in the input `road_spans_laea_gdf`.
            - 'original_event_iloc': Integer location (iloc) of the event
                                     in the input `transcom_events_laea_gdf`.
            - '_distance_m_': Calculated direct Euclidean distance in meters between
                              the original road span and event geometries (in their
                              input LAEA CRS).
            Other data columns originally from `transcom_events_laea_gdf` (e.g.,
            'estimated_duration_mins'), if included when preparing data for the join,
            may also be present as pass-through columns.

            The active geometry column contains `shapely.LineString` objects
            representing the shortest line between the paired road span geometry
            and event point geometry.

    Raises:
        ValueError: If input GeoDataFrame validations fail, if the final combined
                  index is not unique, or if reprojection to `output_crs` fails.
        KeyError: If essential columns (like 'event_id') are missing or if columns
                  are unexpectedly missing during processing.
        TypeError: If geometry columns are not of the expected GeoSeries type.
    """
    logger.info("Starting join_road_spans_and_transcom_events process...")

    # Step 0: Validate Inputs
    expected_span_index_names = ["u", "v", "key", "span_idx"]
    required_event_cols = ["event_id"]
    # Add 'estimated_duration_mins' to required_event_cols if it's essential for attributes
    # that THIS function's output directly promises (not for downstream functions unless specified)
    # if 'estimated_duration_mins' in transcom_events_laea_gdf.columns: # or simply add it if mandatory
    #     required_event_cols.append('estimated_duration_mins')

    input_crs = _validate_join_inputs(
        road_spans_gdf=road_spans_laea_gdf,
        events_gdf=transcom_events_laea_gdf,
        expected_road_index_names=expected_span_index_names,
        required_event_columns=required_event_cols,
    )

    # Step 1: Prepare lean GDFs for joining
    road_spans_for_join, events_for_join = _prepare_gdfs_for_sjoin(
        road_spans_laea_gdf, transcom_events_laea_gdf, input_crs
    )

    # Step 2: Perform spatial join and initial cleanup
    joined_gdf = _perform_spatial_join_and_cleanup(
        road_spans_for_join, events_for_join, max_distance_m
    )

    # Step 3: Handle empty join result
    if joined_gdf.empty:
        logger.warning(
            f"No events found within {max_distance_m}m. Returning empty GDF."
        )
        # Define data columns for the empty GDF schema.
        empty_df_data_columns = [
            "original_road_span_iloc",
            "original_event_iloc",
            "_distance_m_",
        ]
        # 'transcom_event_id' will be part of the index name list
        # Add other columns that would have been in events_for_join's data part
        # for col in events_for_join.columns:
        #     if col not in ['geometry', 'original_event_iloc', 'transcom_event_id']: # transcom_event_id is now index part
        #         empty_df_data_columns.append(col)

        target_crs_for_empty = output_crs if output_crs is not None else input_crs
        if output_crs is not None:
            try:
                pyproj.CRS.from_user_input(output_crs)  # Validate for empty case too
            except pyproj.exceptions.CRSError as e:
                raise ValueError(f"Invalid output_crs: {e}")

        final_empty_index_names = expected_span_index_names + ["transcom_event_id"]
        return gpd.GeoDataFrame(
            [],
            columns=empty_df_data_columns,
            geometry=[],
            crs=target_crs_for_empty,
            index=pd.MultiIndex(
                levels=[[]] * len(final_empty_index_names),
                codes=[[]] * len(final_empty_index_names),
                names=final_empty_index_names,
            ),
        )

    # Step 4: Calculate distances and shortest lines
    # These Series/GeoSeries are aligned with joined_gdf's rows (before re-indexing)
    distances_series, shortest_lines_geoseries_4326 = (
        _calculate_distances_and_shortest_lines(
            joined_gdf, road_spans_laea_gdf, transcom_events_laea_gdf, input_crs
        )
    )

    # Step 5: Add calculated attributes, set final geometry, index, and output CRS
    # Temporarily, joined_gdf still has its original road_span index from sjoin
    # And 'transcom_event_id' is a column

    # Add distances
    joined_gdf["_distance_m_"] = distances_series.values

    # Update geometry to shortest lines and set its CRS (EPSG:4326)
    if len(shortest_lines_geoseries_4326) == len(joined_gdf):
        joined_gdf["geometry"] = shortest_lines_geoseries_4326.values
        joined_gdf = joined_gdf.set_crs(
            shortest_lines_geoseries_4326.crs, allow_override=True
        )
    elif not joined_gdf.empty:
        raise ValueError("Length mismatch for shortest_line assignment.")

    if not joined_gdf.empty and joined_gdf.geometry.name != "geometry":
        joined_gdf = joined_gdf.rename_geometry("geometry")

    # At this point, joined_gdf has road_span index, attributes from events_for_join (incl. transcom_event_id col),
    # _distance_m_, and shortest_line geometry in EPSG:4326.

    final_gdf = _set_final_index_and_crs(
        joined_gdf.copy(),  # Pass a copy to avoid SettingWithCopyWarning from chained operations
        input_crs,
        output_crs,
    )

    logger.info(f"Successfully joined spans and events. Output rows: {len(final_gdf)}")
    return final_gdf


def identify_paired_transcom_events(
    transcom_events_gdf: gpd.GeoDataFrame,
    join_gdf: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """Marks TRANSCOM events based on whether they were successfully paired.

    This function checks which events from `transcom_events_gdf` have their
    'transcom_event_id' present in the index (as a named level) of `join_gdf`.
    It adds a boolean column '_was_paired_' to a copy of `transcom_events_gdf`.

    Args:
        transcom_events_gdf (gpd.GeoDataFrame): The original GeoDataFrame
            containing all TRANSCOM events. Must include an 'event_id' column.
        join_gdf (gpd.GeoDataFrame): A GeoDataFrame resulting from a previous
            pairing operation (e.g., output of
            `join_road_spans_and_transcom_events`). Its index must contain a
            level named 'transcom_event_id'.

    Returns:
        gpd.GeoDataFrame: A copy of `transcom_events_gdf` with an added
            boolean column '_was_paired_'. True if the event's 'event_id'
            was found as a 'transcom_event_id' in the index of `join_gdf`.

    Raises:
        KeyError: If 'event_id' column is not in `transcom_events_gdf`,
                or if 'transcom_event_id' is not a named level in the
                index of `join_gdf`.
    """
    # --- Validate existence of the event ID column in the primary events GDF ---
    if "event_id" not in transcom_events_gdf.columns:
        raise KeyError("'event_id' column not found in the input transcom_events_gdf.")

    # --- Validate existence of the transcom_event_id column in the join_gdf ---
    if "transcom_event_id" not in join_gdf.index.names:
        raise KeyError(
            "'transcom_event_id' level not found in the input join_gdf index."
        )

    # --- Step 1: Create a set of all unique event IDs that were actually paired ---
    # Assumes join_gdf["transcom_event_id"] contains the IDs of events that were successfully paired
    paired_event_ids_set: Set[str] = set(
        join_gdf.index.get_level_values(level="transcom_event_id").unique()
    )

    # --- Step 2: Create a copy of the input events GeoDataFrame ---
    # Ensures the original DataFrame is not modified.
    transcom_events_copy = transcom_events_gdf.copy()

    # --- Step 3: Add the '_was_paired_' boolean column ---
    # Use the vectorized .isin() method for efficient checking against the set.
    transcom_events_copy["_was_paired_"] = transcom_events_copy["event_id"].isin(
        paired_event_ids_set
    )

    # --- Step 4: Return the modified copy ---
    return transcom_events_copy


def extract_unpaired_transcom_events(
    transcom_events_gdf: gpd.GeoDataFrame,
    join_gdf: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """Extracts TRANSCOM events that were not part of a previous pairing.

    This function uses `identify_paired_transcom_events` to determine which
    events were paired (by checking for their ID in the 'transcom_event_id'
    index level of `join_gdf`). It then filters to return only unpaired events.
    The index of the resulting GeoDataFrame is reset.

    Args:
        transcom_events_gdf (gpd.GeoDataFrame): The original GeoDataFrame
            containing all TRANSCOM events. Must include an 'event_id' column.
        join_gdf (gpd.GeoDataFrame): A GeoDataFrame from a previous pairing.
            `identify_paired_transcom_events` expects its index to contain
            a level named 'transcom_event_id'.

    Returns:
        gpd.GeoDataFrame: A new GeoDataFrame containing only those TRASCOM events
            from `transcom_events_gdf` that were determined to be unpaired.
            The index is reset (note: `reset_index()` without `drop=True`
            will turn the old index into columns).

    Raises:
        KeyError: Propagated from `identify_paired_transcom_events`.
    """
    transcom_events_with_pairing_status = identify_paired_transcom_events(
        transcom_events_gdf=transcom_events_gdf,
        join_gdf=join_gdf,
    )

    unpaired_events_gdf = transcom_events_with_pairing_status[
        ~transcom_events_with_pairing_status["_was_paired_"]
    ].reset_index()

    return unpaired_events_gdf


def aggregate_roadway_flooding_event_data(
    transcom_events_gdf: gpd.GeoDataFrame,
    join_gdf: gpd.GeoDataFrame,
) -> pd.DataFrame:
    """
    Aggregates TRANSCOM event data for road segments, calculating total
    affected durations and event details for various categories by handling
    overlapping time intervals using DuckDB.

    This function processes event data linked to specific road segments (identified
    by u, v, key). It uses a templated SQL query executed in DuckDB to:
    1. Prepare a 'collected' table joining road-event pairs with detailed event
       attributes, calculating event end times and boolean flags for categories
       (e.g., road closed, flooding, road repairs) based on event properties.
    2. Apply a gaps-and-islands algorithm to merge overlapping event time
       intervals for each road segment and for each defined event category.
    3. For each road segment and category, it calculates the total duration
       of impact (in hours), a list of unique contributing TRANSCOM event IDs,
       and a count of the distinct merged event periods.
    4. The final output is a Pandas DataFrame summarizing these metrics for each
       road segment across all defined event categories.

    Args:
        transcom_events_gdf (gpd.GeoDataFrame): GeoDataFrame containing detailed
            TRANSCOM event information. Expected to include columns like 'event_id',
            'start_date_time', 'estimated_duration_mins' (or 'event_interval'),
            'lanes_detail', 'lanes_status', 'nysdot_sub_category', and 'description'
            for filtering and calculating event end times. Geometry is typically
            dropped before processing in DuckDB.
        join_gdf (gpd.GeoDataFrame): GeoDataFrame representing the initial pairings
            between road segments and TRANSCOM events. It must provide the road
            segment identifiers (expected as columns 'u', 'v', 'key' after potential
            reset_index) and the 'transcom_event_id' for each pair.
            Other columns like '_distance_m_' are expected if validated.
            Geometry is typically dropped before processing in DuckDB.

    Returns:
        pd.DataFrame: A Pandas DataFrame indexed by the road segment identifiers
            (u, v, key). Columns include:
            - 'all_events_ids': Sorted list of unique TRANSCOM event IDs for all
              relevant events affecting the road segment.
            - 'all_intervals_count': Count of distinct merged time periods for
              all relevant events.
            - 'all_events_total_duration_hours': Total duration in hours road
              segment was affected by any relevant event, overlaps resolved.
            - Similar columns for specific categories (e.g., 'road_closed_events_ids',
              'road_closed_intervals_count', 'road_closed_total_duration_hours',
              and for 'road_flooded' and 'road_repairs').

    Raises:
        ValueError: If `join_gdf` is missing required columns (u, v, key,
            transcom_event_id, _distance_m_).
            (Also, if `roads_gdf` validation were active and failed for its index names).
        KeyError: If `transcom_events_gdf` is missing 'event_id' or
            'estimated_duration_mins'.
        duckdb.Error: If any DuckDB query execution fails.
    """

    required_join_gdf_cols = set(["u", "v", "key", "transcom_event_id", "_distance_m_"])
    actual_join_gdf_cols = set(list(join_gdf.index.names) + list(join_gdf.columns))

    missing_join_gdf_cols = required_join_gdf_cols - actual_join_gdf_cols
    if missing_join_gdf_cols:
        raise ValueError(
            f"join_gdf is missing the following required columns: {missing_join_gdf_cols}."
        )

    # Fail fast if essential columns are missing (User Preference)
    if "event_id" not in transcom_events_gdf.columns:
        raise KeyError("Required column 'event_id' missing from events GDF.")
    if "estimated_duration_mins" not in transcom_events_gdf.columns:
        raise KeyError(
            "Required column 'estimated_duration_mins' missing from events GDF."
        )

    events_df = transcom_events_gdf.drop(columns="geometry").reset_index()
    events_df["estimated_duration_mins"] = pd.to_numeric(
        events_df["estimated_duration_mins"], errors="coerce"
    )

    join_df = join_gdf.drop(columns="geometry").reset_index()

    # SEE: SQL Gaps and Islands
    sql_template = Template("""
      CREATE TABLE $_output_table_name_
        AS
          WITH RoadEventIntervals AS (
              SELECT DISTINCT
                  "u",
                  "v",
                  "key",
                  "transcom_event_id",
                  "start_date_time",
                  "end_date_time"
              FROM (
                  SELECT
                      u,
                      v,
                      key,

                      transcom_event_id,

                      start_date_time,
                      end_date_time

                  FROM collected
                  WHERE $_events_where_clause_
              ) AS t

          ),
          OrderedIntervals AS (
              SELECT
                  *,
                  -- Find the maximum end time encountered so far within this road key partition
                  MAX("end_date_time") OVER (
                      PARTITION BY "u", "v", "key"
                      ORDER BY "start_date_time", "end_date_time"
                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                  ) AS running_max_end
              FROM RoadEventIntervals
          ),
          LaggedMaxEnd AS (
              SELECT
                  *,
                  -- Get the max end time from the *previous* row in the ordered partition
                  LAG(running_max_end, 1, '1970-01-01'::TIMESTAMP) OVER ( -- Use a very early default for first row
                      PARTITION BY "u", "v", "key"
                      ORDER BY "start_date_time", "end_date_time"
                  ) AS max_end_of_previous_grouping
              FROM OrderedIntervals
          ),
          IslandStarts AS (
              -- An interval starts a new island if its start time is after the max end time of the previous block
              SELECT
                  *,
                  CASE
                      WHEN "start_date_time" > max_end_of_previous_grouping THEN 1
                      ELSE 0
                  END AS is_island_start
              FROM LaggedMaxEnd
          ),
          IslandGroups AS (
              -- Assign a cumulative sum to identify intervals belonging to the same island
              SELECT
                  *,
                  SUM(is_island_start) OVER (
                      PARTITION BY "u", "v", "key"
                      ORDER BY "start_date_time", "end_date_time"
                      ROWS UNBOUNDED PRECEDING
                  ) AS island_id
              FROM IslandStarts
          ),
          MergedIntervals AS (
              -- Find the min start and max end for each identified island
              SELECT
                  "u", "v", "key", -- Road key columns
                  list(transcom_event_id) AS transcom_event_ids,
                  island_id,
                  MIN("start_date_time") AS merged_start,
                  MAX("end_date_time") AS merged_end
              FROM IslandGroups
              GROUP BY "u", "v", "key", island_id -- Group by road key AND island id
          ),
          MergedDurations AS (
              -- Calculate the duration of each merged interval
              SELECT
                  "u",
                  "v",
                  "key",
                  transcom_event_ids,
                  (merged_end - merged_start) AS duration  -- DuckDB can subtract timestamps to get INTERVAL
              FROM MergedIntervals
              WHERE merged_end > merged_start -- Ensure positive duration
          )
          -- Final aggregation: Sum the durations of merged intervals per road key
            SELECT
                "u",
                "v",
                "key",
                list_sort(
                  flatten(
                    list(transcom_event_ids)
                  )
                ) AS transcom_event_ids,
                COUNT(1) AS num_distinct_flood_intervals,
                ROUND(
                  SUM(epoch(duration)) / 3600.0,
                  2
                ) AS total_duration_hours
              FROM MergedDurations
              GROUP BY "u", "v", "key";
    """)

    # Execute query in DuckDB
    try:
        with duckdb.connect(database=":memory:", read_only=False) as con:
            # Register DataFrames as virtual tables
            con.register("events", events_df)
            con.register("road_spans_to_events", join_df)

            con.execute("""
              CREATE TABLE collected
                AS
                  SELECT
                      a.u,
                      a.v,
                      a.key,

                      a.transcom_event_id,

                      c.start_date_time::TIMESTAMP AS start_date_time,
                      (
                        c.start_date_time::TIMESTAMP
                        +
                        INTERVAL (COALESCE(
                          c.event_interval,
                          c.estimated_duration_mins::TEXT || 'MINUTES'
                        ))
                      ) AS end_date_time,

                        (
                            ( c.lanes_detail ILIKE '%ALL LANES%' )
                            AND
                            (
                                ( c.lanes_status ILIKE '%CLOSED%' )
                                OR
                                ( c.lanes_status ILIKE '%BLOCKED%' )
                            )
                      ) AS event_closed_road,

                    (UPPER(c.nysdot_sub_category) = 'WEATHER HAZARD')                  AS road_flooded,
                    (UPPER(c.nysdot_sub_category) IN ('CONSTRUCTION', 'MAINTENANCE'))  AS road_repairs

                  FROM road_spans_to_events AS a
                      INNER JOIN events AS c
                        ON ( a.transcom_event_id = c.event_id )

                  WHERE (
                    ( UPPER(c.nysdot_sub_category) = 'WEATHER HAZARD' )
                    OR
                    (
                      ( UPPER(c.nysdot_sub_category) IN ( 'CONSTRUCTION', 'MAINTENANCE' ) )
                      AND
                      (
                        regexp_matches(
                          -- Believe it or not, Washout Road in Glenville, NY gets a lot of routine maintenance
                          regexp_replace(
                            c.description,
                           '(washout rd)|(washout road)',
                           '',
                           'i'
                          ),
                          'flood|washout',
                          'i'
                        )
                      )
                    )
                  )
            """)

            con.execute(
                sql_template.substitute(
                    _output_table_name_="all_events_aggregated",
                    _events_where_clause_="( TRUE )",
                )
            )

            con.execute(
                sql_template.substitute(
                    _output_table_name_="road_closed_events_aggregated",
                    _events_where_clause_="( event_closed_road )",
                )
            )

            con.execute(
                sql_template.substitute(
                    _output_table_name_="flooding_events_aggregated",
                    _events_where_clause_="( road_flooded )",
                )
            )

            con.execute(
                sql_template.substitute(
                    _output_table_name_="road_repairs_events_aggregated",
                    _events_where_clause_="( road_repairs )",
                )
            )

            result_df = con.query("""
                SELECT
                    u,
                    v,
                    key,

                    a.transcom_event_ids                         AS all_events_ids,
                    a.num_distinct_flood_intervals               AS all_incidents_count,
                    a.total_duration_hours                       AS all_events_total_duration_hours,

                    COALESCE(b.transcom_event_ids, [])           AS road_closed_events_ids,
                    COALESCE(b.num_distinct_flood_intervals, 0)  AS road_closed_incidents_count,
                    COALESCE(b.total_duration_hours, 0)          AS road_closed_total_duration_hours,

                    COALESCE(c.transcom_event_ids, [])           AS road_flooded_events_ids,
                    COALESCE(c.num_distinct_flood_intervals, 0)  AS road_flooded_incidents_count,
                    COALESCE(c.total_duration_hours, 0)          AS road_flooded_events_total_duration_hours,

                    COALESCE(d.transcom_event_ids, [])           AS road_repairs_events_ids,
                    COALESCE(d.num_distinct_flood_intervals, 0)  AS road_repairs_incidents_count,
                    COALESCE(d.total_duration_hours, 0)          AS road_repairs_events_total_duration_hours

                  FROM all_events_aggregated AS a
                    LEFT OUTER JOIN road_closed_events_aggregated AS b
                      USING (u, v, key)
                    LEFT OUTER JOIN flooding_events_aggregated AS c
                      USING (u, v, key)
                    LEFT OUTER JOIN road_repairs_events_aggregated AS d
                      USING (u, v, key)
                  ORDER BY all_events_total_duration_hours DESC
            """).to_df()

            result_df.set_index(keys=["u", "v", "key"], inplace=True)

            return result_df

    except Exception as e:
        logger.error(f"DuckDB query failed: {e}")
        raise  # Re-raise the exception after logging
