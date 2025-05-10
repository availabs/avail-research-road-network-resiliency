"""
Prefect workflow to pair TRANSCOM events with nearby OSM roadway segments
within a specified distance. Calculates a dynamic LAEA CRS based on the
buffered OSM region boundary for consistent spatial operations.
"""

import argparse
import logging
import os
import shutil
import stat
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import geopandas as gpd
import pandas as pd
from prefect import flow, get_run_logger, task
from pyproj import CRS  # Keep CRS for type hinting

# --- Required Imports ---
import experiments.e007_pjt_transcom_flooding_events.src.transcom_events_to_osm_pairing_logic as pairing_logic
from common.us_census.tiger.utils import get_laea_crs_for_region
from tasks.osm import (
    create_bridge_spans_gdf_task,
    create_nonbridge_spans_gdf_task,
    enrich_osm_task,
)
from tasks.transcom import get_clipped_transcom_events_data_task

experiment_root = Path(__file__).parent.parent.resolve()
experiment_data_dir = experiment_root / "data"


# --- Prefect Tasks ---
# Note: Task function signatures use descriptive names, but the flow
# will use variables matching the notebook naming conventions.


@task(name="Get LAEA CRS from Buffer")
def get_laea_crs_task(
    buffered_region_gdf: gpd.GeoDataFrame,  #
) -> CRS:
    """
    Calculates a local Lambert Azimuthal Equal-Area (LAEA) CRS.

    The LAEA CRS is centered on the centroid of the provided
    `buffered_region_gdf`. This ensures an appropriate projection
    for distance-based calculations within the region.

    Args:
        buffered_region_gdf: A GeoDataFrame representing the buffered
                             boundary of the study region.

    Returns:
        pyproj.CRS: The calculated LAEA CRS as a pyproj.CRS object
    """
    logger = logging.getLogger(__name__)
    logger.info("Calculating LAEA CRS from buffered region (original notebook method).")

    laea_crs = get_laea_crs_for_region(region_gdf=buffered_region_gdf)

    return laea_crs


@task(name="Project Data")
def project_data_task(
    gdf: gpd.GeoDataFrame,  #
    target_crs: CRS | str,
) -> gpd.GeoDataFrame:
    """
    Projects an input GeoDataFrame to a specified target CRS.

    This task utilizes the `pairing_logic.project_and_validate` function,
    which also asserts that the target CRS units are in meters and that
    the GeoDataFrame's index remains consistent after projection.

    Args:
        gdf: The GeoDataFrame to be projected (e.g., road spans or events).
        target_crs: The target Coordinate Reference System (CRS) to project to.
                    Expected to be a CRS with meter units.

    Returns:
        gpd.GeoDataFrame: The GeoDataFrame projected to the `target_crs`.
    """

    # Returns 'roads_laea_gdf' or 'transcom_events_laea_gdf'
    return pairing_logic.project_and_validate(
        gdf=gdf,  #
        target_crs=target_crs,
    )


@task(
    name="Find Nearest Events for Roads",
)
def join_road_spans_and_transcom_events_task(
    road_spans_laea_gdf: gpd.GeoDataFrame,
    transcom_events_laea_gdf: gpd.GeoDataFrame,
    max_distance_m: float,
    output_crs: Optional[Any] = "EPSG:4326",
) -> gpd.GeoDataFrame:
    """
    Spatially joins road spans with TRANSCOM events within a given distance.

    This task calls `pairing_logic.join_road_spans_and_transcom_events`
    to find all event-road span pairs where the event is within
    `max_distance_m` of the road span. The output includes calculated
    distances and shortest line geometries between pairs.

    Args:
        road_spans_laea_gdf: GeoDataFrame of road spans, projected to a
            metric CRS (e.g., LAEA).
        transcom_events_laea_gdf: GeoDataFrame of TRANSCOM events, projected
            to the same metric CRS.
        max_distance_m: Maximum distance in meters for pairing events to
            road spans.
        output_crs: Optional target CRS for the output GeoDataFrame. Defaults
            to "EPSG:4326". If None, uses the input CRS.

    Returns:
        gpd.GeoDataFrame: A GeoDataFrame of paired road spans and events,
            indexed by (road_span_index_levels, transcom_event_id).
            Includes distance and shortest line geometry.
    """

    join_gdf = pairing_logic.join_road_spans_and_transcom_events(
        road_spans_laea_gdf=road_spans_laea_gdf,
        transcom_events_laea_gdf=transcom_events_laea_gdf,
        max_distance_m=max_distance_m,
        output_crs=output_crs,
    )

    return join_gdf


@task(
    name="Extract Unpaired TRANSCOM Events",
)
def extract_unpaired_transcom_events_task(
    transcom_events_gdf: gpd.GeoDataFrame,
    join_gdf: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """
    Filters TRANSCOM events to return only those not present in the join_gdf.

    This task relies on `pairing_logic.extract_unpaired_transcom_events`,
    which typically uses `join_gdf` (containing paired events) to identify
    and exclude events from the original `transcom_events_gdf` that were
    successfully paired.

    Args:
        transcom_events_gdf: The original GeoDataFrame of all TRANSCOM events.
        join_gdf: A GeoDataFrame representing events that were successfully
            paired with road spans. Expected to have 'transcom_event_id'
            as a named level in its index, and a '_road_span_type_' column
            (as per the `create_qa_transcom_events_gdf_task` which creates a
            similar structure, or `union_join_geodataframes_task` output).
            The `pairing_logic.identify_paired_transcom_events` called by
            `pairing_logic.extract_unpaired_transcom_events` has specific
            expectations for `join_gdf`'s structure (e.g., `transcom_event_id`
            as the 5th index level).

    Returns:
        gpd.GeoDataFrame: A GeoDataFrame containing only the TRANSCOM events
            that were not found in the `join_gdf`.
    """

    unpaired_transcom_events_gdf = pairing_logic.extract_unpaired_transcom_events(
        transcom_events_gdf=transcom_events_gdf,
        join_gdf=join_gdf,
    )

    return unpaired_transcom_events_gdf


@task(name="Concatenate JOIN GeoDataFrames")
def union_join_geodataframes_task(
    nonbridge_join_gdf: gpd.GeoDataFrame,
    bridge_join_gdf: gpd.GeoDataFrame,
) -> Dict[str, gpd.GeoDataFrame]:
    """
    Concatenates GeoDataFrames of non-bridge and bridge road span pairings
    and adds a '_road_span_type_' column.

    This task takes two GeoDataFrames, presumably one for event pairings with
    non-bridge spans and one for pairings with bridge spans. It combines them
    into a single GeoDataFrame. It also adds a new column, '_road_span_type_',
    to distinguish between 'NONBRIDGE_SPAN' and 'BRIDGE_SPAN' based on which
    input GeoDataFrame the row originated from (inferred from the index).
    It raises an error if the indices of the input GeoDataFrames overlap.

    Args:
        nonbridge_join_gdf: GeoDataFrame containing event pairings for
            non-bridge road spans.
        bridge_join_gdf: GeoDataFrame containing event pairings for
            bridge road spans.

    Returns:
        gpd.GeoDataFrame: A unified GeoDataFrame containing all pairings,
            with an added '_road_span_type_' column indicating whether each
            pairing pertains to a 'NONBRIDGE_SPAN' or 'BRIDGE_SPAN'.

    Raises:
        ValueError: If the indices of `nonbridge_join_gdf` and
            `bridge_join_gdf` are not disjoint.
    """
    if len(nonbridge_join_gdf.index.intersection(bridge_join_gdf.index)) > 0:
        raise ValueError(
            "nonbridge_join_gdf.index and bridge_join_gdf.index MUST be disjoint."
        )

    join_gdfs = [nonbridge_join_gdf, bridge_join_gdf]

    nonempty_gdfs = [d for d in join_gdfs if not (d is None or d.empty)]

    union_gdf = pd.concat(
        objs=nonempty_gdfs,  #
        copy=True,
    )

    union_gdf["_road_span_type_"] = union_gdf.index.map(
        lambda loc: "BRIDGE_SPAN" if loc in bridge_join_gdf.index else "NONBRIDGE_SPAN"
    )

    return union_gdf


@task(name="Create Pairings")
def aggregate_roadway_flooding_event_data_task(
    transcom_events_gdf: gpd.GeoDataFrame,
    join_gdf: Tuple,
) -> gpd.GeoDataFrame:
    """
    Aggregates TRANSCOM event data for road segments using DuckDB.

    This task calls `pairing_logic.aggregate_roadway_flooding_event_data`
    to calculate total affected durations, lists of event IDs, and counts of
    distinct event periods for various categories (all, road closed, flooding,
    road repairs), handling overlapping time intervals.

    Args:
        transcom_events_gdf: GeoDataFrame of original TRANSCOM event details.
        join_gdf: GeoDataFrame containing the joined road segment and event
            data (e.g., the output of `union_join_geodataframes_task`).
            This GDF provides the link between road segments (u,v,key) and
            TRANSCOM event IDs, along with their attributes.

    Returns:
        pd.DataFrame: A Pandas DataFrame indexed by road segment (u,v,key),
            with columns summarizing event impacts for different categories.
    """

    road_events_data_agg_df = pairing_logic.aggregate_roadway_flooding_event_data(
        transcom_events_gdf=transcom_events_gdf,
        join_gdf=join_gdf,
    )

    return road_events_data_agg_df


@task(name="Identify Unpaired Data")
def create_qa_transcom_events_gdf_task(
    transcom_events_gdf: gpd.GeoDataFrame,
    union_join_gdf: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """
    Augments TRANSCOM events with a '_paired_with_' status for QA.

    This task analyzes the `union_join_gdf` (which contains all pairings
    of events to road spans, distinguishing between bridge and non-bridge spans)
    to determine how each original TRANSCOM event was paired.

    A new column, '_paired_with_', is added to a copy of `transcom_events_gdf`.
    Possible values for '_paired_with_':
    - 'NONBRIDGE_SPAN': Event paired only with non-bridge road spans.
    - 'BRIDGE_SPAN': Event paired only with bridge road spans.
    - 'BOTH': Event paired with both non-bridge and bridge road spans.
    - None (or NaN): Event was not found in `union_join_gdf` (unpaired).

    Args:
        transcom_events_gdf: The original GeoDataFrame of all TRANSCOM events.
            Must contain an 'event_id' column.
        union_join_gdf: A GeoDataFrame containing all pairings between events
            and road spans. It must have 'transcom_event_id' as its 5th
            index level name (index.names[4]) and a data column named
            '_road_span_type_' (with values like 'NONBRIDGE_SPAN', 'BRIDGE_SPAN').

    Returns:
        gpd.GeoDataFrame: A copy of `transcom_events_gdf` with the added
            '_paired_with_' column indicating its pairing status/category.

    Raises:
        KeyError: If 'event_id' is missing from `transcom_events_gdf`, or if
            'transcom_event_id' is not the 5th index level / '_road_span_type_'
            column is missing in `union_join_gdf`.
    """

    # --- Validate existence of the event ID column in the primary events GDF ---
    if "event_id" not in transcom_events_gdf.columns:
        raise KeyError("'event_id' column not found in the input transcom_events_gdf.")

    # --- Validate existence of the transcom_event_id index level in the join_gdf ---
    if "transcom_event_id" not in union_join_gdf.index.names[4]:
        raise KeyError(
            "'transcom_event_id' is expected to be the 5th index key in input join_gdf."
        )

    # --- Validate existence of the transcom_event_id column in the join_gdf ---
    if "_road_span_type_" not in union_join_gdf.columns:
        raise KeyError("'_road_span_type_' column not found in the input join_gdf.")

    # --- Create a subset DataFrame with just the transcom_event_id and _road_span_type_ columns.
    pairings_df = union_join_gdf.reset_index(level="transcom_event_id")[
        ["transcom_event_id", "_road_span_type_"]
    ]
    pairings_df = pairings_df.set_index("transcom_event_id")

    # --- Group on transcom_event_id, value is set of _road_span_type_ values.
    grouped_road_types_set = (
        pairings_df.groupby(pairings_df.index)["_road_span_type_"].unique().apply(set)
    )

    def determine_match(transcom_event_id):
        """If single _road_span_type in match, return that. Otherwise None or BOTH, accordingly."""

        if transcom_event_id not in grouped_road_types_set.index:
            return None

        uniq_values = grouped_road_types_set[transcom_event_id]

        if len(uniq_values) == 0:
            return None
        elif len(uniq_values) == 1:
            return uniq_values.pop()
        else:
            return "BOTH"

    transcom_events_copy_gdf = transcom_events_gdf.copy()

    transcom_events_copy_gdf["_paired_with_"] = transcom_events_gdf["event_id"].apply(
        determine_match
    )

    # --- Step 4: Return the modified copy ---
    return transcom_events_copy_gdf


@task(name="Save Main Output")
def save_main_output_task(
    roads_gdf: gpd.GeoDataFrame,  # Use correct name
    road_events_data_agg_df: pd.DataFrame,
    output_gpkg: Path,
    output_layer_name: str,
) -> Tuple[Path, str]:
    """
    Merges road geometries with aggregated event data and saves to a GeoPackage.

    This task takes the original road geometries, merges them with the
    aggregated event data (based on a common road segment index), and saves
    the resulting GeoDataFrame as a layer in the specified GeoPackage file.
    The output file is set to read-only permissions.

    Args:
        roads_gdf: GeoDataFrame containing road segment geometries, indexed
            by road segment identifiers (e.g., u, v, key). Only geometry is used.
        road_events_data_agg_df: Pandas DataFrame containing aggregated event
            data, indexed by the same road segment identifiers as `roads_gdf`.
        output_gpkg: Path to the output GeoPackage file.
        output_layer_name: Name for the layer within the GeoPackage.

    Returns:
        Tuple[Path, str]: A tuple containing the path to the saved
            GeoPackage file and the layer name.

    Raises:
        Exception: If saving to GeoPackage fails.
    """
    logger = logging.getLogger(__name__)

    logger.info(f"Saving main output to: {output_gpkg} (Layer: {output_layer_name})")

    try:
        output_gpkg.parent.mkdir(parents=True, exist_ok=True)
        output_gpkg.unlink(missing_ok=True)

        roads_columns_to_drop = [
            col for col in roads_gdf.columns if col != roads_gdf.geometry.name
        ]

        road_geoms_gdf = roads_gdf.drop(columns=roads_columns_to_drop)

        result_gdf = pd.merge(
            left=road_geoms_gdf,
            right=road_events_data_agg_df,
            left_index=True,
            right_index=True,
            how="inner",
        )

        result_gdf.to_file(
            filename=output_gpkg,  #
            layer=output_layer_name,
            driver="GPKG",
            engine="pyogrio",
        )

        read_only_perms = stat.S_IREAD | stat.S_IRGRP | stat.S_IROTH
        os.chmod(output_gpkg, read_only_perms)

        logger.info("Main output saved successfully.")

        return output_gpkg, output_layer_name
    except Exception as e:
        logger.error(f"Failed to save main output GPKG: {e}")
        raise


@task(name="Save QA Output")
def save_qa_output_task(
    nonbridge_spans_gdf: gpd.GeoDataFrame,
    bridge_spans_gdf: gpd.GeoDataFrame,
    qa_transcom_events_gdf: gpd.GeoDataFrame,
    union_join_gdf: gpd.GeoDataFrame,
    qa_output_gpkg: Path,
) -> Path:
    """
    Saves multiple Quality Assurance (QA) related GeoDataFrames to separate
    layers within a single GeoPackage file.

    The saved layers include:
    - Road span/event pairings (`union_join_gdf`)
    - Non-bridge road spans
    - Bridge road spans
    - TRANSCOM events augmented with pairing status (`qa_transcom_events_gdf`)

    The output GeoPackage file is set to read-only permissions.

    Args:
        nonbridge_spans_gdf: GeoDataFrame of non-bridge road spans.
        bridge_spans_gdf: GeoDataFrame of bridge road spans.
        qa_transcom_events_gdf: GeoDataFrame of TRANSCOM events with QA pairing
            information (e.g., '_paired_with_' column).
        union_join_gdf: GeoDataFrame of all road span to event pairings.
        qa_output_gpkg: Path to the output QA GeoPackage file.

    Returns:
        Path: The path to the saved QA GeoPackage file.

    Raises:
        Exception: If saving any layer to the GeoPackage fails.
    """

    logger = logging.getLogger(__name__)
    logger.info(f"Saving QA output to: {qa_output_gpkg}")

    try:
        qa_output_gpkg.parent.mkdir(parents=True, exist_ok=True)
        qa_output_gpkg.unlink(missing_ok=True)

        logger.info(
            f"QA: Saving layer: road_span_and_event_pairs ({len(union_join_gdf)} features)"
        )

        union_join_gdf.to_file(  # Use correct name
            filename=qa_output_gpkg,
            layer="road_span_and_event_pairs",
            driver="GPKG",
            engine="pyogrio",
        )

        logger.info(
            f"QA: Saving layer: nonbridge_spans ({len(nonbridge_spans_gdf)} features)"
        )

        nonbridge_spans_gdf.to_file(  # Use correct name
            filename=qa_output_gpkg,
            layer="nonbridge_spans",
            driver="GPKG",
            engine="pyogrio",
        )

        logger.info(
            f"QA: Saving layer: bridge_spans ({len(bridge_spans_gdf)} features)"
        )

        bridge_spans_gdf.to_file(  # Use correct name
            filename=qa_output_gpkg,
            layer="bridge_spans",
            driver="GPKG",
            engine="pyogrio",
        )

        logger.info(
            f"Saving layer: transcom_flood_events "
            f"({len(qa_transcom_events_gdf)} features)"
        )

        qa_transcom_events_gdf.to_file(  # Use correct name
            filename=qa_output_gpkg,
            layer="transcom_flood_events",
            driver="GPKG",
            engine="pyogrio",
        )

        read_only_perms = stat.S_IREAD | stat.S_IRGRP | stat.S_IROTH
        os.chmod(qa_output_gpkg, read_only_perms)

        logger.info("QA output saved successfully.")
    except Exception as e:
        logger.error(f"Failed to save QA output GPKG: {e}")
        raise
    return qa_output_gpkg


@flow(name="Pair TRANSCOM Events to OSM Roadways")
def join_roadways_and_transcom_events_flow(
    osm_pbf: str,
    transcom_events_gpkg: str,
    max_distance_m: float = pairing_logic.DEFAULT_MAX_DIST_METERS,
    clean: bool = False,
    verbose: bool = False,
):
    """
    Orchestrates the end-to-end workflow for pairing TRANSCOM events with
    OSM roadway segments (differentiating bridge and non-bridge spans).

    The workflow includes:
    1. Enriching OSM data to get road networks and region boundaries.
    2. Calculating a local LAEA CRS for consistent spatial operations.
    3. Loading and clipping TRANSCOM event data to the region.
    4. Projecting road spans (bridge/non-bridge) and events to LAEA CRS.
    5. Spatially joining events to non-bridge spans, then remaining events to bridge spans.
    6. Unifying these pairings and adding road span type information.
    7. Aggregating event data (counts, durations, event ID lists) per road segment.
    8. Performing QA by categorizing how each event was paired.
    9. Saving main aggregated output and detailed QA layers to GeoPackages.

    Uses Prefect tasks and futures for managing dependencies and execution.
    Variable names within the flow aim to be consistent with a source notebook.

    Args:
        osm_pbf (str): Path to the input OpenStreetMap PBF file.
        transcom_events_gpkg (str): Path to the input TRANSCOM events GeoPackage file.
        max_distance_m (float, optional): Maximum distance in meters for pairing
            events to road spans. Defaults to 25.0 meters.
        clean (bool, optional): If True, removes the existing output directory for
            the region before running the workflow. Defaults to False.
        verbose (bool, optional): If True, sets logging to DEBUG level for more
            detailed output. Defaults to False.

    Returns:
        Tuple[Path, str]: A tuple containing the path to the main output
            GeoPackage file and the name of the primary data layer within it.
    """
    logger = get_run_logger()
    log_level = logging.DEBUG if verbose else logging.INFO
    logger.setLevel(log_level)
    logging.getLogger().setLevel(log_level)

    logger.info("--- Starting TRANSCOM Flooding Events to OSM pairing workflow ---")

    logger.info(f"OSM Input: {osm_pbf}")
    logger.info(f"TRANSCOM Events Input: {transcom_events_gpkg}")
    logger.info(f"Clean Run: {clean}")
    logger.info(f"Verbose Logging: {verbose}")

    # 1. Enrich OSM data
    enriched_osm = enrich_osm_task(osm_pbf=osm_pbf)
    region_name = enriched_osm["region_name"]
    roads_gdf = enriched_osm["edges_gdf"]
    buffered_region_gdf = enriched_osm["buffered_region_gdf"]

    output_dir = experiment_data_dir / region_name
    output_gpkg = output_dir / f"e007-pjt-transcom-flooding-events.{region_name}.gpkg"
    output_layer_name = "aggregated_transcom_flood_events_data"

    qa_output_gpkg = (
        output_dir / f"qa-e007-pjt-transcom-flooding-events.{region_name}.gpkg"
    )

    if clean and output_dir.exists():
        logger.warning(
            f"Clean flag is True. Removing existing output directory: {output_dir}"
        )
        try:
            shutil.rmtree(output_dir)
            logger.info(f"Successfully removed directory: {output_dir}")
        except OSError as e:
            logger.error(f"Failed to remove directory {output_dir}: {e}", exc_info=True)
            # Decide if this is a fatal error or if we can proceed
            # For now, re-raise to stop the flow if cleanup fails
            raise RuntimeError(f"Failed to clean output directory {output_dir}") from e

        # Re-create the directory after removing it, ensuring it exists for saving later
        output_dir.mkdir(
            parents=True, exist_ok=False
        )  # exist_ok=False to ensure it was removed

    if not clean and output_gpkg.is_file():
        logger.info(f"Output file already exists: {output_gpkg}")
        logger.info(
            "Clean flag is False. Skipping analysis and returning existing path."
        )
        return str(output_gpkg), output_layer_name

    elif not clean and not output_gpkg.is_file():
        logger.info("Output file does not exist. Proceeding with analysis.")

    # 2-1. Get the nonbridge road spans -> nonbridge_spans_gdf_future
    nonbridge_spans_gdf_future = create_nonbridge_spans_gdf_task.submit(
        edges_gdf=roads_gdf
    )

    # 2-2. Get the bridge road spans -> bridge_spans_gdf_future
    bridge_spans_gdf_future = create_bridge_spans_gdf_task(
        edges_gdf=roads_gdf  #
    )

    # 3. Load TRANSCOM data -> transcom_events_gdf_future
    transcom_events_gdf_future = get_clipped_transcom_events_data_task(
        transcom_events_gpkg=transcom_events_gpkg,
        buffered_region_gdf=buffered_region_gdf,
    )

    # 4. Calculate Target CRS -> laea_crs_future
    laea_crs_future = get_laea_crs_task.submit(
        buffered_region_gdf=buffered_region_gdf,
    )

    # 5-1. Project nonbridge road spans to LAEA -> nonbridge_spans_laea_gdf_future
    nonbridge_spans_laea_gdf_future = project_data_task.submit(
        gdf=nonbridge_spans_gdf_future,
        target_crs=laea_crs_future,
    )

    # 5-2. Project bridge road spans to LAEA -> bridge_spans_laea_gdf_future
    bridge_spans_laea_gdf_future = project_data_task.submit(
        gdf=bridge_spans_gdf_future,
        target_crs=laea_crs_future,
    )

    # 6. Project events -> transcom_events_laea_gdf_future
    transcom_events_laea_gdf_future = project_data_task.submit(
        gdf=transcom_events_gdf_future,
        target_crs=laea_crs_future,
    )

    # 7. Find nearest pairs -> roads_to_transcom_event_pairs_future
    nonbridge_join_gdf_future = join_road_spans_and_transcom_events_task.submit(
        road_spans_laea_gdf=nonbridge_spans_laea_gdf_future,  # Pass future
        transcom_events_laea_gdf=transcom_events_laea_gdf_future,  # Pass future
        max_distance_m=max_distance_m,
    )

    # 8. Extract the as yet unpaired events -> unpaired_transcom_events_laea_gdf_future
    unpaired_transcom_events_laea_gdf_future = (
        extract_unpaired_transcom_events_task.submit(
            transcom_events_gdf=transcom_events_laea_gdf_future,
            join_gdf=nonbridge_join_gdf_future,
        )
    )

    # 9. Join the bridge spans with the as yet unpaired events -> bridge_join_gdf_future
    bridge_join_gdf_future = join_road_spans_and_transcom_events_task.submit(
        road_spans_laea_gdf=bridge_spans_laea_gdf_future,  # Pass future
        transcom_events_laea_gdf=unpaired_transcom_events_laea_gdf_future,  # Pass future
        max_distance_m=max_distance_m,
    )

    # 10. UNION the Nonbridge JOINs and Bridge JOINs -> union_join_gdf_future
    #     NOTE: Will add _road_span_type_ attribute to the GeoDataFrame features.
    union_join_gdf_future = union_join_geodataframes_task.submit(
        nonbridge_join_gdf=nonbridge_join_gdf_future,
        bridge_join_gdf=bridge_join_gdf_future,
    )

    # 11. Aggregate the UNIONed JOINs by RoadWay -> road_events_data_agg_df_future
    road_events_data_agg_df_future = aggregate_roadway_flooding_event_data_task.submit(
        transcom_events_gdf=transcom_events_gdf_future,
        join_gdf=union_join_gdf_future,
    )

    # 12. Create the QA Transcom Events GeoDataFrame -> qa_transcom_events_gdf_future
    #     NOTE: Will add _paired_with_ (NONBRIDGE, BRIDGE, BOTH, NONE) to the Transcom Event features.
    qa_transcom_events_gdf_future = create_qa_transcom_events_gdf_task.submit(
        transcom_events_gdf=transcom_events_gdf_future,  # Pass future
        union_join_gdf=union_join_gdf_future,
    )

    # 13. Save main output
    main_output_gpkg_future = save_main_output_task.submit(
        roads_gdf=roads_gdf,  # Pass future
        road_events_data_agg_df=road_events_data_agg_df_future,  # Pass future
        output_gpkg=output_gpkg,  # Pass future
        output_layer_name=output_layer_name,
    )

    # 14. Save QA output
    qa_output_gpkg_future = save_qa_output_task.submit(
        nonbridge_spans_gdf=nonbridge_spans_gdf_future,
        bridge_spans_gdf=bridge_spans_gdf_future,
        qa_transcom_events_gdf=qa_transcom_events_gdf_future,  # Pass result
        union_join_gdf=union_join_gdf_future,
        qa_output_gpkg=qa_output_gpkg,
    )

    # # Optionally wait for final tasks if needed, but flow will wait implicitly
    # main_output_path = main_output_path_future.result()  # type: ignore[misc]
    # qa_output_path = qa_output_path_future.result()  # type: ignore[misc]

    main_output_gpkg_future.result()
    qa_output_gpkg_future.result()

    logger.info(f"Workflow complete. Main output: {output_gpkg}")
    logger.info(f"Workflow complete. QA output: {qa_output_gpkg}")

    return output_gpkg, output_layer_name


# --- Script Execution ---
if __name__ == "__main__":
    # --- Add argparse logic ---
    parser = argparse.ArgumentParser(
        description="Run the Prefect flow to pair TRANSCOM events to OSM roadways."
    )
    parser.add_argument(
        "--osm-pbf",
        required=True,
        help="Path to the input OpenStreetMap PBF file.",
    )
    parser.add_argument(
        "--transcom-events-gpkg",
        required=True,
        help="Path to the input TRANSCOM events GeoPackage file.",
    )
    parser.add_argument(
        "--max-distance-m",
        type=float,
        default=pairing_logic.DEFAULT_MAX_DIST_METERS,
        help=f"Maximum distance (meters) for pairing (default: {pairing_logic.DEFAULT_MAX_DIST_METERS}).",
    )
    parser.add_argument(
        "--clean",
        action="store_true",  # Use action='store_true' for boolean flags
        help="If set, delete the region's output directory before starting.",
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose (DEBUG level) logging for the workflow.",
    )

    args = parser.parse_args()

    # --- Call the flow with parsed arguments ---
    join_roadways_and_transcom_events_flow(
        osm_pbf=args.osm_pbf,
        transcom_events_gpkg=args.transcom_events_gpkg,
        max_distance_m=args.max_distance_m,
        clean=args.clean,
    )
