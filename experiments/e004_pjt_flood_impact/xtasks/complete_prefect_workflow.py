# filename: xtasks/prefect_workflow.py
import argparse
import logging
import os
import shutil
import stat
import sys
from collections.abc import Mapping  # Import Mapping
from os import PathLike
from pathlib import Path
from typing import Dict, Tuple

# Third-party imports
import geopandas as gpd
import networkx as nx  # Import networkx
import pandas as pd
from prefect import flow, get_run_logger, task

# --- Import project-specific modules ---
# Import refactored functions and necessary types/classes from core_logic
from experiments.e004_pjt_flood_impact.src.core_logic import (
    DEFAULT_QUADRAT_WIDTH,
    FloodingImpactInfo,
    InitialImpactData,
    IntegrityHashKey,
    add_impact_columns_to_gdf,
    calculate_highest_road_risk_for_nonbridge_spans,
    identify_impacted_edges,
    initialize_flood_impact_data,
    perform_spatial_join_with_floodplains,
)
from tasks.fema.floodplains import get_clipped_floodplain_data_task
from tasks.osm import enrich_osm_task

experiment_root = Path(__file__).parent.parent.resolve()
experiment_data_dir = experiment_root / "data"

# --- Task Definitions ---


# Task wrapping the refactored initialization function
@task(name="Initialize Flood Impact Data")
def initialize_flood_impact_data_task(
    osmnx_simplified_g: nx.MultiDiGraph,
    floodplains_gdf: gpd.GeoDataFrame,
) -> InitialImpactData:
    """
    Prefect task wrapper for core_logic.initialize_flood_impact_data.
    Initializes GDFs and graph, calculates initial integrity hashes, and clears caches.

    Args:
        osmnx_simplified_g: The simplified osmnx MultiDiGraph for the region.
        floodplains_gdf: GeoDataFrame of floodplain polygons including the
                         '_flood_risk_level_' column.

    Returns:
        An InitialImpactData dataclass instance containing the initial data
        and a mapping of their integrity hashes.
    """
    logger = get_run_logger()
    logger.info("Task: Initializing flood impact data...")
    try:
        initial_data = initialize_flood_impact_data(
            osmnx_simplified_g=osmnx_simplified_g,
            floodplains_gdf=floodplains_gdf,
        )
        logger.info("Task: Flood impact data initialized successfully.")

        return initial_data
    except Exception as e:
        logger.error(
            f"Task Error: Failed during flood impact data initialization: {e}",
            exc_info=True,
        )
        raise


# Task wrapping the refactored spatial join function
@task(name="Perform Spatial Join (Floodplains)")
def perform_spatial_join_task(
    nonbridge_spans_gdf: gpd.GeoDataFrame,
    floodplains_gdf: gpd.GeoDataFrame,
    integrity_hashes: Mapping[IntegrityHashKey, int],
    quadrat_width: float = DEFAULT_QUADRAT_WIDTH,
) -> Tuple[gpd.GeoDataFrame, int]:  # Return mandatory hash
    """
    Prefect task wrapper for core_logic.perform_spatial_join_with_floodplains.
    Performs spatial join and returns the resulting GDF and its hash.
    Verifies input integrity using hashes from the provided mapping.

    Args:
        nonbridge_spans_gdf: GeoDataFrame of non-bridge spans.
        floodplains_gdf: GeoDataFrame of floodplain polygons.
        integrity_hashes: Mapping containing expected integrity hashes for inputs.
        quadrat_width: Width for subdividing the spatial index.

    Returns:
        A tuple containing:
            - join_gdf: Result of the spatial join.
            - join_gdf_integrity_hash: Hash of the resulting join GDF's index.
    """
    logger = get_run_logger()
    logger.info(
        "Task: Performing spatial join between non-bridge spans and floodplains..."
    )
    try:
        # Pass integrity_hashes directly; core logic function expects Optional but here we know it's provided
        join_gdf, join_gdf_integrity_hash_opt = perform_spatial_join_with_floodplains(
            nonbridge_spans_gdf=nonbridge_spans_gdf,
            floodplains_gdf=floodplains_gdf,
            quadrat_width=quadrat_width,
            integrity_hashes=integrity_hashes,
        )

        join_gdf_integrity_hash = join_gdf_integrity_hash_opt

        logger.info(
            f"Task: Spatial join complete. Found {len(join_gdf)} intersections."
        )

        return join_gdf, join_gdf_integrity_hash
    except Exception as e:
        logger.error(f"Task Error: Failed during spatial join: {e}", exc_info=True)
        raise


# Task wrapping the refactored highest risk calculation function
@task(name="Calculate Highest Road Risk")
def calculate_highest_road_risk_for_nonbridge_spans_task(
    join_gdf: gpd.GeoDataFrame,
    integrity_hashes: Mapping[IntegrityHashKey, int],
) -> pd.Series:
    """
    Prefect task wrapper for core_logic.calculate_highest_road_risk.
    Calculates the highest flood risk for each original non-bridge road span.
    Verifies input integrity using the hash from the provided mapping.

    Args:
        join_gdf: GDF from the spatial join step.
        integrity_hashes: Mapping containing expected integrity hashes (must include JOIN_GDF).

    Returns:
        A pandas Series mapping original non-bridge span int_loc to its highest risk level.
    """
    logger = get_run_logger()
    logger.info("Task: Calculating highest flood risk for non-bridge road spans...")
    try:
        highest_risk_series = calculate_highest_road_risk_for_nonbridge_spans(
            nonbridge_spans_floodplains_join_gdf=join_gdf,
            integrity_hashes=integrity_hashes,
        )
        logger.info(
            f"Task: Highest risk calculation complete for {len(highest_risk_series)} spans."
        )
        return highest_risk_series
    except Exception as e:
        logger.error(
            f"Task Error: Failed calculating highest road risk: {e}", exc_info=True
        )
        raise


# Task wrapping the refactored impacted edges identification function
@task(name="Identify Impacted Edges")
def identify_impacted_edges_task(
    osmnx_simplified_g: nx.MultiDiGraph,
    nonbridge_spans_gdf: gpd.GeoDataFrame,
    bridge_spans_gdf: gpd.GeoDataFrame,
    highest_risk_series: pd.Series,
    integrity_hashes: Mapping[IntegrityHashKey, int],
) -> Tuple[
    Dict[Tuple[int, int, int], FloodingImpactInfo], Dict[Tuple[int, int, int], int]
]:
    """
    Prefect task wrapper for core_logic.identify_impacted_edges.
    Identifies nonfunctional and isolated edges based on flood risk.
    Verifies input integrity using hashes from the provided mapping.

    Args:
        osmnx_simplified_g: The simplified osmnx MultiDiGraph.
        nonbridge_spans_gdf: GeoDataFrame of non-bridge spans.
        bridge_spans_gdf: GeoDataFrame of bridge spans.
        highest_risk_series: Series mapping non-bridge span int_loc to risk level.
        integrity_hashes: Mapping containing expected integrity hashes for inputs.

    Returns:
        A tuple containing the nonfunctional_edges and isolated_edges dictionaries.
    """
    logger = get_run_logger()
    logger.info("Task: Identifying nonfunctional and isolated edges...")
    try:
        nonfunctional_edges, isolated_edges = identify_impacted_edges(
            osmnx_simplified_g=osmnx_simplified_g,
            nonbridge_spans_gdf=nonbridge_spans_gdf,
            bridge_spans_gdf=bridge_spans_gdf,
            nonbridge_spans_int_loc_to_highest_flood_risk=highest_risk_series,
            integrity_hashes=integrity_hashes,
        )

        logger.info(
            f"Task: Identified {len(nonfunctional_edges)} nonfunctional and {len(isolated_edges)} isolated edges."
        )

        return nonfunctional_edges, isolated_edges
    except Exception as e:
        logger.error(
            f"Task Error: Failed identifying impacted edges: {e}", exc_info=True
        )
        raise


# Task wrapping the refactored impact column addition function
@task(name="Add Impact Columns to GDF")
def add_impact_columns_task(
    roads_gdf: gpd.GeoDataFrame,
    nonfunctional_edges: Dict[Tuple[int, int, int], FloodingImpactInfo],
    isolated_edges: Dict[Tuple[int, int, int], int],
    # Integrity hashes are now mandatory for the task wrapper
    integrity_hashes: Mapping[IntegrityHashKey, int],
) -> Tuple[gpd.GeoDataFrame, int]:  # Return mandatory hash
    """
    Prefect task wrapper for core_logic.add_impact_columns_to_gdf.
    Adds columns detailing flood impacts to the main roads GeoDataFrame.
    Verifies input integrity using the hash from the provided mapping.

    Args:
        roads_gdf: The original GeoDataFrame of all road edges.
        nonfunctional_edges: Dict mapping nonfunctional edges to impact info.
        isolated_edges: Dict mapping isolated edges to risk level.
        integrity_hashes: Mapping containing expected integrity hash for roads_gdf.

    Returns:
        A tuple containing:
            - final_impact_gdf: The GeoDataFrame with added impact columns.
            - final_impact_gdf_integrity_hash: Hash of the final GDF index.
    """
    logger = get_run_logger()

    logger.info("Task: Adding impact columns to final GeoDataFrame...")
    try:
        final_gdf, final_gdf_hash_opt = add_impact_columns_to_gdf(
            roads_gdf=roads_gdf,
            nonfunctional_edges_to_reason_and_risk=nonfunctional_edges,
            isolated_edges_to_risk_level=isolated_edges,
            integrity_hashes=integrity_hashes,
        )

        final_gdf_hash = final_gdf_hash_opt

        logger.info("Task: Impact columns added successfully.")
        return final_gdf, final_gdf_hash
    except Exception as e:
        logger.error(f"Task Error: Failed adding impact columns: {e}", exc_info=True)
        raise


# Task for saving output to read-only GPKG
@task(name="Save Flood Impact GDF")
def save_flood_impact_gdf_task(
    gdf: gpd.GeoDataFrame,  #
    output_path: PathLike,
) -> str:
    """
    Saves the final flood impact GeoDataFrame to a GeoPackage file using pyogrio,
    then sets the file permissions to read-only.
    The output path follows the pattern: data/<region_name>/e004_pjt_flood_impact_analysis.<region_name>.gpkg

    Args:
        gdf: The final GeoDataFrame with flood impact information.
        region_name: The name of the region (used for the directory and filename).
        base_output_dir: The root directory for output data (defaults to 'data').

    Returns:
        The absolute path to the saved GeoPackage file as a string.
    """
    logger = get_run_logger()
    output_fpath = Path(output_path).resolve()  # Ensure it's a resolved Path object

    try:
        logger.info(f"Task: Saving final flood impact GeoDataFrame to: {output_fpath}")
        output_fpath.parent.mkdir(parents=True, exist_ok=True)

        # Write the GeoPackage file
        gdf.to_file(
            filename=output_fpath,  #
            layer="flood_impact",
            driver="GPKG",
            engine="pyogrio",
        )
        logger.info(f"Task: Successfully saved GeoDataFrame to {output_fpath}")

        # Set file to read-only
        # Permissions: Read for owner, group, and others. No write/execute.
        read_only_perms = stat.S_IREAD | stat.S_IRGRP | stat.S_IROTH
        os.chmod(output_fpath, read_only_perms)
        logger.info(f"Task: Set file permissions to read-only for {output_fpath}")

        return str(output_fpath)

    except Exception as e:
        logger.error(
            f"Task Error: Failed during save or permission setting for {output_fpath}: {e}",
            exc_info=True,
        )
        # Attempt cleanup if file exists but failed during write/chmod
        if output_fpath is not None and output_fpath.exists():
            logger.warning(
                f"Attempting to remove potentially incomplete file: {output_fpath}"
            )
            try:
                output_fpath.unlink()
            except OSError as unlink_e:
                logger.error(
                    f"Failed to remove incomplete file {output_fpath}: {unlink_e}"
                )
        raise  # Re-raise the original exception


# --- Flow Definition ---


@flow(name="Flood Impact Analysis Workflow")
def flood_impact_analysis_flow(
    osm_pbf: str,
    floodplains_gpkg: str,
    clean: bool = False,
    verbose: bool = False,
):
    """
    Prefect flow orchestrating the refactored flood impact analysis pipeline
    with integrity hash verification.

    Args:
        osm_pbf_path: Filesystem path to the input OpenStreetMap PBF file.
        floodplains_gpkg_path: Filesystem path to the input Floodplains GeoPackage.
        verbose: If True, sets logging level to DEBUG; otherwise, INFO.
    """
    logger = get_run_logger()
    log_level = logging.DEBUG if verbose else logging.INFO
    logger.setLevel(log_level)
    logging.getLogger().setLevel(log_level)

    logger.info("--- Starting Flood Impact Analysis Workflow ---")
    logger.info(f"OSM Input: {osm_pbf}")
    logger.info(f"Floodplains Input: {floodplains_gpkg}")
    logger.info(f"Clean Run: {clean}")
    logger.info(f"Verbose Logging: {verbose}")
    logger.info("Integrity checks are always enabled.")

    # --- Step 1: Get Initial Graph, Region, and Floodplains ---
    # Call tasks directly for sequential execution
    logger.info("Running task: Enrich OSM data...")
    enriched_osm = enrich_osm_task(osm_pbf=osm_pbf)  # Direct call

    # Validate and extract OSM enrichment results
    if not enriched_osm:
        raise ValueError("OSM enrichment task did not return expected results.")
    osmnx_simplified_g = enriched_osm["g"]
    buffered_region_gdf = enriched_osm["buffered_region_gdf"]
    region_name = enriched_osm["region_name"]

    run_output_dir = experiment_data_dir / region_name
    run_output_filename = f"e004_pjt_flood_impact_analysis.{region_name}.gpkg"
    output_gpkg_path = run_output_dir / run_output_filename

    if not all(
        [
            osmnx_simplified_g is not None,  #
            buffered_region_gdf is not None,
            region_name,
        ]
    ):
        raise ValueError(
            f"OSM enrichment task missing required outputs. Found: {enriched_osm.keys()}"
        )
    logger.info(f"OSM enrichment complete for region: {region_name}")

    logger.info("Running task: Clip Floodplain data...")

    # --- Handle --clean flag: Remove output directory if requested ---
    if clean and run_output_dir.exists():
        logger.warning(
            f"Clean flag is True. Removing existing output directory: {run_output_dir}"
        )
        try:
            shutil.rmtree(run_output_dir)
            logger.info(f"Successfully removed directory: {run_output_dir}")
        except OSError as e:
            logger.error(
                f"Failed to remove directory {run_output_dir}: {e}", exc_info=True
            )
            # Decide if this is a fatal error or if we can proceed
            # For now, re-raise to stop the flow if cleanup fails
            raise RuntimeError(
                f"Failed to clean output directory {run_output_dir}"
            ) from e

        # Re-create the directory after removing it, ensuring it exists for saving later
        run_output_dir.mkdir(
            parents=True, exist_ok=False
        )  # exist_ok=False to ensure it was removed

    if not clean and output_gpkg_path.is_file():
        logger.info(f"Output file already exists: {output_gpkg_path}")
        logger.info(
            "Clean flag is False. Skipping analysis and returning existing path."
        )
        return str(output_gpkg_path)
    elif not clean and not output_gpkg_path.is_file():
        logger.info("Output file does not exist. Proceeding with analysis.")

    floodplains_gdf = get_clipped_floodplain_data_task(  # Direct call
        floodplains_gpkg_path=floodplains_gpkg,
        buffered_region_gdf=buffered_region_gdf,
    )

    # Validate clipped floodplains
    if floodplains_gdf is None or floodplains_gdf.empty:
        logger.warning(
            "Floodplain clipping resulted in empty or None GeoDataFrame. Analysis may be incomplete."
        )
        floodplains_gdf = gpd.GeoDataFrame(
            {"_flood_risk_level_": []}, geometry=[]
        )  # Create empty with column
    elif "_flood_risk_level_" not in floodplains_gdf.columns:
        raise ValueError(
            "Required column '_flood_risk_level_' not found in floodplain data after clipping task."
        )
    logger.info(f"Floodplain clipping complete. Found {len(floodplains_gdf)} features.")

    # --- Step 2: Initialize Core Impact Data & Hashes ---
    # This task always runs and calculates initial hashes internally
    initial_impact_data = initialize_flood_impact_data_task(
        osmnx_simplified_g=osmnx_simplified_g,
        floodplains_gdf=floodplains_gdf,
    )

    # Extract data needed for the next steps
    initial_hashes_map = initial_impact_data.integrity_hashes
    nonbridge_spans_gdf = initial_impact_data.nonbridge_spans_gdf
    bridge_spans_gdf = initial_impact_data.bridge_spans_gdf
    roads_gdf = initial_impact_data.roads_gdf

    # Always create a mutable dict from the initial hashes mapping
    hashes_to_pass = dict(initial_hashes_map)

    # --- Step 3: Spatial Join ---
    join_gdf, join_gdf_hash = perform_spatial_join_task(
        nonbridge_spans_gdf=nonbridge_spans_gdf,
        floodplains_gdf=floodplains_gdf,  # Use the one from Step 1
        integrity_hashes=hashes_to_pass,  # Pass the dict
    )

    # --- Step 4: Update Hash Dictionary ---
    # Add the newly calculated join_gdf_hash to the dictionary
    hashes_to_pass[IntegrityHashKey.JOIN_GDF] = join_gdf_hash
    logger.debug(f"Added JOIN_GDF hash. Current hashes: {hashes_to_pass}")

    # --- Step 5: Calculate Highest Risk ---
    highest_risk_series = calculate_highest_road_risk_for_nonbridge_spans_task(
        join_gdf=join_gdf,
        integrity_hashes=hashes_to_pass,  # Pass the UPDATED dict
    )

    # --- Step 6: Identify Impacted Edges ---
    nonfunctional_edges, isolated_edges = identify_impacted_edges_task(
        osmnx_simplified_g=osmnx_simplified_g,  # Use graph from Step 1
        nonbridge_spans_gdf=nonbridge_spans_gdf,
        bridge_spans_gdf=bridge_spans_gdf,
        highest_risk_series=highest_risk_series,
        integrity_hashes=hashes_to_pass,  # Pass the UPDATED dict
    )

    # --- Step 7: Add Impact Columns ---
    final_gdf, _ = add_impact_columns_task(  # Don't need final hash here
        roads_gdf=roads_gdf,
        nonfunctional_edges=nonfunctional_edges,
        isolated_edges=isolated_edges,
        integrity_hashes=hashes_to_pass,  # Pass the UPDATED dict
    )

    # --- Step 8: Save Output ---
    saved_gpkg_path = save_flood_impact_gdf_task(
        gdf=final_gdf,
        output_path=output_gpkg_path,  # Pass the full path
    )

    assert str(saved_gpkg_path) == str(output_gpkg_path)

    logger.info(
        "--- Flood Impact Analysis Workflow (Refactored) Finished Successfully ---"
    )
    logger.info(f"Output saved to: {output_gpkg_path}")

    return output_gpkg_path


# --- CLI Entrypoint ---

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run the Flood Impact Analysis Prefect Workflow.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--osm-pbf",
        required=True,
        help="Path to the input OpenStreetMap PBF file.",
    )

    parser.add_argument(
        "--floodplains-gpkg",
        required=True,
        help="Path to the input Floodplains GeoPackage file.",
    )

    parser.add_argument(
        "--clean",
        action="store_true",
        help=(
            "If set, remove the output directory for the region before running the analysis."
            "If not set, the analysis will skip calculations if previous results exists for the region."
        ),
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose (DEBUG level) logging for the workflow.",
    )

    args = parser.parse_args()

    try:
        final_output_path = flood_impact_analysis_flow(
            osm_pbf=args.osm_pbf,
            floodplains_gpkg=args.floodplains_gpkg,
            clean=args.clean,
            verbose=args.verbose,
        )
        print("\n--- Workflow Execution Summary ---")
        print("Status: Success")
        print(f"Final Output GeoPackage: {final_output_path}")
        print("---------------------------------")

    except Exception as flow_exception:
        # Log the exception within the flow context if possible,
        # otherwise print to stderr.
        try:
            flow_logger = get_run_logger()
            flow_logger.critical(f"Workflow failed: {flow_exception}", exc_info=True)
        except Exception:  # Handle cases where logger might not be available
            print("\n--- Workflow Execution Summary ---", file=sys.stderr)
            print("Status: Failed", file=sys.stderr)
            print("Error: {flow_exception}", file=sys.stderr)
            print("---------------------------------", file=sys.stderr)
        sys.exit(1)  # Exit with error code
