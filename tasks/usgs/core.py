# Example: prefect_workflows/usgs_nhdplus_workflow.py
import argparse
import logging
import os
import pathlib
import stat
from os import PathLike  # Keep PathLike if used in type hints for flow/task params

import geopandas as gpd  # Still needed for type hints if GDFs are passed around
from prefect import flow, get_run_logger, task

# Import the core logic function
from common.usgs.nhdplus_h.core import (
    # Import default values if needed for task signatures or configuration
    DEFAULT_NHDPLUS_CACHE_DIR,
    DEFAULT_NHDPLUS_FLOWLINES_LAYER_NAME,
    get_clipped_nhdplus_flowlines,
)

# Assuming your OSM tasks are also refactored or are Prefect-aware tasks
from tasks.osm.tasks import (  # Or common.osm.tasks if also refactored
    enrich_osm_task,
    extract_osm_region_road_network_task,
)

# --- Project Path Configuration (Managed by the workflow/application layer) ---
# It's good practice for the workflow to define where data lives relative to a project root
# or use absolute paths derived from configuration.
PROJECT_ROOT = pathlib.Path(__file__).parent.parent.parent.resolve()  # Adjust as needed
DEFAULT_DATA_ROOT = PROJECT_ROOT / "data"
DEFAULT_RAW_DATA_DIR = DEFAULT_DATA_ROOT / "raw"
DEFAULT_PROCESSED_DATA_DIR = DEFAULT_DATA_ROOT / "processed"

# Define default paths for data sources, using the project-relative paths
DEFAULT_BASE_OSM_PBF_PATH = (
    DEFAULT_PROCESSED_DATA_DIR
    / "osm/nonservice-roadways-buffer-50mi-state-36_us-250101.osm.pbf"
)
DEFAULT_NHDPLUS_H_GDB_PATH = (
    DEFAULT_RAW_DATA_DIR
    / "usgs/national_hydrology_dataset/NHDPlus_H_National_Release_2_GDB.zip"
)
# Define the actual cache directory to be used by this workflow's tasks
# This can be an absolute path.
WORKFLOW_NHDPLUS_CACHE_DIR = DEFAULT_PROCESSED_DATA_DIR / DEFAULT_NHDPLUS_CACHE_DIR


@task(name="Clip NHDPlus Flowlines")  # More descriptive task name
def get_clipped_nhdplus_flowlines_task(
    nhdplus_path: PathLike,
    buffered_region_gdf: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """
    Prefect task to clip NHDPlus flowlines for a given region.
    This task wraps the core domain logic.
    """
    logger = get_run_logger()  # Prefect logger for task-specific logging
    logger.info(f"Task: Starting NHDPlus flowlines clipping for source: {nhdplus_path}")

    # Call the core (Prefect-oblivious) function
    clipped_gdf = get_clipped_nhdplus_flowlines(
        nhdplus_path=nhdplus_path,
        buffered_region_gdf=buffered_region_gdf,
    )

    logger.info(
        f"Task: Successfully clipped NHDPlus flowlines. Features: {len(clipped_gdf)}"
    )
    return clipped_gdf


@flow(
    name="USGS NHDPlus Data Processing Workflow",  # More generic flow name
    log_prints=True,
)
def usgs_nhdplus_data_workflow(
    geoid: str,
    base_osm_pbf: str = str(DEFAULT_BASE_OSM_PBF_PATH),
    nhdplus_path: str = str(DEFAULT_NHDPLUS_H_GDB_PATH),
    verbose: bool = False,
):
    """
    Orchestrates extraction of a region buffer and clips NHDPlus flowlines to it.
    """
    log_level = logging.DEBUG if verbose else logging.INFO
    logger = get_run_logger()
    logger.setLevel(log_level)
    logging.getLogger("prefect").setLevel(
        log_level
    )  # Adjust Prefect's own logger level
    logger.info(f"Flow: Starting NHDPlus data processing for GEOID: {geoid}")

    # --- OSM Data Preparation ---
    regional_osm_pbf_path = extract_osm_region_road_network_task(
        base_osm_pbf=base_osm_pbf,
        geoid=geoid,
        buffer_dist_mi=10,  # Example buffer
    )
    logger.info(f"Flow: Regional OSM PBF extracted to: {regional_osm_pbf_path}")

    enriched_osm = enrich_osm_task(osm_pbf=regional_osm_pbf_path)
    region_name = enriched_osm["region_name"]
    buffered_region_gdf = enriched_osm["buffered_region_gdf"]

    logger.info(f"Flow: Buffered region mask created for GEOID: {geoid}")

    # --- NHDPlus Clipping ---
    clipped_flowlines = get_clipped_nhdplus_flowlines_task(
        nhdplus_path=nhdplus_path,
        buffered_region_gdf=buffered_region_gdf,
    )

    # --- Output/Saving (Application/Infrastructure concern, handled in the flow) ---
    if not clipped_flowlines.empty:
        # Define output path within the processed data directory, managed by the workflow
        output_dir = DEFAULT_PROCESSED_DATA_DIR / "usgs"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_gpkg_path = output_dir / f"nhdplus_flowlines.{region_name}.gpkg"

        output_gpkg_path.unlink(missing_ok=True)

        clipped_flowlines.to_file(
            output_gpkg_path,
            engine="pyogrio",
            layer=DEFAULT_NHDPLUS_FLOWLINES_LAYER_NAME,  # Layer name can also be dynamic
        )

        logger.info(f"Flow: Saved clipped NHDPlus flowlines to: {output_gpkg_path}")

        read_only_perms = stat.S_IREAD | stat.S_IRGRP | stat.S_IROTH
        os.chmod(output_gpkg_path, read_only_perms)
        logger.info(f"Task: Set file permissions to read-only for {output_gpkg_path}")
    else:
        logger.warning(
            f"Flow: No NHDPlus flowlines were clipped for GEOID: {geoid}. No output file created."
        )

    logger.info(f"Flow: NHDPlus data processing workflow completed for GEOID: {geoid}.")
    return output_gpkg_path if not clipped_flowlines.empty else None


# --- Command-Line Interface (for direct execution of the flow) ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run USGS NHDPlus Data Processing Workflow."
    )
    parser.add_argument(
        "--geoid", type=str, required=True, help="GEOID for the region."
    )
    parser.add_argument(
        "--base-osm-pbf",
        type=str,
        default=str(DEFAULT_BASE_OSM_PBF_PATH),
        help="Path to the base OSM PBF file.",
    )
    parser.add_argument(
        "--nhdplus-path",
        type=str,
        default=str(DEFAULT_NHDPLUS_H_GDB_PATH),
        help="Path to the NHDPlus H GDB zip file.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose (DEBUG level) logging.",
    )

    args = parser.parse_args()

    # --- Basic Logging Config for Script ---
    # Configures logging before Prefect takes over for the flow run
    init_log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=init_log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    # Set httpx logging lower to avoid excessive noise, unless verbose
    logging.getLogger("httpx").setLevel(
        logging.WARNING if not args.verbose else logging.DEBUG
    )

    args = parser.parse_args()

    usgs_nhdplus_data_workflow(
        geoid=args.geoid,
        base_osm_pbf=args.base_osm_pbf,
        nhdplus_path=args.nhdplus_path,
    )
