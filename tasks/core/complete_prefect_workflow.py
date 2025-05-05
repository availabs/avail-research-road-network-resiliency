# core/complete_prefect_workflow.py

import argparse
import logging
import os
import pathlib
import shutil

from prefect import (  # task import might be needed if subflows aren't used directly
    flow,
    get_run_logger,
)
from prefect.task_runners import ConcurrentTaskRunner

# --- Assumed Experiment Flow Imports ---
# Adjust these paths based on your actual project structure
from experiments.e001_pjt_utah_redundancy import (
    complete_prefect_workflow as e001_utah_redundancy_flow,
)
from experiments.e004_pjt_flood_impact import (
    complete_prefect_workflow as e004_flood_impact_flow,
)
from experiments.e008_pjt_networkx_centrality import (
    complete_prefect_workflow as e008_network_metrics_flow,
)
from experiments.e009_pjt_osm_ris_conflation import (
    complete_prefect_workflow as e009_conflation_flow,
)

# --- Import Processing Tasks ---
# Assumes prefect_tasks.py is saved as core/tasks/processing.py
from tasks.core.prefect_tasks import (
    join_results_task,
    process_base_edges_task,
    process_e001_task,
    process_e004_task,
    process_e008_task,
    process_e009_task,
    save_results_task,
)
from tasks.osm.tasks import (
    enrich_osm_task,
    extract_osm_region_road_network_task,
)

# --- Default Input Paths ---
# Use pathlib for robust path handling
THIS_DIR = pathlib.Path(__file__).parent.resolve()
# Default assumption: 'data' is two levels up from 'core/'
# Adjust if your structure is different
DEFAULT_DATA_ROOT = THIS_DIR.parent.parent / "data"
DEFAULT_PROCESSED_DATA_DIR = DEFAULT_DATA_ROOT / "processed"
DEFAULT_RAW_DATA_DIR = DEFAULT_DATA_ROOT / "raw"

DEFAULT_BASE_OSM_PBF = (
    DEFAULT_PROCESSED_DATA_DIR
    / "osm/nonservice-roadways-buffer-50mi-state-36_us-250101.osm.pbf"
)
DEFAULT_FLOODPLAINS_PATH = (
    DEFAULT_RAW_DATA_DIR
    / "avail/merged_floodplains/hazmit_db.s379_v841_avail_nys_floodplains_merged.1730233335.gpkg.zip"
)
DEFAULT_RIS_PATH = (
    DEFAULT_RAW_DATA_DIR / "nysdot/milepoint_snapshot/lrsn_milepoint.gpkg"
)
DEFAULT_NYSDOT_BRIDGES_PATH = (
    DEFAULT_RAW_DATA_DIR / "nysdot/nysdot_structures/NYSDOT_Bridges.20240909"
)
DEFAULT_NYSDOT_LARGE_CULVERS_PATH = (
    DEFAULT_RAW_DATA_DIR / "nysdot/nysdot_structures/NYSDOT_Large_Culverts.20241111"
)


@flow(name="Experiment Orchestration and Aggregation Workflow", log_prints=True)
def main_flow(
    # --- Region Args ---
    geoid: str,
    # --- Execution Args ---
    max_workers: int = 2,
    clean: bool = False,
    verbose: bool = False,
    # --- Input Data Path Args ---
    base_osm_pbf: str = str(DEFAULT_BASE_OSM_PBF),
    floodplains_gpkg: str = str(DEFAULT_FLOODPLAINS_PATH),
    ris_path: str = str(DEFAULT_RIS_PATH),
    nysdot_bridges_path: str = str(DEFAULT_NYSDOT_BRIDGES_PATH),
    nysdot_large_culverts_path: str = str(DEFAULT_NYSDOT_LARGE_CULVERS_PATH),
):
    """
    Orchestrates multiple experiments, processes results, joins them, and saves outputs.

    Args:
        geoid: The county FIPS code (or other region identifier).
        max_workers: Max concurrent Prefect tasks.
        clean: If True, remove existing output directory for the region before running.
        verbose: If True, enable DEBUG level logging.
        base_osm_pbf: Path to the base OSM PBF file covering a large area.
        floodplains_gpkg: Path to the floodplains GPKG (zipped or unzipped).
        ris_path: Path to the NYS RIS GPKG file.
        nysdot_bridges_path: Path to the NYSDOT bridges data directory or file.
        nysdot_large_culverts_path: Path to the NYSDOT large culverts data directory or file.
    """
    # --- Logging Setup ---
    log_level = logging.DEBUG if verbose else logging.INFO
    logger = get_run_logger()
    logger.setLevel(log_level)
    logging.getLogger("prefect").setLevel(
        log_level
    )  # Adjust Prefect's own logger level
    # Optionally set root logger level if other libraries need it
    # logging.getLogger().setLevel(log_level)

    logger.info("--- Starting Experiment Orchestration and Aggregation Workflow ---")
    logger.info(f"GEOID: {geoid}")
    logger.info(f"Max Workers: {max_workers}")
    logger.info(f"Clean Run: {clean}")
    logger.info(f"Verbose Logging: {verbose}")
    logger.debug(f"Base OSM PBF: {base_osm_pbf}")
    logger.debug(f"Floodplains GPKG: {floodplains_gpkg}")
    logger.debug(f"RIS Path: {ris_path}")
    logger.debug(f"NYSDOT Bridges Path: {nysdot_bridges_path}")
    logger.debug(f"NYSDOT Large Culverts Path: {nysdot_large_culverts_path}")

    # --- Configure Task Runner ---
    flow.task_runner = ConcurrentTaskRunner(max_workers=max_workers)
    logger.info(f"Configured ConcurrentTaskRunner with max_workers={max_workers}")

    # --- Initial Data Preparation ---
    # Extract the specific region's road network PBF
    # Pass base_osm_pbf path as string
    regional_osm_pbf_path = extract_osm_region_road_network_task(
        base_osm_pbf=base_osm_pbf,
        geoid=geoid,
        buffer_dist_mi=10,  # Consistent with original notebook/flow
    )
    logger.info(f"Extracted regional OSM PBF: {regional_osm_pbf_path}")

    # Enrich OSM data - get graph, nodes, edges, region name etc.
    enriched_osm = enrich_osm_task(osm_pbf=regional_osm_pbf_path)

    # Extract needed components from enrichment result
    # Critical: Ensure 'edges_gdf' returned by enrich_osm_task has the ['u','v','key'] index set!
    roadways_gdf = enriched_osm[
        "edges_gdf"
    ]  # This is the full GDF with _intersects_region_
    region_name = enriched_osm["region_name"]

    logger.info(f"Region Name: {region_name}")

    # --- Output Directory Setup ---

    # Output dir relative to this script: data/<region_name>/
    output_dir = THIS_DIR / "data" / region_name
    final_gpkg_path = output_dir / f"fused_experiments_output.{region_name}.gpkg"

    logger.info(f"Target output directory: {output_dir}")
    logger.info(f"Final aggregated GPKG path: {final_gpkg_path}")

    if clean and output_dir.exists():
        logger.warning(
            f"Clean flag is True. Removing existing output directory: {output_dir}"
        )
        try:
            shutil.rmtree(output_dir)
            logger.info(f"Successfully removed directory: {output_dir}")
        except OSError as e:
            logger.error(f"Failed to remove directory {output_dir}: {e}", exc_info=True)
            raise RuntimeError(f"Failed to clean output directory {output_dir}") from e
    # No need to create dir here, save_results_task will handle it

    # --- Run Experiments Concurrently ---
    # Pass the regional PBF path and other required static paths
    # Convert paths to strings if the sub-flows expect strings
    # Ensure sub-flows accept necessary arguments (e.g., clean, verbose, paths)
    logger.info("Submitting experiment sub-flows...")

    e001_gpkg = e001_utah_redundancy_flow(
        osm_pbf=regional_osm_pbf_path,
        clean=clean,
        verbose=verbose,
    )
    e004_gpkg = e004_flood_impact_flow(
        osm_pbf=regional_osm_pbf_path,
        floodplains_gpkg=floodplains_gpkg,  # Pass path from CLI arg
        clean=clean,
        verbose=verbose,
    )
    e008_gpkg = e008_network_metrics_flow(
        osm_pbf=regional_osm_pbf_path,
        clean=clean,
        verbose=verbose,
    )
    e009_gpkg = e009_conflation_flow(
        osm_pbf=regional_osm_pbf_path,
        ris_path=ris_path,  # Pass path from CLI arg
        nysdot_bridges_path=nysdot_bridges_path,  # Pass path from CLI arg
        nysdot_large_culverts_path=nysdot_large_culverts_path,  # Pass path from CLI arg
        clean=clean,
        verbose=verbose,
    )

    # --- Process Base Edges Concurrently with Experiments ---
    logger.info("Submitting base edge processing task...")
    # Pass the full roadways GDF from enrich_osm_task; the task handles filtering
    region_roadways_gdf_future = process_base_edges_task.submit(
        roadways_gdf=roadways_gdf
    )

    # --- Wait for Experiments and Get Output Paths ---
    logger.info("Waiting for experiment flows to complete...")
    # Using .result() here will block until each flow finishes and raise errors if they failed

    # --- Process Experiment Results Concurrently ---
    logger.info("Submitting experiment result processing tasks...")
    processed_e001_df_future = process_e001_task.submit(output_gpkg=e001_gpkg)
    processed_e004_df_future = process_e004_task.submit(output_gpkg=e004_gpkg)
    processed_e008_df_future = process_e008_task.submit(output_gpkg=e008_gpkg)
    processed_e009_df_future = process_e009_task.submit(output_gpkg=e009_gpkg)

    # --- Collect Processed Results ---
    # Wait for base edge processing and experiment processing tasks
    logger.info("Waiting for processing tasks to complete...")

    processed_experiment_dfs = {
        "e001": processed_e001_df_future.result(),
        "e004": processed_e004_df_future.result(),
        "e008": processed_e008_df_future.result(),
        "e009": processed_e009_df_future.result(),
    }
    logger.info("Processing tasks completed.")

    # --- Join Results ---
    logger.info("Submitting join results task...")
    final_aggregated_gdf_future = join_results_task.submit(
        region_roadways_gdf=region_roadways_gdf_future,
        processed_experiment_dfs=processed_experiment_dfs,
    )

    logger.info("Joining completed.")

    # --- Save Final Results ---
    logger.info("Submitting save results task...")

    # Pass the final aggregated GDF and the target output GPKG path
    save_future = save_results_task.submit(
        final_aggregated_gdf=final_aggregated_gdf_future,
        fused_results_gpkg=final_gpkg_path,  # Pass the constructed Path object
    )

    final_output_paths = save_future.result()  # Wait for save

    logger.info("Saving completed.")

    # --- Log Final Output Paths ---
    logger.info("--- Workflow Finished Successfully ---")
    if final_output_paths and final_output_paths.get("gpkg_path"):
        logger.info(f"Aggregated GPKG: {final_output_paths['gpkg_path']}")
    else:
        logger.error("Aggregated GPKG path not returned from save task.")

    if final_output_paths and final_output_paths.get("csv_path"):
        logger.info(f"Aggregated CSV: {final_output_paths['csv_path']}")
    else:
        logger.error("Aggregated CSV path not returned from save task.")

    return final_output_paths


# --- Command-Line Interface ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run Experiment Orchestration and Aggregation Workflow."
    )

    # --- CLI Arguments ---
    parser.add_argument(
        "--geoid",
        type=str,
        required=True,
        help="The GEOID (e.g., county FIPS code) for the region under study.",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=2,
        help="Maximum number of concurrent Prefect tasks. Default: CPU count or 2.",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help=(
            "If set, remove the output directory (data/<region_name>/) "
            "before running the analysis. Use with caution."
        ),
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose (DEBUG level) logging.",
    )

    # Input Path Arguments
    parser.add_argument(
        "--base-osm-pbf",
        type=str,
        default=str(DEFAULT_BASE_OSM_PBF),
        help=f"Path to the base OSM PBF file. Default: {DEFAULT_BASE_OSM_PBF}",
    )
    parser.add_argument(
        "--floodplains-gpkg",
        type=str,
        default=str(DEFAULT_FLOODPLAINS_PATH),
        help=f"Path to the floodplains GPKG/zip file. Default: {DEFAULT_FLOODPLAINS_PATH}",
    )
    parser.add_argument(
        "--ris-path",
        type=str,
        default=str(DEFAULT_RIS_PATH),
        help=f"Path to the NYS RIS GPKG file. Default: {DEFAULT_RIS_PATH}",
    )
    parser.add_argument(
        "--nysdot-bridges-path",
        type=str,
        default=str(DEFAULT_NYSDOT_BRIDGES_PATH),
        help=f"Path to the NYSDOT bridges data. Default: {DEFAULT_NYSDOT_BRIDGES_PATH}",
    )
    parser.add_argument(
        "--nysdot-large-culverts-path",
        type=str,
        default=str(DEFAULT_NYSDOT_LARGE_CULVERS_PATH),
        help=f"Path to the NYSDOT large culverts data. Default: {DEFAULT_NYSDOT_LARGE_CULVERS_PATH}",
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

    logging.info(f"Running flow with CLI args: {vars(args)}")  # Log parsed args

    # --- Execute the Flow ---
    # Pass parsed arguments directly to the flow function
    main_flow(
        geoid=args.geoid,
        max_workers=args.max_workers,
        clean=args.clean,
        verbose=args.verbose,
        base_osm_pbf=args.base_osm_pbf,
        floodplains_gpkg=args.floodplains_gpkg,
        ris_path=args.ris_path,
        nysdot_bridges_path=args.nysdot_bridges_path,
        nysdot_large_culverts_path=args.nysdot_large_culverts_path,
    )
