# -*- coding: utf-8 -*-
"""
Prefect flow to compute NetworkX metrics for a region and generate outputs.

This script orchestrates the following steps:
1. Load or create an enriched NetworkX graph from an OSM PBF file.
2. Compute all defined network metrics using functions from src.calculate_metrics.
3. Pickle each metric result and set the file to read-only.
4. Create a GeoPackage file summarizing node and edge metrics by reading
   the pickled results.
5. Set the final GeoPackage file to read-only.

Requires: prefect, networkx, geopandas, pyogrio, common.osm.enrich module,
          and src.calculate_metrics module.
"""

import argparse
import logging
import os
import pathlib
import pickle
import shutil
import stat
from os import PathLike
from typing import Any, Dict, List, Set

import geopandas as gpd
import networkx as nx
from prefect import flow, get_run_logger, task
from prefect.states import State
from prefect.task_runners import ConcurrentTaskRunner

# Option 2: Install modules in editable mode or rely on PYTHONPATH
try:
    # This assumes 'common' and 'src' are in the Python path or installed
    from experiments.e008_pjt_networkx_centrality.src.calculate_metrics import (
        NetworkMetric,
        compute_closeness_centrality,
        compute_edge_betweenness_centrality,
        compute_louvain_communities,
        compute_node_betweenness_centrality,
        get_pickle_file_path,
    )
    from tasks.osm import enrich_osm_task

except ImportError as e:
    print(
        "Error: Could not import necessary modules."
        " Ensure 'src.calculate_metrics' are available."
        f" Details: {e}"
    )
    exit(1)

# --- Constants and Configuration ---
# Mapping from NetworkMetric enum to its calculation function in src.calculate_metrics
# Note: Modularity and Partition Quality require communities, handled in GPKG task
METRIC_CALCULATION_FUNCTIONS = {
    NetworkMetric.NODE_BETWEENNESS_CENTRALITY: compute_node_betweenness_centrality,
    NetworkMetric.CLOSENESS_CENTRALITY: compute_closeness_centrality,
    NetworkMetric.EDGE_BETWEENNESS_CENTRALITY: compute_edge_betweenness_centrality,
    NetworkMetric.LOUVAIN_COMMUNITIES: compute_louvain_communities,
}

GPKG_LAYER_NODES = "intersections_with_centrality_metrics"
GPKG_LAYER_EDGES = "roads_with_centrality_metrics"
GPKG_FILENAME_TEMPLATE = "e008_001_networkx_centrality_metrics.{region_name}.gpkg"

EXPERIMENT_DATA_DIR = pathlib.Path(__file__).parent.parent / "data"

# --- Helper Functions ---


def set_read_only(path: pathlib.Path):
    """Sets the file at the given path to read-only."""
    try:
        current_permissions = os.stat(path).st_mode
        # Remove write permissions for owner, group, and others
        read_only_permissions = (
            current_permissions & ~stat.S_IWUSR & ~stat.S_IWGRP & ~stat.S_IWOTH
        )
        os.chmod(path, read_only_permissions)
        get_run_logger().info(f"Set read-only permissions for: {path}")
    except Exception as e:
        get_run_logger().error(
            f"Failed to set read-only permissions for {path}: {e}",
            exc_info=True,
        )


# --- Prefect Tasks ---


@task(retries=1, retry_delay_seconds=5)
def calculate_and_pickle_metric_task(
    g: nx.MultiDiGraph,
    metric: NetworkMetric,
    pickle_dir: pathlib.Path,
    region_name: str,
) -> None:
    """
    Prefect task to calculate a single network metric, pickle it, and set read-only.

    Args:
        g (nx.MultiDiGraph): The graph to analyze.
        metric (NetworkMetric): The metric to calculate.
        pickle_dir (pathlib.Path): The directory to save the pickle file.
        region_name (str): The region name for file naming.

    Returns:
        None: This task primarily performs side effects (file writing).

    Raises:
        KeyError: If the metric is not found in METRIC_CALCULATION_FUNCTIONS.
        Exception: Propagates exceptions from the metric calculation function.
    """
    logger = get_run_logger()
    logger.info(f"Starting calculation for metric: {metric.value}")

    pickle_path = pathlib.Path(get_pickle_file_path(pickle_dir, metric, region_name))

    # --- CHECK FOR EXISTING VALID PICKLE ---
    if pickle_path.exists():
        try:
            # Attempt to load the pickle to check if it's valid
            with open(pickle_path, "rb") as f:
                pickle.load(f)
            # If loading succeeds, skip calculation
            logger.info(
                f"Metric {metric.value}: Valid pickle file found at "
                f"{pickle_path}. Skipping calculation."
            )
            # Ensure it's read-only just in case permissions were changed
            set_read_only(pickle_path)
            return  # Exit the task successfully
        except (
            pickle.UnpicklingError,
            EOFError,
            AttributeError,
            ImportError,
            IndexError,
        ) as e:
            # Common errors indicating a corrupt or incompatible pickle
            logger.warning(
                f"Metric {metric.value}: Found existing pickle file "
                f"{pickle_path}, but it appears corrupt or invalid ({e}). "
                f"Proceeding with recalculation."
            )
            # Remove the corrupt file before recalculating
            try:
                pickle_path.unlink()
            except OSError as unlink_e:
                logger.error(
                    f"Could not remove corrupt pickle file {pickle_path}: {unlink_e}"
                )
                # Depending on severity, might want to raise here
        except Exception as e:
            # Catch other potential loading errors
            logger.warning(
                f"Metric {metric.value}: Error reading existing pickle file "
                f"{pickle_path}: {e}. Proceeding with recalculation."
            )
            try:
                pickle_path.unlink()
            except OSError as unlink_e:
                logger.error(
                    f"Could not remove problematic pickle file {pickle_path}: {unlink_e}"
                )
    # --- END CHECK ---

    # If we reach here, either the file didn't exist or was invalid

    if metric not in METRIC_CALCULATION_FUNCTIONS:
        raise KeyError(f"No calculation function defined for metric: {metric}")

    calculation_func = METRIC_CALCULATION_FUNCTIONS[metric]

    try:
        # Calculate the metric in memory
        metric_result = calculation_func(g)

        # Pickle the result
        logger.info(f"Saving metric {metric.value} to {pickle_path}...")
        with open(pickle_path, "wb") as f:
            pickle.dump(metric_result, f)

        # Set read-only permissions
        set_read_only(pickle_path)
        logger.info(f"Successfully calculated and saved metric: {metric.value}")

    except Exception as e:
        logger.error(
            f"Failed to calculate or save metric {metric.value}: {e}",
            exc_info=True,
        )
        # Ensure partial file is removed if pickling failed mid-way
        if pickle_path.exists():
            try:
                pickle_path.unlink()
            except OSError:
                logger.warning(
                    f"Could not remove potentially corrupt file: {pickle_path}"
                )
        raise  # Re-raise to mark the task as failed


@task(retries=1, retry_delay_seconds=5)
def create_geopackage_task(
    nodes_gdf: gpd.GeoDataFrame,
    edges_gdf: gpd.GeoDataFrame,
    region_name: str,
    pickle_dir: pathlib.Path,
    output_gpkg: PathLike,
) -> None:
    """
    Prefect task to create a GeoPackage file with calculated metrics.

    Reads metrics from pickle files, maps them to node/edge GeoDataFrames,
    and saves the result as a two-layer GeoPackage.

    Args:
        nodes_gdf (gpd.GeoDataFrame): Original GeoDataFrame of graph nodes.
        edges_gdf (gpd.GeoDataFrame): Original GeoDataFrame of graph edges.
        region_name (str): Identifier for the region.
        pickle_dir (pathlib.Path): Directory containing the metric pickle files.
        output_dir (pathlib.Path): Directory to save the final GPKG file.

    Returns:
        None: This task primarily performs side effects (file writing).

    Raises:
        FileNotFoundError: If required metric pickle files are missing.
        Exception: Propagates exceptions during file loading or GPKG writing.
    """
    logger = get_run_logger()
    logger.info("Starting GeoPackage creation...")

    output_gpkg_path = pathlib.Path(output_gpkg)

    # --- Create copies to avoid modifying original DataFrames ---
    nodes_out_gdf = nodes_gdf.copy()
    edges_out_gdf = edges_gdf.copy()

    # --- Helper function to load a metric pickle ---
    def load_metric(metric: NetworkMetric) -> Any:
        fpath = pathlib.Path(get_pickle_file_path(pickle_dir, metric, region_name))
        logger.info(f"Loading metric {metric.value} from {fpath}...")
        if not fpath.exists():
            logger.error(f"Required metric pickle file not found: {fpath}")
            raise FileNotFoundError(f"Missing metric file: {fpath}")
        try:
            with open(fpath, "rb") as file:
                return pickle.load(file)
        except Exception as e:
            logger.error(f"Failed to load pickle file {fpath}: {e}", exc_info=True)
            raise

    # --- Load and map metrics ---
    try:
        # Node Betweenness Centrality
        node_bc = load_metric(NetworkMetric.NODE_BETWEENNESS_CENTRALITY)
        nodes_out_gdf["betweenness_centrality"] = nodes_out_gdf.index.map(
            lambda n: node_bc.get(n)
        )
        edges_out_gdf["node_betweenness_centrality_u"] = (
            edges_out_gdf.index.get_level_values(0).map(lambda u: node_bc.get(u))
        )
        edges_out_gdf["node_betweenness_centrality_v"] = (
            edges_out_gdf.index.get_level_values(1).map(lambda v: node_bc.get(v))
        )

        # Edge Betweenness Centrality
        edge_bc = load_metric(NetworkMetric.EDGE_BETWEENNESS_CENTRALITY)
        edges_out_gdf["edge_betweenness_centrality"] = edges_out_gdf.index.map(
            lambda loc: edge_bc.get(
                loc
            )  # Assumes index is (u, v, key) or similar handled by get
        )

        # Closeness Centrality
        closeness = load_metric(NetworkMetric.CLOSENESS_CENTRALITY)
        nodes_out_gdf["closeness_centrality"] = nodes_out_gdf.index.map(
            lambda n: closeness.get(n)
        )
        edges_out_gdf["closeness_centrality_u"] = edges_out_gdf.index.get_level_values(
            0
        ).map(lambda u: closeness.get(u))
        edges_out_gdf["closeness_centrality_v"] = edges_out_gdf.index.get_level_values(
            1
        ).map(lambda v: closeness.get(v))

        # Louvain Communities (used for mapping and potentially modularity/quality)
        louvain_communities: List[Set[Any]] = load_metric(
            NetworkMetric.LOUVAIN_COMMUNITIES
        )
        node_to_louvain_id = {
            node: comm_id
            for comm_id, comm_nodes in enumerate(louvain_communities)
            for node in comm_nodes
        }
        nodes_out_gdf["louvain_community"] = nodes_out_gdf.index.map(
            lambda n: node_to_louvain_id.get(n)
        )
        edges_out_gdf["louvain_community_u"] = edges_out_gdf.index.get_level_values(
            0
        ).map(lambda u: node_to_louvain_id.get(u))
        edges_out_gdf["louvain_community_v"] = edges_out_gdf.index.get_level_values(
            1
        ).map(lambda v: node_to_louvain_id.get(v))

    except FileNotFoundError:
        # Error already logged by load_metric
        raise  # Re-raise to fail the task
    except Exception as e:
        logger.error(f"Error during metric mapping: {e}", exc_info=True)
        raise

    # --- Write to GeoPackage ---
    try:
        logger.info(f"Writing nodes layer to: {output_gpkg_path}")
        nodes_out_gdf.to_file(
            filename=output_gpkg_path,
            layer=GPKG_LAYER_NODES,
            driver="GPKG",
            engine="pyogrio",
        )

        logger.info(f"Writing edges layer to: {output_gpkg_path}")
        # Important: Use mode='a' to append layer to existing GPKG
        edges_out_gdf.to_file(
            filename=output_gpkg_path,
            layer=GPKG_LAYER_EDGES,
            driver="GPKG",
            engine="pyogrio",
            mode="a",  # Append layer
        )

        # Set final GPKG read-only
        set_read_only(output_gpkg_path)
        logger.info(f"Successfully created GeoPackage: {output_gpkg_path}")

    except Exception as e:
        logger.error(f"Failed to write GeoPackage file: {e}", exc_info=True)
        # Attempt to remove potentially incomplete GPKG file
        if output_gpkg_path.exists():
            try:
                output_gpkg_path.unlink()
            except OSError:
                logger.warning(
                    f"Could not remove potentially corrupt GPKG file: {output_gpkg_path}"
                )
        raise


# --- Prefect Flow ---


# NEW @flow decorator and function definition:
@flow(
    name="Network Metrics Batch Processing",
    description=(
        "Loads a graph, calculates all network metrics in parallel (with limit), "
        "saves results, and generates a summary GeoPackage."
    ),
)
def network_metrics_flow(
    osm_pbf: str,  # Use string for CLI compatibility
    max_workers: int = 2,  # Default max_workers for metric calculation
    clean: bool = False,  # <-- ADD clean_run parameter
    verbose: bool = False,
):
    """
    The main Prefect flow for calculating network metrics and outputs.

    Args:
        osm_pbf_path (str): Path to the input OpenStreetMap PBF file.
        max_workers (int, optional): Maximum number of concurrent metric
            calculation tasks. Defaults to 2.
        clean_run (bool, optional): If True, delete existing outputs for the
            region before starting. Defaults to False.
    """

    log_level = logging.DEBUG if verbose else logging.INFO

    logger = get_run_logger()
    logger.setLevel(log_level)
    logging.getLogger().setLevel(log_level)

    logger.info("--- Starting Network Metrics Workflow ---")
    logger.info(f"OSM Input: {osm_pbf}")
    logger.info(f"Clean Run: {clean}")
    logger.info(f"Verbose Logging: {verbose}")

    # --- Instantiate the runner inside the flow ---
    # This ensures the runner uses the max_workers value passed to the flow
    logger.info(f"Configuring ConcurrentTaskRunner with max_workers={max_workers}")
    flow.task_runner = ConcurrentTaskRunner(max_workers=max_workers)

    # --- Setup Paths ---
    osm_pbf = pathlib.Path(osm_pbf).resolve()

    enriched_osm = enrich_osm_task(osm_pbf=osm_pbf)

    g = enriched_osm["g"]
    nodes_gdf = enriched_osm["nodes_gdf"]
    edges_gdf = enriched_osm["edges_gdf"]
    region_name = enriched_osm["region_name"]

    logger.info(f"Using region name: {region_name}")

    output_dir = EXPERIMENT_DATA_DIR / region_name
    output_gpkg = output_dir / GPKG_FILENAME_TEMPLATE.format(region_name=region_name)

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

    if not clean and output_gpkg.is_file():
        logger.info(f"Output file already exists: {output_gpkg}")
        logger.info(
            "Clean flag is False. Skipping analysis and returning existing path."
        )
        return str(output_gpkg)
    elif not clean and not output_gpkg.is_file():
        logger.info("Output file does not exist. Proceeding with analysis.")

    # ------------------------

    # --- Prepare Output Directories ---
    metric_pickle_dir = output_dir / "metric_pickles"
    metric_pickle_dir.mkdir(parents=True, exist_ok=True)

    # GPKG output dir is region_output_dir
    logger.info(f"Pickle output directory: {metric_pickle_dir}")
    logger.info(f"GPKG output directory: {output_dir}")

    # --- Submit Metric Calculations ---
    metrics_to_calculate = list(METRIC_CALCULATION_FUNCTIONS.keys())
    metric_futures: Dict[NetworkMetric, State] = {}  # Store future states

    # Limit concurrency for metric calculation tasks if needed
    # Note: ConcurrentTaskRunner doesn't have a direct limit,
    # but DaskTaskRunner or others could. Here, we rely on OS scheduling
    # or external runner config. The max_workers arg is more conceptual here
    # unless using a runner that supports it directly. Let's log the intent.
    logger.info(
        f"Submitting {len(metrics_to_calculate)} metric tasks (intended max workers: {max_workers})..."
    )

    for metric in metrics_to_calculate:
        future = calculate_and_pickle_metric_task.submit(
            g=g,
            metric=metric,
            pickle_dir=metric_pickle_dir,
            region_name=region_name,
        )
        # Store the future object itself to check state later
        metric_futures[metric] = future

    # --- Wait for Metrics and Check for Failures ---
    logger.info("Waiting for metric calculations to complete...")

    all_metrics_succeeded = True

    for metric, future in metric_futures.items():
        try:
            # Block until the task completes; raises if it failed
            future.result()
            logger.info(f"Metric calculation SUCCEEDED: {metric.value}")
        except Exception as e:
            logger.error(
                f"Metric calculation FAILED: {metric.value}: {e}", exc_info=True
            )
            all_metrics_succeeded = False

    # --- Create GeoPackage (if all metrics succeeded) ---
    if all_metrics_succeeded:
        logger.info(
            "All metric calculations successful. Proceeding with GeoPackage creation."
        )
        # Submit GPKG task, ensuring it waits for all metric tasks implicitly
        output_gpkg = create_geopackage_task(
            nodes_gdf=nodes_gdf,
            edges_gdf=edges_gdf,
            region_name=region_name,
            pickle_dir=metric_pickle_dir,
            output_gpkg=output_gpkg,
            wait_for=metric_futures.values(),
        )
    else:
        raise RuntimeError("Metric calculation failures prevented GPKG creation.")

    logger.info("Network metrics flow finished.")

    return output_gpkg


# --- Command-Line Interface ---

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run the Prefect flow to calculate network metrics."
    )

    parser.add_argument(
        "--osm-pbf",
        required=True,
        help="Path to the input OpenStreetMap PBF file.",
    )

    parser.add_argument(
        "--max-workers",
        type=int,
        default=2,
        help="Maximum number of concurrent metric calculation tasks (conceptual limit). Default: 2.",
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
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    # Run the Prefect flow
    network_metrics_flow(
        osm_pbf=args.osm_pbf,
        max_workers=args.max_workers,
        clean=args.clean,  # Pass the clean flag to the flow
    )
