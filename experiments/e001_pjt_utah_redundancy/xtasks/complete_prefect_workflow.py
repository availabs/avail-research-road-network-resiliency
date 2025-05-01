"""
Roadway Redundancy Analysis Tool (Prefect-enabled).
"""

import argparse
import logging
import os
import shutil
import sys
from dataclasses import dataclass
from os import PathLike
from pathlib import Path
from typing import Set, Tuple

import geopandas as gpd
import networkx as nx
import pandas as pd
from prefect import flow, get_run_logger, task

# =============================================================================
# CONFIGURE LOGGING
# =============================================================================

# Consistent logging setup (do this once at the module level)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

# =============================================================================
# IMPORT EXTERNAL DEPENDENCIES
# =============================================================================

try:
    from experiments.e001_pjt_utah_redundancy.src.core.redundancy_metric import (
        compute_redundancy_for_edges,
        identify_major_roads,
    )
    from experiments.e001_pjt_utah_redundancy.src.utils.io_helpers import (
        create_detours_gpkg,
    )
    from tasks.osm import enrich_osm_task

except ImportError as e:
    logger.critical(f"Failed to import required modules: {e}")
    sys.exit(1)

# =============================================================================
# DATA CLASSES
# =============================================================================


@dataclass
class AnalysisPaths:
    """
    Standardized file paths for redundancy analysis outputs.
    """

    output_dir: Path
    detour_maps_dir: Path
    output_gpkg: Path

    @classmethod
    def for_region(
        cls,  # The class reference.
        region_name: str,
    ) -> "AnalysisPaths":
        """
        Factory method to create paths for a specific region.

        Parameters:
            region_name (str): Name of the geographic region being analyzed.

        Returns:
            AnalysisPaths: An instance with paths configured for the given region.
        """
        output_dir = Path(__file__).resolve().parent.parent / "data" / region_name

        return cls(
            output_dir=output_dir,
            detour_maps_dir=output_dir / "detour_maps",
            output_gpkg=(
                output_dir / f"e001_002_utah_redundancy_analysis.{region_name}.gpkg"
            ),
        )


# =============================================================================
# TASK FUNCTIONS
# =============================================================================


@task
def setup_directories_task(
    paths: AnalysisPaths,  #
) -> None:
    """
    Creates clean output directories for analysis results.

    Parameters:
        paths (AnalysisPaths): The output paths configuration.

    Returns:
        None
    """
    logger.info("Starting setup_directories_task")
    logger.debug(f"  paths: {paths}")  # Debug log for paths

    try:
        if paths.output_dir.exists():
            logger.info(f"  Removing existing output directory: {paths.output_dir}")
            shutil.rmtree(paths.output_dir)

        logger.info(f"  Creating output directory: {paths.output_dir}")
        paths.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"  Creating detour maps directory: {paths.detour_maps_dir}")
        paths.detour_maps_dir.mkdir(parents=True, exist_ok=True)

    except (PermissionError, OSError) as e:
        logger.error(f"  Failed to set up directories: {e}")
        raise

    logger.info("Finished setup_directories_task")


@task
def compute_redundancy_task(
    graph: nx.MultiDiGraph,  #
    edge_ids: Set[Tuple],
) -> pd.DataFrame:
    """
    Computes redundancy metrics for the specified road segments.

    Parameters:
        graph (nx.MultiDiGraph): The road network graph.
        edge_ids (Set[Tuple]): Identifiers for the road segments to analyze.

    Returns:
        pd.DataFrame: A DataFrame containing the computed redundancy metrics.
    """
    logger.info("Starting compute_redundancy_task")
    logger.debug(f"  Number of edges: {len(edge_ids)}")  # Debug log for edge count

    detours_df = compute_redundancy_for_edges(g=graph, edge_ids=edge_ids)

    if detours_df is None or detours_df.empty:
        logger.error("  Redundancy computation returned empty results")
        raise RuntimeError("Redundancy computation returned empty results")

    logger.info(f"  Computed redundancy metrics for {len(detours_df)} road segments")
    logger.info("Finished compute_redundancy_task")
    return detours_df


@task
def export_results_task(
    roadways_gdf: gpd.GeoDataFrame,
    detours_df: pd.DataFrame,
    output_gpkg: PathLike,
) -> None:
    """
    Exports analysis results to a GeoPackage.

    Parameters:
        paths (AnalysisPaths): Output file paths configuration.
        edges_gdf (gpd.GeoDataFrame): GeoDataFrame of road segments.
        detours_df (pd.DataFrame): DataFrame containing redundancy metrics.

    Returns:
        None
    """
    logger.info("Starting export_results_task")
    logger.debug(f"  Output path: {output_gpkg}")  # Debug log for output path

    if roadways_gdf is None or detours_df is None:
        logger.error("  Missing required data for export")
        raise ValueError("Missing required data for export")

    create_detours_gpkg(
        filename=output_gpkg,
        roadways_gdf=roadways_gdf,
        detours_info_df=detours_df,
    )

    logger.info(f"  Results exported to: {output_gpkg}")
    logger.info("Finished export_results_task")


@flow
def redundancy_analysis_flow(
    osm_pbf: str,  #
    clean: bool = False,
    verbose: bool = False,
) -> None:
    """
    Runs the complete redundancy analysis workflow.

    Parameters:
        osm_pbf (str): File path to the OSM PBF data.
    """

    log_level = logging.DEBUG if verbose else logging.INFO

    logger = get_run_logger()
    logger.setLevel(log_level)
    logging.getLogger().setLevel(log_level)

    logger.info("--- Starting Utah Redundancy Methodology Workflow ---")
    logger.info(f"OSM Input: {osm_pbf}")
    logger.info(f"Clean Run: {clean}")
    logger.info(f"Verbose Logging: {verbose}")

    if not os.path.exists(osm_pbf):
        logger.error(f"  OSM PBF file not found: {osm_pbf}")
        raise ValueError(f"OSM PBF file not found: {osm_pbf}")

    d = enrich_osm_task(osm_pbf=osm_pbf)

    g = d["g"]
    region_name = d["region_name"]
    roadways_gdf = d["edges_gdf"]

    paths = AnalysisPaths.for_region(region_name)
    output_dir = paths.output_dir
    output_gpkg = paths.output_gpkg

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

    setup_directories_task(paths)

    edge_ids = identify_major_roads(roadways_gdf=roadways_gdf)

    detours_df = compute_redundancy_task(
        graph=g,
        edge_ids=edge_ids,
    )

    export_results_task(
        roadways_gdf=roadways_gdf,  #
        detours_df=detours_df,
        output_gpkg=output_gpkg,
    )

    logger.info("Analysis completed successfully")
    logger.info("Finished redundancy_analysis_flow")

    return output_gpkg


# =============================================================================
# CLI FUNCTIONS
# =============================================================================


def parse_arguments() -> argparse.Namespace:
    """
    Parse command line arguments.

    Returns:
        argparse.Namespace: The parsed command line arguments.
    """
    parser = argparse.ArgumentParser(
        description="Compute road network redundancy using detour analysis methodology.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "-f",
        "--osm-pbf",
        type=str,
        help="Path to the OpenStreetMap PBF file for the region",
        required=True,
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

    return parser.parse_args()


def main() -> int:
    """
    Main entry point for the redundancy analysis.
    """
    args = parse_arguments()

    return redundancy_analysis_flow(
        osm_pbf=args.osm_pbf,  #
        clean=args.clean,
        verbose=args.verbose,
    )  # Run the Prefect flow


if __name__ == "__main__":
    main()
