import argparse
import logging
import os
import shutil
import stat
from pathlib import Path
from typing import Any

import geopandas as gpd
import pandas as pd
import pyproj
from prefect import flow, get_run_logger, task

from common.nysdot.structures.nysdot_bridges import get_clipped_nysdot_bridges_data
from common.nysdot.structures.nysdot_large_culverts import (
    get_clipped_large_culvert_data,
)

# From the experiment's src dir
from experiments.e009_pjt_osm_ris_conflation.src.ris_processing_pipeline import (
    enrich_with_ris_attributes,
    load_ris_data,
    osrm_matches_to_osmnx_edges,
    perform_osrm_matching,
    select_best_ris_for_osmnx_edge,
)
from tasks.osm import enrich_osm_task

# from tasks.osrm import start_osrm_docker_container_task

conflation_output_dir = os.path.join(
    os.path.dirname(__file__), "../data/processed/conflation/"
)

experiment_root = Path(__file__).parent.parent.resolve()
experiment_data_dir = experiment_root / "data"


@task
def initialize_geod_task() -> pyproj.Geod:
    """Task to initialize the pyproj Geod object."""
    # FIXME: Use a more accurate projection.
    print("Initializing pyproj Geod")
    return pyproj.Geod(ellps="WGS84", sphere=True)


@task
def load_ris_data_task(ris_source_path: str, county_geoid: str) -> pd.DataFrame:
    """Task to load RIS data."""
    print(f"Loading RIS data from {ris_source_path} for county {county_geoid}")
    return load_ris_data(
        ris_source_path=ris_source_path,
        county_geoid=county_geoid,
    )


@task
def perform_osrm_matching_task(
    ris_gdf: pd.DataFrame,  #
    osm_graph: Any,
    geod: pyproj.Geod,
    # osrm_host: str,
) -> pd.DataFrame:
    """Task to perform OSRM matching."""
    # print(f"Performing OSRM matching using host {osrm_host}")
    return perform_osrm_matching(
        ris_gdf=ris_gdf,
        osm_graph=osm_graph,
        # osrm_host=osrm_host,
        geod=geod,
    )


@task
def osrm_matches_to_osmnx_edges_task(
    osrm_match_results_df: pd.DataFrame, osmnx_graph_simplified: Any
) -> pd.DataFrame:
    """Task to convert OSRM matches to OSMnx edges."""
    print("Converting OSRM matches to OSMnx edges")
    return osrm_matches_to_osmnx_edges(
        osrm_match_results_df=osrm_match_results_df,
        osmnx_graph_simplified=osmnx_graph_simplified,
    )


@task
def select_best_ris_for_osmnx_edge_task(
    osmnx_edge_matches_df: pd.DataFrame, osmnx_graph_simplified: Any
) -> pd.DataFrame:
    """Task to select best RIS matches for OSMnx edges."""
    print("Selecting best RIS matches for OSMnx edges")
    return select_best_ris_for_osmnx_edge(
        osmnx_edge_matches_df=osmnx_edge_matches_df,
        osmnx_graph_simplified=osmnx_graph_simplified,
    )


@task
def get_clipped_nysdot_bridges_data_task(
    nysdot_bridges_source: str,
    buffered_region_gdf: pd.DataFrame,
    filter_out_rail_bridges: bool = True,
) -> pd.DataFrame:
    """Task to get clipped NYSDOT bridges data."""
    print(f"Getting clipped NYSDOT bridges data from {nysdot_bridges_source}")
    return get_clipped_nysdot_bridges_data(
        nysdot_bridges_source=nysdot_bridges_source,
        buffered_region_gdf=buffered_region_gdf,
        filter_out_rail_bridges=filter_out_rail_bridges,
    ).drop(columns="geometry")


@task
def get_clipped_nysdot_large_culverts_data_task(
    gis_source: str, buffered_region_gdf: pd.DataFrame
) -> pd.DataFrame:
    """Task to get clipped NYSDOT large culverts data."""
    print(f"Getting clipped NYSDOT large culverts data from {gis_source}")
    return get_clipped_large_culvert_data(
        gis_source=gis_source,
        buffered_region_gdf=buffered_region_gdf,
    ).drop(columns="geometry")


@task
def enrich_with_ris_attributes_task(
    selected_osmnx_matches_df: pd.DataFrame,
    nysdot_bridges_df: pd.DataFrame,
    nysdot_large_culverts_df: pd.DataFrame,
    ris_gpkg_path: str,
) -> pd.DataFrame:
    """Task to enrich with RIS attributes."""
    print(f"Enriching with RIS attributes using RIS GeoPackage at {ris_gpkg_path}")
    return enrich_with_ris_attributes(
        selected_osmnx_matches_df=selected_osmnx_matches_df,
        nysdot_bridges_df=nysdot_bridges_df,
        nysdot_large_culverts_df=nysdot_large_culverts_df,
        ris_gpkg_path=ris_gpkg_path,
    )


@task
def join_roads_geometry_task(
    roads_gdf: pd.DataFrame, enriched_with_ris_df: pd.DataFrame
) -> pd.DataFrame:
    """Task to join roads geometry with final conflation results."""
    print("Joining roads geometry with conflation results")
    # Assuming '_intersects_region_' is a boolean column in roads_gdf
    roads_geoms_gdf = roads_gdf[roads_gdf["_intersects_region_"]].drop(
        columns=roads_gdf.columns.difference(["geometry"])
    )

    osrm_conflation_gdf = roads_geoms_gdf.join(
        other=enriched_with_ris_df,
        how="inner",
    )
    return osrm_conflation_gdf


@task
def save_conflation_output_task(
    osrm_conflation_gdf: pd.DataFrame,
    output_gpkg: Path,
):
    """Task to save the conflation results to a GeoPackage and log statistics."""
    logger = get_run_logger()  # Get logger instance

    # Remove existing file if it exists
    if output_gpkg.exists():
        os.remove(output_gpkg)
    else:
        output_gpkg.parent.mkdir(parents=True, exist_ok=True)

    # Save layers to GeoPackage
    try:
        osrm_conflation_gdf.to_file(
            filename=output_gpkg,  #
            layer="osm_edges_with_ris_meta",
            driver="GPKG",
        )
        logger.info("Saved 'osm_edges_with_ris_meta' layer.")

        read_only_perms = stat.S_IREAD | stat.S_IRGRP | stat.S_IROTH
        os.chmod(output_gpkg, read_only_perms)
        logger.info(f"Task: Set file permissions to read-only for {output_gpkg}")

    except Exception as e:
        logger.error(f"Error saving GeoPackage layers to {output_gpkg}: {e}")
        # Depending on the severity, you might raise a more specific exception
        raise  # Re-raise the exception to fail the task

    return output_gpkg  # Return the path to the saved file


@task
def save_conflation_qa_task(
    roads_gdf: gpd.GeoDataFrame,
    ris_gdf: gpd.GeoDataFrame,
    osrm_conflation_gdf: pd.DataFrame,
    qa_output_gpkg: Path,
):
    """Task to save the conflation results to a GeoPackage and log statistics."""
    logger = get_run_logger()  # Get logger instance

    matched_route_ids = set(osrm_conflation_gdf["ris_route_id"])

    unmatched_ris_gdf = ris_gdf[
        ~ris_gdf["ROUTE_ID"].isin(matched_route_ids)
    ].copy()  # Add .copy() to avoid SettingWithCopyWarning if modified later

    total_ris_routes_ids = len(set(ris_gdf["ROUTE_ID"]))
    matched_ris_routes_ids = len(matched_route_ids)  # Use the set for matched count

    logger.info("--- Conflation Statistics ---")
    logger.info(f"Total RIS Routes: {total_ris_routes_ids}")
    logger.info(f"Matched RIS Routes: {matched_ris_routes_ids}")

    # Avoid division by zero
    matching_rate = (
        matched_ris_routes_ids / total_ris_routes_ids if total_ris_routes_ids > 0 else 0
    )
    logger.info(f"Matching Rate: {matching_rate:.4f}")
    logger.info("---------------------------")

    # Remove existing file if it exists
    if qa_output_gpkg.exists():
        os.remove(qa_output_gpkg)
    else:
        qa_output_gpkg.parent.mkdir(parents=True, exist_ok=True)

    # Save layers to GeoPackage
    try:
        # geopandas to_file method is typically used with DataFrames having a 'geometry' column
        # Assuming roads_gdf, ris_gdf, unmatched_ris_gdf, and osrm_conflation_gdf are GeoDataFrames
        # and the to_file method is from geopandas.
        roads_gdf.to_file(filename=qa_output_gpkg, layer="all_osm_edges", driver="GPKG")
        logger.info("Saved 'all_osm_edges' layer.")

        ris_gdf.to_file(filename=qa_output_gpkg, layer="all_ris", driver="GPKG")
        logger.info("Saved 'all_ris' layer.")

        unmatched_ris_gdf.to_file(
            filename=qa_output_gpkg, layer="unmatched_ris", driver="GPKG"
        )
        logger.info("Saved 'unmatched_ris' layer.")

        logger.info(f"Successfully saved all layers to {qa_output_gpkg}.")

        read_only_perms = stat.S_IREAD | stat.S_IRGRP | stat.S_IROTH
        os.chmod(qa_output_gpkg, read_only_perms)
        logger.debug(f"Task: Set file permissions to read-only for {qa_output_gpkg}")

    except Exception as e:
        logger.error(f"Error saving GeoPackage layers to {qa_output_gpkg}: {e}")
        # Depending on the severity, you might raise a more specific exception
        raise  # Re-raise the exception to fail the task

    return qa_output_gpkg  # Return the path to the saved file


@flow(name="RIS Conflation Workflow")
def perform_osmnx_conflation_flow(
    osm_pbf: str,
    ris_path: str,  # This will be used for loading RIS and as ris_gpkg_path
    nysdot_bridges_path: str,
    nysdot_large_culverts_path: str,
    clean: bool = False,
    verbose: bool = False,
):
    """
    Prefect flow that orchestrates the OSMnx and RIS conflation process.

    Args:
        osm_pbf: Path to the OSM PBF file.
        ris_path: Path to the RIS milepoint snapshot file (also used as ris_gpkg_path).
        nysdot_bridges_path: Path to the NYSDOT bridges shapefile.
        nysdot_large_culverts_path: Path to the NYSDOT large culverts shapefile.
        osrm_host: OSRM server host address.
        output_dir: Directory to save the output GeoPackage.
    """

    logger = get_run_logger()
    log_level = logging.DEBUG if verbose else logging.INFO
    logger.setLevel(log_level)
    logging.getLogger().setLevel(log_level)

    logger.info("--- Starting RIS Conflation Workflow Workflow ---")
    logger.info(f"OSM Input: {osm_pbf}")
    logger.info(f"RIS Input: {ris_path}")
    logger.info(f"NYSDOT Bridges Input: {nysdot_bridges_path}")
    logger.info(f"NYSDOT Large Culverts Input: {nysdot_large_culverts_path}")
    logger.info(f"Clean Run: {clean}")
    logger.info(f"Verbose Logging: {verbose}")
    logger.info("Integrity checks are always enabled.")

    osrm_cleanup = None
    try:
        # [1] Create or load enriched OSMnx graph
        enriched_osm = enrich_osm_task(osm_pbf=osm_pbf)

        G = enriched_osm["G"]
        g = enriched_osm["g"]
        geoid = enriched_osm["geoid"]
        region_name = enriched_osm["region_name"]
        roads_gdf = enriched_osm["edges_gdf"]
        buffered_region_gdf = enriched_osm["buffered_region_gdf"]

        output_dir = experiment_data_dir / region_name
        output_gpkg = output_dir / f"e009-pjt-ris-conflation.{region_name}.gpkg"
        qa_output_gpkg = output_dir / f"qa-e009-pjt-ris-conflation.{region_name}.gpkg"

        if clean and output_dir.exists():
            logger.warning(
                f"Clean flag is True. Removing existing output directory: {output_dir}"
            )
            try:
                shutil.rmtree(output_dir)
                logger.info(f"Successfully removed directory: {output_dir}")
            except OSError as e:
                logger.error(
                    f"Failed to remove directory {output_dir}: {e}", exc_info=True
                )
                # Decide if this is a fatal error or if we can proceed
                # For now, re-raise to stop the flow if cleanup fails
                raise RuntimeError(
                    f"Failed to clean output directory {output_dir}"
                ) from e

            # Re-create the directory after removing it, ensuring it exists for saving later
            output_dir.mkdir(
                parents=True, exist_ok=False
            )  # exist_ok=False to ensure it was removed

        if not clean and output_gpkg.is_file():
            logger.info(f"Output file already exists: {output_gpkg}")
            logger.info(
                "Clean flag is False. Skipping analysis and returning existing path."
            )
            return str(output_gpkg)
        elif not clean and not output_gpkg.is_file():
            logger.info("Output file does not exist. Proceeding with analysis.")

        # [2] Initialize pyproj Geod
        geod = initialize_geod_task()

        # [3] Load RIS data
        ris_gdf = load_ris_data_task(
            ris_source_path=ris_path,
            county_geoid=geoid,
        )

        # osrm_host, osrm_cleanup = start_osrm_docker_container_task(osm_pbf=osm_pbf)

        # [4] Perform OSRM matching
        osrm_match_results_df = perform_osrm_matching_task(
            ris_gdf=ris_gdf,
            osm_graph=G,
            # osrm_host=osrm_host,
            geod=geod,
        )

        # [5] Convert OSRM matches to OSMnx edges
        osmnx_edge_matches_df = osrm_matches_to_osmnx_edges_task(
            osrm_match_results_df=osrm_match_results_df,  #
            osmnx_graph_simplified=g,
        )

        # [6] Select best RIS matches for OSMnx edges
        selected_osmnx_matches_df = select_best_ris_for_osmnx_edge_task(
            osmnx_edge_matches_df=osmnx_edge_matches_df,  #
            osmnx_graph_simplified=g,
        )

        # [7] Get clipped NYSDOT bridges data
        nysdot_bridges_df = get_clipped_nysdot_bridges_data_task(
            nysdot_bridges_source=nysdot_bridges_path,
            buffered_region_gdf=buffered_region_gdf,
        )  # filter_out_rail_bridges defaults to True

        # [8] Get clipped NYSDOT large culverts data
        nysdot_large_culverts_df = get_clipped_nysdot_large_culverts_data_task(
            gis_source=nysdot_large_culverts_path,
            buffered_region_gdf=buffered_region_gdf,
        )

        # [9] Enrich with RIS attributes
        enriched_with_ris_df = enrich_with_ris_attributes_task(
            selected_osmnx_matches_df=selected_osmnx_matches_df,
            nysdot_bridges_df=nysdot_bridges_df,
            nysdot_large_culverts_df=nysdot_large_culverts_df,
            ris_gpkg_path=ris_path,  # Use ris_path as ris_gpkg_path
        )

        # [10] Join roads geometry with final conflation results
        osrm_conflation_gdf = join_roads_geometry_task(
            roads_gdf=roads_gdf,
            enriched_with_ris_df=enriched_with_ris_df,
        )

        # [11] Save results to GeoPackage and print statistics
        save_conflation_output_task(
            osrm_conflation_gdf=osrm_conflation_gdf,  #
            output_gpkg=output_gpkg,
        )

        # [11] Save results to GeoPackage and print statistics
        save_conflation_qa_task(
            roads_gdf=roads_gdf,
            ris_gdf=ris_gdf,
            osrm_conflation_gdf=osrm_conflation_gdf,
            qa_output_gpkg=qa_output_gpkg,
        )

        return output_gpkg  # Return the path to the final output file
    finally:
        try:
            if osrm_cleanup:
                osrm_cleanup()
        except Exception:
            pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run OSMnx and RIS Conflation Prefect Flow"
    )

    parser.add_argument(
        "--osm-pbf",  #
        required=True,
        help="Path to the OSM PBF file",
    )

    parser.add_argument(
        "--ris-path",
        required=True,
        help="Path to the RIS data (milepoint snapshot or GPKG)",
    )

    parser.add_argument(
        "--nysdot-bridges-path",
        required=True,
        help="Path to the NYSDOT bridges shapefile",
    )

    parser.add_argument(
        "--nysdot-large-culverts-path",
        required=True,
        help="Path to the NYSDOT large culverts shapefile",
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

    # Run the flow
    final_output_gpkg_path = perform_osmnx_conflation_flow(
        osm_pbf=args.osm_pbf,
        ris_path=args.ris_path,
        nysdot_bridges_path=args.nysdot_bridges_path,
        nysdot_large_culverts_path=args.nysdot_large_culverts_path,
        clean=args.clean,
        verbose=args.verbose,
    )

    print(f"\nConflation flow finished. Output saved to: {final_output_gpkg_path}")
