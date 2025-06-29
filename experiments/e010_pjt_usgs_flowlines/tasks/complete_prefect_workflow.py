import argparse
import logging
import os
import shutil
import stat
from dataclasses import dataclass
from os import PathLike
from pathlib import Path
from typing import Callable, Optional

import geopandas as gpd
import pandas as pd
from prefect import flow, get_run_logger, task
from prefect.futures import PrefectFuture

from experiments.e010_pjt_usgs_flowlines.src.road_intersection_risk_assessment_v1 import (
    aggregate_risks,
    assign_simple_hydrological_risk,
    calculate_composite_roadway_hazard_score,
)
from experiments.e010_pjt_usgs_flowlines.src.road_spans_x_flowlines import (
    process_road_flowline_intersections,
)

# Assuming 'tasks' and 'experiments' are importable from the current PYTHONPATH
# Adjust these imports based on your actual project structure
from tasks.osm import (
    create_combined_road_spans_task,
    enrich_osm_task,
)
from tasks.usgs.core import get_clipped_nhdplus_flowlines_task

experiment_root = Path(__file__).parent.parent.resolve()
experiment_data_dir = experiment_root / "data"


@task(name="Prepare Roads for Merge")
def prepare_roads_for_composite_merge_task(
    osm_roads_gdf: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """
    Prepares the original OSM roads GeoDataFrame for merging with composite hazard scores.
    Selects only the geometry and the (u,v,key) index.
    """
    logger = get_run_logger()
    logger.info("Preparing osm_roads_gdf for merging with composite hazard scores.")

    # osm_roads_gdf is indexed by (u,v,key) from enrich_osm_task
    if not all(idx_name in osm_roads_gdf.index.names for idx_name in ["u", "v", "key"]):
        msg = f"Expected (u,v,key) index in osm_roads_gdf, found: {osm_roads_gdf.index.names}"
        logger.error(msg)
        raise ValueError(msg)

    # Keep only geometry, and reset index to have u,v,key as columns for selection, then set back
    roads_for_merge_gdf = osm_roads_gdf.reset_index()

    columns_to_keep = ["u", "v", "key", osm_roads_gdf.geometry.name]
    roads_for_merge_gdf = roads_for_merge_gdf[columns_to_keep]
    roads_for_merge_gdf = roads_for_merge_gdf.set_index(["u", "v", "key"])

    logger.info(
        f"Prepared roads for merge. Index: {roads_for_merge_gdf.index.names}, Columns: {roads_for_merge_gdf.columns.tolist()}"
    )
    return roads_for_merge_gdf


@task(name="Process Road/Flowline Intersections")
def process_road_flowline_intersections_task(
    road_spans_gdf: gpd.GeoDataFrame,
    nhd_flowlines_gdf: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """Prefect task wrapper for process_road_flowline_intersections."""
    logger = get_run_logger()
    logger.info("Starting road span and NHD flowline intersection processing task.")
    # The core function has a `strategy` parameter, but it has a default.
    # The current call in the flow doesn't provide it, so we omit it from the
    # task signature for simplicity and rely on the default.
    return process_road_flowline_intersections(
        road_spans_gdf=road_spans_gdf,
        nhd_flowlines_gdf=nhd_flowlines_gdf,
    )


@task(name="Merge Composite Hazard Score")
def merge_composite_hazard_score_task(
    roads_for_merge_gdf: gpd.GeoDataFrame, composite_df: pd.DataFrame
) -> gpd.GeoDataFrame:
    """
    Merges the composite hazard scores (DataFrame) with road geometries (GeoDataFrame).
    """
    logger = get_run_logger()
    logger.info("Merging composite hazard scores with road geometries.")

    # Ensure composite_df is indexed by (u,v,key)
    if not all(idx_name in composite_df.index.names for idx_name in ["u", "v", "key"]):
        msg = f"Expected (u,v,key) index in composite_df, found: {composite_df.index.names}"
        logger.error(msg)
        raise ValueError(msg)

    composite_gdf_with_geom = roads_for_merge_gdf.merge(
        right=composite_df,
        left_index=True,
        right_index=True,
        how="left",  # Keep all roads
    )
    logger.info(
        f"Merged composite hazards. Resulting GDF has columns: {composite_gdf_with_geom.columns.tolist()}"
    )
    return composite_gdf_with_geom


@task(name="Save Workflow Outputs")
def save_outputs_task(
    road_spans_gdf: gpd.GeoDataFrame,
    nhd_flowlines_gdf: gpd.GeoDataFrame,
    intersections_with_risk_gdf: gpd.GeoDataFrame,
    composite_gdf_with_geom: gpd.GeoDataFrame,
    output_gpkg_path: Path,
    region_name: str,  # For logging consistency
):
    """
    Saves the output GeoDataFrames to a single GeoPackage file with multiple layers.
    """
    logger = get_run_logger()
    logger.info(f"Saving outputs to {output_gpkg_path} for region {region_name}")

    if output_gpkg_path.exists():
        logger.info(f"Output file {output_gpkg_path} exists, removing.")
        try:
            os.remove(output_gpkg_path)
        except OSError as e:
            logger.error(
                f"Failed to remove existing output file {output_gpkg_path}: {e}",
                exc_info=True,
            )
            # Depending on desired behavior, you might raise the error or just warn
            # For now, let's try to proceed if removal fails but directory creation might still work

    output_gpkg_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        # Reset index for all GDFs to ensure proper writing if they have complex MultiIndex
        # and to avoid potential issues with some pyogrio/fiona versions.
        # The index information is preserved in the columns.
        road_spans_gdf.reset_index().to_file(
            filename=output_gpkg_path,  #
            layer="road_spans",
            driver="GPKG",
        )
        logger.info("Saved 'road_spans' layer.")

        nhd_flowlines_gdf.reset_index().to_file(
            filename=output_gpkg_path,  #
            layer="nhd_flowlines",
            driver="GPKG",
        )
        logger.info("Saved 'nhd_flowlines' layer.")

        intersections_with_risk_gdf.reset_index().to_file(
            filename=output_gpkg_path,
            layer="roadspans_x_flowlines_with_risk",  # Consistent with earlier name
            driver="GPKG",
        )
        logger.info("Saved 'roadspans_x_flowlines_with_risk' layer.")

        composite_gdf_with_geom.reset_index().to_file(
            filename=output_gpkg_path,  #
            layer="roads_with_hazards",
            driver="GPKG",
        )
        logger.info("Saved 'roads_with_hazards' layer.")

        read_only_perms = stat.S_IREAD | stat.S_IRGRP | stat.S_IROTH
        os.chmod(output_gpkg_path, read_only_perms)
        logger.info(f"Set file permissions to read-only for {output_gpkg_path}")

    except Exception as e:
        logger.error(
            f"Error saving GeoPackage layers to {output_gpkg_path}: {e}", exc_info=True
        )
        raise
    return str(output_gpkg_path)


@task(name="Assign Simple Hydrological Risk")
def assign_simple_hydrological_risk_task(
    road_x_flowline_points_gdf: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """Prefect task wrapper for assign_simple_hydrological_risk."""
    logger = get_run_logger()
    logger.info("Assigning simple hydrological risk to intersection points.")
    return assign_simple_hydrological_risk(
        road_x_flowline_points_gdf=road_x_flowline_points_gdf
    )


@task(name="Aggregate Risks")
def aggregate_risks_task(
    intersections_with_risk_gdf: gpd.GeoDataFrame,
) -> pd.DataFrame:
    """Prefect task wrapper for aggregate_risks."""
    logger = get_run_logger()
    logger.info("Aggregating intersection risks to the roadway level.")
    return aggregate_risks(intersections_with_risk_gdf=intersections_with_risk_gdf)


@task(name="Calculate Composite Roadway Hazard Score")
def calculate_composite_roadway_hazard_score_task(
    roadway_aggregated_hazards_df: pd.DataFrame,
) -> pd.DataFrame:
    """Prefect task wrapper for calculate_composite_roadway_hazard_score."""
    logger = get_run_logger()
    logger.info("Calculating composite roadway hazard score.")
    return calculate_composite_roadway_hazard_score(
        roadway_aggregated_hazards_df=roadway_aggregated_hazards_df
    )


@dataclass
class HydrologicalHazardStrategy:
    """
    A strategy object holding callable tasks for the hydrological hazard workflow.
    """

    enrich_osm: Callable = enrich_osm_task
    create_combined_road_spans: Callable = create_combined_road_spans_task
    get_clipped_nhdplus_flowlines: Callable = get_clipped_nhdplus_flowlines_task
    process_road_flowline_intersections: Callable = (
        process_road_flowline_intersections_task
    )
    assign_simple_hydrological_risk: Callable = assign_simple_hydrological_risk_task
    aggregate_risks: Callable = aggregate_risks_task
    calculate_composite_roadway_hazard_score: Callable = (
        calculate_composite_roadway_hazard_score_task
    )
    prepare_roads_for_composite_merge: Callable = prepare_roads_for_composite_merge_task
    merge_composite_hazard_score: Callable = merge_composite_hazard_score_task
    save_outputs: Callable = save_outputs_task


# --- Main Flow ---
@flow(name="Roadway Hydrological Hazard Workflow")
def roadway_hydrological_hazard_flow(
    osm_pbf: PathLike,
    nhd_flowlines_path: PathLike,
    clean: bool = False,
    verbose: bool = False,
    output_gpkg: Optional[PathLike] = None,
    strategy: HydrologicalHazardStrategy = HydrologicalHazardStrategy(),
):
    """
    Prefect flow that orchestrates the roadway hydrological hazard assessment.
    """
    logger = get_run_logger()
    log_level = logging.DEBUG if verbose else logging.INFO
    # Configure root logger for messages from underlying libraries if needed
    # Prefect's logger will capture task logs. This basicConfig might be redundant
    # if Prefect's logging setup is sufficient.
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,  # if you need to override other logging configs
    )
    logger.info("--- Starting Roadway Hydrological Hazard Workflow ---")
    logger.info(f"OSM Input: {osm_pbf}")
    logger.info(f"NHD Flowlines Input: {nhd_flowlines_path}")
    logger.info(f"Clean Run: {clean}")
    logger.info(f"Verbose Logging: {verbose}")

    # --- 1. Load and Enrich OSM Data ---
    enriched_osm_result = strategy.enrich_osm(osm_pbf=osm_pbf)

    # When a Prefect task is called in a flow, it returns a PrefectFuture.
    # For testing, the strategy might substitute a regular function that returns
    # the result directly. This check handles both cases robustly.
    if isinstance(enriched_osm_result, PrefectFuture):
        enriched_osm = enriched_osm_result.result()
    else:
        enriched_osm = enriched_osm_result

    region_name = enriched_osm["region_name"]
    edges_gdf = enriched_osm["edges_gdf"]
    buffered_region_gdf = enriched_osm["buffered_region_gdf"]

    if output_gpkg:
        output_gpkg_path = Path(output_gpkg)
        if clean and output_gpkg_path.exists():
            logger.warning(
                f"Clean flag is True. Removing existing output file: {output_gpkg_path}"
            )
            try:
                os.remove(output_gpkg_path)
                logger.info(f"Successfully removed file: {output_gpkg_path}")
            except OSError as e:
                logger.error(
                    f"Failed to remove file {output_gpkg_path}: {e}", exc_info=True
                )
                raise RuntimeError(
                    f"Failed to clean output file {output_gpkg_path}"
                ) from e
        output_gpkg_path.parent.mkdir(parents=True, exist_ok=True)
    else:
        output_dir = experiment_data_dir / region_name
        output_gpkg_path = output_dir / f"e010_roadways_x_flowlines.{region_name}.gpkg"
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
                raise RuntimeError(
                    f"Failed to clean output directory {output_dir}"
                ) from e
        output_dir.mkdir(parents=True, exist_ok=True)

    if not clean and output_gpkg_path.is_file():
        logger.info(
            f"Output file {output_gpkg_path} already exists and 'clean' is False. Skipping workflow."
        )
        return str(output_gpkg_path)

    # --- 3. Combine Spans ---
    combined_road_spans_result = strategy.create_combined_road_spans(
        edges_gdf=edges_gdf
    )

    # --- 4. Load NHD Flowlines ---
    nhd_flowlines_gdf_result = strategy.get_clipped_nhdplus_flowlines(
        nhdplus_path=nhd_flowlines_path,
        buffered_region_gdf=buffered_region_gdf,
    )

    # --- 5. Process Road/Flowline Intersections ---
    intersections_gdf_result = strategy.process_road_flowline_intersections(
        road_spans_gdf=combined_road_spans_result,
        nhd_flowlines_gdf=nhd_flowlines_gdf_result,
    )

    # --- 6. Assign Simple Hydrological Risk ---
    intersections_with_risk_gdf_result = strategy.assign_simple_hydrological_risk(
        road_x_flowline_points_gdf=intersections_gdf_result
    )

    print("\n\n\n" + "=" * 50)
    print(intersections_with_risk_gdf_result)

    # --- 7. Aggregate Risks ---
    # This task expects 'intersections_with_risk_gdf' to have specific NHDPlus columns.
    # assign_simple_hydrological_risk should preserve them.
    aggregated_risks_df_result = strategy.aggregate_risks(
        intersections_with_risk_gdf=intersections_with_risk_gdf_result
    )

    # --- 8. Calculate Composite Roadway Hazard Score ---
    composite_df_result = strategy.calculate_composite_roadway_hazard_score(
        roadway_aggregated_hazards_df=aggregated_risks_df_result
    )

    # --- 9. Prepare osm_roads_gdf for merge (select geometry and (u,v,key) index) ---
    roads_for_merge_gdf_result = strategy.prepare_roads_for_composite_merge(
        osm_roads_gdf=edges_gdf
    )

    # --- 10. Merge Composite Score with Road Geometries ---
    composite_gdf_with_geom_result = strategy.merge_composite_hazard_score(
        roads_for_merge_gdf=roads_for_merge_gdf_result,  #
        composite_df=composite_df_result,
    )

    # --- 11. Save All Outputs ---
    final_gpkg_path_result = strategy.save_outputs(
        road_spans_gdf=combined_road_spans_result,
        nhd_flowlines_gdf=nhd_flowlines_gdf_result,
        intersections_with_risk_gdf=intersections_with_risk_gdf_result,
        composite_gdf_with_geom=composite_gdf_with_geom_result,
        output_gpkg_path=output_gpkg_path,
        region_name=region_name,
    )

    final_gpkg_path = (
        final_gpkg_path_result
        if isinstance(final_gpkg_path_result, str)
        else final_gpkg_path_result.result()
    )

    logger.info(
        f"--- Roadway Hydrological Hazard Workflow COMPLETED for {region_name} ---"
    )

    return final_gpkg_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run Roadway Hydrological Hazard Workflow"
    )
    parser.add_argument(
        "--osm-pbf",  #
        required=True,
        help="Path to the OSM PBF file.",
    )

    parser.add_argument(
        "--nhd-flowlines-path",
        required=True,
        help="Path to the NHDPlus Flowlines GeoPackage.",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="If set, remove the region-specific output directory before running.",
    )
    parser.add_argument(
        "--verbose", action="store_true", help="Enable verbose (DEBUG level) logging."
    )
    parser.add_argument(
        "--output-gpkg",
        type=str,
        help="Optional path to save the output GeoPackage file.",
    )
    args = parser.parse_args()

    roadway_hydrological_hazard_flow(
        osm_pbf=args.osm_pbf,
        nhd_flowlines_path=args.nhd_flowlines_path,
        clean=args.clean,
        verbose=args.verbose,
        output_gpkg=args.output_gpkg,
    )
