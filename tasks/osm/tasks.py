import argparse
import logging
import os
from os import PathLike
from pathlib import Path
from typing import Optional

import geopandas as gpd
from prefect import get_run_logger, task

from common.osm.enrich import (
    EnrichedOsmNetworkDataWithFullMetadata,
    create_bridge_spans_gdf,
    create_enriched_osmnx_graph_for_region,
    create_nonbridge_spans_gdf,
)
from common.osm.extract import (
    DEFAULT_OSM_EXTRACT_BUFFER_DIST_MI,
    DEFAULT_OSM_EXTRACTS_DIR,
    create_osm_region_road_network_extract_pbf,
)


@task(name="Create OSM PBF Extract for Region")
def extract_osm_region_road_network_task(
    base_osm_pbf: PathLike,  #
    geoid: str,
    buffer_dist_mi: Optional[int] = DEFAULT_OSM_EXTRACT_BUFFER_DIST_MI,
    output_dir: Optional[PathLike] = DEFAULT_OSM_EXTRACTS_DIR,
    clean: bool = False,
) -> Path:
    """
    Loads and enriches OSM data. Returns dict like 'd' in notebook.
    Keys should include 'region_name', 'edges_gdf', 'buffered_region_gdf'.
    """
    logger = logging.getLogger(__name__)

    logger.info(f"Starting OSM PBF extract creation for GEOID={geoid}")

    if not os.path.exists(base_osm_pbf):
        logger.error(f"Input OSM PBF file not found: {base_osm_pbf}")
        raise FileNotFoundError(f"Input OSM PBF file not found: {base_osm_pbf}")

    osm_pbf = create_osm_region_road_network_extract_pbf(
        base_osm_pbf=base_osm_pbf,
        geoid=geoid,
        buffer_dist_mi=buffer_dist_mi,
        output_dir=output_dir,
        clean=clean,
    )

    logger.info(f"OSM PBF extract creation complete for GEOID={geoid}")

    osm_pbf_path = Path(osm_pbf)

    return osm_pbf_path


@task(name="Enrich OSM Data")
def enrich_osm_task(
    osm_pbf: PathLike,  #
) -> EnrichedOsmNetworkDataWithFullMetadata:
    """
    Prefect task to create or load an enriched OSMnx graph.

    Wraps the common.osm.enrich.create_enriched_osmnx_graph_for_region function.
    Assumes this function handles its own caching logic internally if needed.

    Args:
        osm_pbf (PathLike): Path to the input OpenStreetMap PBF file.

    Returns:
        dict: A dictionary containing:
            - 'g': The enriched and simplified MultiDiGraph
            - 'nodes_gdf': A GeoDataFrame of all nodes in the graph
            - 'edges_gdf': A GeoDataFrame of all edges in the graph
            - 'region_gdf': The main region GeoDataFrame (CRS=EPSG:4326)
            - 'buffered_region_gdf': The buffered region GeoDataFrame (CRS=EPSG:4326)
            - 'geography_region_name': The name of the region, encoding geoid and buffer_dist_mi as
                                    "<geolevel>-<geoid>" if buffer_dist_mi=0
                                     or "buffer-<buffer_dist>mi-<geolevel>-<geoid>" otherwise
            - 'geoid': The GEOID of the region
            - 'buffer_dist_mi': The buffer distance in miles


    Raises:
        FileNotFoundError: If the osm_pbf file does not exist.
        Exception: Propagates exceptions from the underlying graph creation function.
    """
    logger = get_run_logger()

    logger.debug(f"Starting OSM enrichment for: {osm_pbf}")

    if not os.path.exists(osm_pbf):
        raise FileNotFoundError(f"Input OSM PBF file not found: {osm_pbf}")

    logger.debug(f"Creating/loading enriched OSMnx graph from {osm_pbf}...")

    try:
        # include_base_osm_data=True might be needed depending on the function's signature
        enriched_osm = create_enriched_osmnx_graph_for_region(
            osm_pbf=osm_pbf,  #
            include_base_osm_data=True,
        )

        region_name = enriched_osm["region_name"]

        logger.debug(f"Graph loaded successfully for region: {region_name}")

        return enriched_osm

    except Exception as e:
        logger.error(f"Failed to create/load graph from {osm_pbf}: {e}", exc_info=True)
        raise


@task(name="Create bridge spans GeoDataFrame")
def create_bridge_spans_gdf_task(edges_gdf: gpd.GeoDataFrame):
    """
    Prefect task to create a GeoDataFrame of bridge spans in the edges_gdf

    Wraps the common.osm.enrich.create_bridge_spans_gdf function.

    Parameters:
        edges_gdf (gpd.GeoDataFrame): GeoDataFrame of edges from `clean`, with columns:
            - geometry: LineString representing the edge.
            - osm_way_along_info: List of dicts with `start_coord_idx`, `end_coord_idx`,
              `bridge_tag`, `osmid`, `osm_nodes`, etc.
            - u, v, key: Edge identifiers.

    Returns:
        gpd.GeoDataFrame: A new GeoDataFrame with one row per bridge span, containing:
            - geometry: LineString of the bridge span segment.
            - start_coord_idx, end_coord_idx: Indices in the parent edge’s geometry.
            - start_ratio_along, end_ratio_along: Ratios along the parent edge’s length.
            - osmids: List of OSM way IDs in the span.
            - osm_nodes: List of OSM node IDs along the span.
            - u, v, key: Parent edge identifiers (in the index with span_idx).
    """
    logger = get_run_logger()

    logger.debug("Starting OSM create_bridge_spans_gdf")

    nonbridge_spans_gdf = create_bridge_spans_gdf(edges_gdf=edges_gdf)

    logger.debug("Created OSM bridge spans")

    return nonbridge_spans_gdf


@task(name="Create nonbridge spans GeoDataFrame")
def create_nonbridge_spans_gdf_task(edges_gdf: gpd.GeoDataFrame):
    """
    Prefect task to create a GeoDataFrame of nonbridge spans in the edges_gdf

    Wraps the common.osm.enrich.create_nonbridge_spans_gdf function.

    Parameters:
        edges_gdf (gpd.GeoDataFrame): GeoDataFrame of edges from `clean`, with columns:
            - geometry: LineString representing the edge.
            - osm_way_along_info: List of dicts with `start_coord_idx`, `end_coord_idx`,
              `bridge_tag`, `osmid`, `osm_nodes`, etc.
            - u, v, key: Edge identifiers.

    Returns:
        gpd.GeoDataFrame: A new GeoDataFrame with one row per non-bridge span, containing:
            - geometry: LineString of the non-bridge segment.
            - start_coord_idx, end_coord_idx: Indices in the parent edge’s geometry.
            - start_ratio_along, end_ratio_along: Ratios along the parent edge’s length.
            - osmids: List of OSM way IDs in the span.
            - osm_nodes: List of OSM node IDs along the span.
            - u, v, key: Parent edge identifiers (in the index with span_idx).
    """
    logger = get_run_logger()

    logger.debug("Starting OSM create_nonbridge_spans_gdf")

    nonbridge_spans_gdf = create_nonbridge_spans_gdf(edges_gdf=edges_gdf)

    logger.debug("Created OSM nonbridge spans")

    return nonbridge_spans_gdf


# --- Command-Line Interface ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OSM pre-processing")

    subparsers = parser.add_subparsers(
        dest="command", required=True, help="Available tasks"
    )

    extract_parser = subparsers.add_parser(
        name="extract",
        help="Create an osm.pbf by extracting the specified region (geoid) from the base-osm-pbf",
    )

    # --- CLI Arguments ---
    extract_parser.add_argument(
        "--geoid",
        type=str,
        required=True,
        help="The GEOID (e.g., county FIPS code) for the region under study.",
    )
    # Input Path Arguments
    extract_parser.add_argument(
        "--base-osm-pbf",
        type=str,
        required=True,
        help="Path to the base OSM PBF file.",
    )
    # Input Path Arguments
    extract_parser.add_argument(
        "--buffer-dist-mi",
        type=int,
        default=10,
        help="Buffer distance in miles around the geographic region's boundary.",
    )

    args = parser.parse_args()

    if args.command == "extract":
        osm_pbf = create_osm_region_road_network_extract_pbf(
            base_osm_pbf=args.base_osm_pbf,
            geoid=args.geoid,
            buffer_dist_mi=args.buffer_dist_mi,
        )

        print(osm_pbf)
    else:
        raise ValueError("Unsupported command")
