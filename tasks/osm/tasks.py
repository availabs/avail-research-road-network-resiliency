import argparse
import logging
import os
import stat
from os import PathLike
from pathlib import Path
from typing import Optional, cast

import geopandas as gpd
import networkx as nx
from pandera.typing import DataFrame
from prefect import get_run_logger, task

from common.osm.enrich import (
    BridgeSpanSchema,
    CombinedRoadSpansSchema,
    EdgesSchema,
    EnrichedOsmNetworkDataWithFullMetadata,
    NonBridgeSpanSchema,
    convert_graph_to_gdfs,
    create_bridge_spans_gdf,
    create_combined_road_spans,
    create_full_enriched_osmnx_graph_for_region,
    create_nonbridge_spans_gdf,
    parse_osm_region_parameters,
)
from common.osm.extract import (
    DEFAULT_OSM_EXTRACT_BUFFER_DIST_MI,
    DEFAULT_OSM_EXTRACTS_DIR,
    OSMWaysFilter,
    create_osm_region_road_network_extract_pbf,
)


@task(name="Create OSM PBF Extract for Region")
def extract_osm_region_road_network_task(
    base_osm_pbf: PathLike,  #
    geoid: Optional[str],
    buffer_dist_mi: Optional[int] = DEFAULT_OSM_EXTRACT_BUFFER_DIST_MI,
    output_dir: Optional[PathLike] = DEFAULT_OSM_EXTRACTS_DIR,
    clean: bool = False,
    ways_filter: OSMWaysFilter = OSMWaysFilter.NONSERVICE,
) -> Path:
    """
    Prefect task to create a regional OSM PBF extract using Osmosis.

    This task wraps the `create_osm_region_road_network_pbf` function to generate
    a smaller PBF file containing road network data for a specified geographic
    area, defined by a US Census GEOID and an optional buffer distance.

    Args:
        base_osm_pbf (PathLike): The path to the source OSM PBF file from which
            the extract will be created.
        geoid (Optional[str]): The US Census GEOID for the target region. If None,
            no spatial filter will be applied to the OSM PBF file.
        buffer_dist_mi (Optional[int]): An optional buffer distance in miles
            around the region. Defaults to `DEFAULT_OSM_EXTRACT_BUFFER_DIST_MI`.
        output_dir (Optional[PathLike]): The directory where the output PBF
            extract will be saved. Defaults to `DEFAULT_OSM_EXTRACTS_DIR`.
        clean (bool): If `True`, forces the recreation of the output PBF file
            even if it already exists. Defaults to `False`.
        ways_filter (OSMWaysFilter): An enum member specifying which OSM ways to
            include based on highway tags. Defaults to
            `OSMWaysFilter.NONSERVICE`.

    Returns:
        Path: The path to the newly created or existing OSM PBF extract file.

    Raises:
        FileNotFoundError: If the `base_osm_pbf` file does not exist.
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
        ways_filter=ways_filter,
    )

    logger.info(f"OSM PBF extract creation complete for GEOID={geoid}")

    osm_pbf_path = Path(osm_pbf)

    return osm_pbf_path


@task(name="Convert Graph to GDFs")
def convert_graph_to_gdfs_task(g: nx.MultiDiGraph):
    return convert_graph_to_gdfs(g=g)


@task(name="Enrich OSM Data")
def enrich_osm_task(
    osm_pbf: PathLike,
    geoid: Optional[str] = None,
    buffer_dist_mi: Optional[int] = None,
    network_type: str = "driving",
) -> EnrichedOsmNetworkDataWithFullMetadata:
    """
    Prefect task to create or load an enriched OSMnx graph.

    Wraps the common.osm.enrich.create_enriched_osmnx_graph_for_region function.
    Assumes this function handles its own caching logic internally if needed.

    Args:
        osm_pbf (PathLike): Path to the input OpenStreetMap PBF file.
        geoid (Optional[str]): Geographic identifier for the region. If None, it's
            inferred from the `osm_pbf` filename. Defaults to None.
        buffer_dist_mi (Optional[int]): Buffer distance in miles around the region.
            If None, it's inferred from the `osm_pbf` filename. Defaults to None.
        network_type (str): The type of network to extract (e.g., "driving").
            Defaults to "driving".

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
        enriched_osm = create_full_enriched_osmnx_graph_for_region(
            osm_pbf=osm_pbf,
            geoid=geoid,
            buffer_dist_mi=buffer_dist_mi,
            network_type=network_type,
        )

        region_name = enriched_osm["region_name"]

        logger.debug(f"Graph loaded successfully for region: {region_name}")

        return enriched_osm

    except Exception as e:
        logger.error(f"Failed to create/load graph from {osm_pbf}: {e}", exc_info=True)
        raise


@task(name="Create bridge spans GeoDataFrame")
def create_bridge_spans_gdf_task(
    edges_gdf: DataFrame[EdgesSchema],
) -> DataFrame[BridgeSpanSchema]:
    """
    Prefect task to create a GeoDataFrame of bridge spans in the edges_gdf

    Wraps the common.osm.enrich.create_bridge_spans_gdf function.

    Parameters:
        edges_gdf (DataFrame[EdgesSchema]): GeoDataFrame of edges from `clean`, with columns:
            - geometry: LineString representing the edge.
            - osm_way_along_info: List of dicts with `start_coord_idx`, `end_coord_idx`,
              `bridge_tag`, `osmid`, `osm_nodes`, etc.
            - u, v, key: Edge identifiers.

    Returns:
        DataFrame[BridgeSpanSchema]: A new GeoDataFrame with one row per bridge span, containing:
            - geometry: LineString of the bridge span segment.
            - start_coord_idx, end_coord_idx: Indices in the parent edge’s geometry.
            - start_ratio_along, end_ratio_along: Ratios along the parent edge’s length.
            - osmids: List of OSM way IDs in the span.
            - osm_nodes: List of OSM node IDs along the span.
            - u, v, key: Parent edge identifiers (in the index with span_idx).
    """
    logger = get_run_logger()

    logger.debug("Starting OSM create_bridge_spans_gdf")

    bridge_spans_gdf = create_bridge_spans_gdf(edges_gdf=edges_gdf)

    logger.debug("Created OSM bridge spans")

    return bridge_spans_gdf


@task(name="Create nonbridge spans GeoDataFrame")
def create_nonbridge_spans_gdf_task(
    edges_gdf: DataFrame[EdgesSchema],
) -> DataFrame[NonBridgeSpanSchema]:
    """
    Prefect task to create a GeoDataFrame of nonbridge spans in the edges_gdf

    Wraps the common.osm.enrich.create_nonbridge_spans_gdf function.

    Parameters:
        edges_gdf (DataFrame[EdgesSchema]): GeoDataFrame of edges from `clean`, with columns:
            - geometry: LineString representing the edge.
            - osm_way_along_info: List of dicts with `start_coord_idx`, `end_coord_idx`,
              `bridge_tag`, `osmid`, `osm_nodes`, etc.
            - u, v, key: Edge identifiers.

    Returns:
        DataFrame[NonBridgeSpanSchema]: A new GeoDataFrame with one row per non-bridge span, containing:
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


@task(name="Create combined road spans GeoDataFrame")
def create_combined_road_spans_task(
    edges_gdf: DataFrame[EdgesSchema],
) -> DataFrame[CombinedRoadSpansSchema]:
    """
    Wraps the `create_combined_road_spans` function to combine non-bridge and
    bridge GeoDataFrames, add a `_road_span_type_` column, and set a new
    combined index.
    """
    logger = get_run_logger()
    logger.info("Combining bridge and non-bridge spans.")

    road_spans_gdf = create_combined_road_spans(edges_gdf=edges_gdf)

    logger.info(f"Combined road spans. Index: {road_spans_gdf.index.names}")

    return road_spans_gdf


@task(name="Create enriched OSM GeoPackage")
def create_enriched_osm_geopackage(
    osm_pbf: PathLike,  #
    clean: bool = False,
) -> Path:
    osm_network_metadata = parse_osm_region_parameters(osm_pbf=osm_pbf)

    region_name = osm_network_metadata["region_name"]
    osm_version = osm_network_metadata["osm_version"]

    enriched_osm_fname = f"enriched-osm_{region_name}_{osm_version}.gpkg"
    enriched_osm_path = DEFAULT_OSM_EXTRACTS_DIR / enriched_osm_fname

    if enriched_osm_path.exists():
        return enriched_osm_path

    enriched_osm = enrich_osm_task(osm_pbf=osm_pbf)

    nodes_gdf = enriched_osm["nodes_gdf"]
    edges_gdf = enriched_osm["edges_gdf"]

    cast(gpd.GeoDataFrame, nodes_gdf).to_file(
        enriched_osm_path,  #
        engine="pyogrio",
        layer="osmnx_simplified_nodes",
    )

    cast(gpd.GeoDataFrame, edges_gdf).to_file(
        enriched_osm_path,  #
        engine="pyogrio",
        layer="osmnx_simplified_edges",
    )

    enriched_osm_path.chmod(stat.S_IREAD | stat.S_IRGRP | stat.S_IROTH)

    return enriched_osm_path


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
        type=str,  # type: ignore
        required=False,
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

    extract_parser.add_argument(
        "--osm-ways-filter",
        type=str.upper,
        choices=[e.name for e in OSMWaysFilter],
        default=OSMWaysFilter.ALL.name,
        help=f"Filter for OSM ways based on highway tags. Defaults to ALL. Choices: {[e.name for e in OSMWaysFilter]}",
    )

    args = parser.parse_args()

    if args.command == "extract":
        ways_filter_enum = OSMWaysFilter[args.osm_ways_filter]

        osm_pbf = extract_osm_region_road_network_task.fn(
            base_osm_pbf=args.base_osm_pbf,
            geoid=args.geoid,
            buffer_dist_mi=args.buffer_dist_mi,
            ways_filter=ways_filter_enum,
        )

        print(osm_pbf)
    else:
        raise ValueError("Unsupported command")
