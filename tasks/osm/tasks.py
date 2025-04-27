import logging
import os
from os import PathLike
from pathlib import Path
from typing import Any, Dict, Optional

from prefect import get_run_logger, task

from common.osm.enrich import create_enriched_osmnx_graph_for_region
from common.osm.extract import (
    DEFAULT_OSM_EXTRACT_BUFFER_DIST_MI,
    DEFAULT_OSM_EXTRACTS_DIR,
    create_osm_region_road_network_extract_pbf,
)


@task(name="Create OSM PBF Extract for Region")
def create_osm_region_road_network_extract_task(
    base_osm_pbf: os.PathLike,  #
    geoid: str,
    buffer_dist_mi: Optional[int] = DEFAULT_OSM_EXTRACT_BUFFER_DIST_MI,
    output_dir: Optional[os.PathLike] = DEFAULT_OSM_EXTRACTS_DIR,
    clean: bool = False,
) -> Dict[str, Any]:
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

    return osm_pbf


@task(name="Enrich OSM Data")
def enrich_osm_task(
    osm_pbf: PathLike,  #
) -> Dict[str, Any]:
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
