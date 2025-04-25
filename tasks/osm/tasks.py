import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

from prefect import task

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
    osm_pbf_path: str,  #
) -> Dict[str, Any]:
    """
    Loads and enriches OSM data. Returns dict like 'd' in notebook.
    Keys should include 'region_name', 'edges_gdf', 'buffered_region_gdf'.
    """
    logger = logging.getLogger(__name__)

    logger.info(f"Starting OSM enrichment for: {osm_pbf_path}")

    osm_path = Path(osm_pbf_path)

    if not osm_path.exists():
        logger.error(f"Input OSM PBF file not found: {osm_pbf_path}")
        raise FileNotFoundError(f"Input OSM PBF file not found: {osm_pbf_path}")

    enriched_data = create_enriched_osmnx_graph_for_region(
        osm_pbf=osm_path,  #
        include_base_osm_data=True,
    )

    logger.info(f"OSM enrichment complete for region: {enriched_data['region_name']}")

    return enriched_data  # type: ignore[no-any-return]
