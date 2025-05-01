# tasks/osm/__init__.py

from .tasks import enrich_osm_task, extract_osm_region_road_network_task

__all__ = [
    "extract_osm_region_road_network_task",  #
    "enrich_osm_task",
]
