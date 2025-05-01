# In your imported module (e.g., your_module.py)
import logging
from operator import itemgetter
from typing import List, Optional, Set, Tuple

import geopandas as gpd
import networkx as nx
import osmnx as ox
import pandas as pd
from tqdm import tqdm

from common.osm.analyze import Direction, find_nodes_along_road
from common.osm.enrich import RoadClass, get_best_edge_name
from common.utils.logging import timer

logger = logging.getLogger(__name__)

ARTIFICIAL_BARRICADE_TRAVEL_TIME = 1_000_000


# class DetourInfoDict(TypedDict):
#     base_path_edge_ids: str
#     base_travel_time_sec: float
#     base_length_miles: float
#     detour_path_edge_ids: str
#     detour_travel_time_sec: float
#     detour_length_miles: float
#     difference_sec: float
#     difference_miles: float
#     difference_ratio: float
#     origin_node_id: int
#     origin_node_upstream_count: int
#     origin_intersection_name: str
#     origin_lon: float
#     origin_lat: float
#     destination_node_id: int
#     destination_node_downstream_count: int
#     destination_intersection_name: str
#     destination_lon: float
#     destination_lat: float
#
# DetourInfoDataFrame: TypeAlias = DataFrame[DetourInfoDict]


def identify_major_roads(
    roadways_gdf: gpd.GeoDataFrame,  #
    max_roadclass: Optional[int] = RoadClass.tertiary.value,
) -> Set[Tuple[int, int, int]]:
    """
    Identifies major road segments within the region.

    Parameters:
        roadways_gdf (gpd.GeoDataFrame): GeoDataFrame with road segment details.
        max_road_class (int): The maximum roadclass to include. Default is 4 (tertiary roads).

    Returns:
        Set[Tuple]: A set of indices representing major road segments.
    """
    with timer("Identifying major roads"):
        required_columns = ["roadclass", "_intersects_region_"]
        missing_columns = [
            col for col in required_columns if col not in roadways_gdf.columns
        ]

        if missing_columns:
            raise ValueError(
                f"Missing required columns in edges GeoDataFrame: {missing_columns}"
            )

        major_roads = roadways_gdf[
            roadways_gdf["roadclass"].between(0, max_roadclass, inclusive="both")
            & roadways_gdf["_intersects_region_"]
        ]

        edge_ids = set(major_roads.index)

        logger.info(f"Identified {len(edge_ids)} major road segments for analysis")

        return edge_ids


def compute_detour_cost(
    g: nx.MultiDiGraph,  #
    edge_id: Tuple[int, int, int],
    origin_node_id: int,
    destination_node_id: int,
):
    # To create an artificial barricade, we set the edge travel_time to 1M seconds.
    # We store the original travel time in this variable so we can restore it after.

    original_travel_time = g.get_edge_data(*edge_id)["travel_time"]

    # For loops, origin == destination. It doesn't make sense to do a detour.
    base_path_nodes = ox.shortest_path(
        G=g,  #
        orig=origin_node_id,
        dest=destination_node_id,
        weight="travel_time",
    )

    base_path_edge_ids = [
        (u, v, 0) for u, v in zip(base_path_nodes[:-1], base_path_nodes[1:])
    ]

    # CONSIDER: Should be throw if the edge_id is not in the base path?
    #           We will be able to identify these cases because the detour cost will be zero.
    # if edge_id not in base_path_edge_ids:
    #     raise BaseException('edge_id not in the base path')

    base_edge_data = [
        itemgetter("travel_time", "length_mi")(g.get_edge_data(*uv_pair, 0))
        for uv_pair in base_path_edge_ids
    ]

    base_travel_time_s = sum([d[0] for d in base_edge_data])
    base_length_miles = sum([d[1] for d in base_edge_data])

    # Using try/finally so we are certain to reset the travel_time for the edge.
    try:
        ## Set the edges travel time to 1,000,00 seconds to create an artificial barricade.
        g.get_edge_data(*edge_id)["travel_time"] = ARTIFICIAL_BARRICADE_TRAVEL_TIME

        detour_path_nodes = ox.shortest_path(
            G=g,  #
            orig=origin_node_id,
            dest=destination_node_id,
            weight="travel_time",
        )
        detour_path_edge_ids = [
            (u, v, 0) for u, v in zip(detour_path_nodes[:-1], detour_path_nodes[1:])
        ]

        detour_edge_data = [
            itemgetter("travel_time", "length_mi")(g.get_edge_data(*uv_pair, 0))
            for uv_pair in detour_path_edge_ids
        ]

        detour_travel_time_s = sum([d[0] for d in detour_edge_data])
        detour_length_miles = sum([d[1] for d in detour_edge_data])

        # Through the barricade was the only route.
        if (not detour_travel_time_s) or (
            detour_travel_time_s >= ARTIFICIAL_BARRICADE_TRAVEL_TIME
        ):
            detour_path_edge_ids = None
            detour_travel_time_s = None
            detour_length_miles = None

    finally:
        # Reset the travel time to original value.
        g.get_edge_data(*edge_id)["travel_time"] = original_travel_time

    # neither_is_none = base_path_edge_ids is not None and detour_path_edge_ids is not None
    neither_is_none = base_path_edge_ids and detour_path_edge_ids

    difference_sec = (
        round(detour_travel_time_s - base_travel_time_s, 3) if neither_is_none else None
    )
    difference_miles = (
        round(detour_length_miles - base_length_miles, 3) if neither_is_none else None
    )
    difference_ratio = (
        round(detour_travel_time_s / base_travel_time_s, 3)
        if (neither_is_none and base_travel_time_s > 0)
        else None
    )

    return {
        "base_path_edge_ids": base_path_edge_ids,
        "base_travel_time_sec": base_travel_time_s and round(base_travel_time_s, 3),
        "base_length_miles": base_length_miles and round(base_length_miles, 3),
        "detour_path_edge_ids": detour_path_edge_ids,
        "detour_travel_time_sec": (
            detour_travel_time_s and round(detour_travel_time_s, 3)
        ),
        "detour_length_miles": (detour_length_miles and round(detour_length_miles, 3)),
        "difference_sec": difference_sec,
        "difference_miles": difference_miles,
        "difference_ratio": difference_ratio,
    }


def compute_redundancy_for_edges(
    g: nx.MultiDiGraph,  #
    edge_ids: Optional[List[Tuple[int, int, int]]],
    exclude_nonpaved_roads=True,
) -> pd.DataFrame:
    # Calculate the detour costs for each surface_roadways_edge
    index = []
    data = []

    if edge_ids is None:
        edge_ids = [
            e[:3] for e in g.edges(keys=True, data="_intersects_region_") if e[3]
        ]
    elif isinstance(edge_ids, tuple):
        edge_ids = [edge_ids]

    try:
        logger.info(f"Computing redundancy for {len(edge_ids)} edges.")

        if exclude_nonpaved_roads:
            # Per the Utah Methodology, do not reroute traffic onto nonpaved roads.
            original_travel_times = nx.get_edge_attributes(g, "travel_time")

            for u, v, d in g.edges(data=True):
                if not d["is_paved"]:
                    logger.debug(
                        f"UNPAVED ROAD: {d['road_name']} is_paved={d['is_paved']}"
                    )
                    d["travel_time"] = ARTIFICIAL_BARRICADE_TRAVEL_TIME
            else:
                original_travel_times = None

        for edge_id in tqdm(edge_ids):
            logger.debug(edge_id)
            upstream_nodes = find_nodes_along_road(g, edge_id, Direction.UPSTREAM)
            origin_node_id = upstream_nodes[-1] if upstream_nodes else edge_id[0]

            downstream_nodes = find_nodes_along_road(g, edge_id, Direction.DOWNSTREAM)
            destination_node_id = (
                downstream_nodes[-1] if downstream_nodes else edge_id[1]
            )

            d = compute_detour_cost(
                g=g,
                edge_id=edge_id,
                origin_node_id=origin_node_id,
                destination_node_id=destination_node_id,
            )

            o_cross_rd_0 = get_best_edge_name(g, g.edges(origin_node_id))
            o_cross_rd_1 = get_best_edge_name(g, g.edges(origin_node_id), o_cross_rd_0)

            d["origin_node_id"] = origin_node_id
            d["origin_node_upstream_count"] = len(upstream_nodes)
            d["origin_intersection_name"] = f"{o_cross_rd_0} AND {o_cross_rd_1}"

            d["origin_lon"] = g.nodes[origin_node_id]["x"]
            d["origin_lat"] = g.nodes[origin_node_id]["y"]

            d_cross_rd_0 = get_best_edge_name(g, g.edges(destination_node_id))
            d_cross_rd_1 = get_best_edge_name(
                g, g.edges(destination_node_id), d_cross_rd_0
            )

            d["destination_node_id"] = destination_node_id
            d["destination_node_downstream_count"] = len(downstream_nodes)
            d["destination_intersection_name"] = f"{d_cross_rd_0} AND {d_cross_rd_1}"

            d["destination_lon"] = g.nodes[destination_node_id]["x"]
            d["destination_lat"] = g.nodes[destination_node_id]["y"]

            index.append(edge_id)
            data.append(d)

        logger.info(f"Completed redundancy computation for {len(edge_ids)} edges.")
    finally:
        if original_travel_times is not None:
            nx.set_edge_attributes(g, original_travel_times)

    return pd.DataFrame(
        data=data,
        # NOTE: Using a MultiIndex here because df.loc[] assumes a tuple is a multi-index. and OSMnx edge ids are tuples.
        index=pd.MultiIndex.from_tuples(index, names=["u", "v", "key"]),
    )
