# https://geoffboeing.com/share/osmnx-paper.pdf
# https://geoffboeing.com/2016/11/osmnx-python-street-networks/

import logging
import math
import os
import pickle
from collections.abc import Iterable
from enum import Enum
from os import PathLike
from pprint import pprint
from typing import List, Literal, Optional, Tuple, TypeAlias, TypedDict, Union

import geopandas as gpd
import networkx as nx
import osmnx as ox
import pandas as pd
import pyproj
import pyrosm
import shapely
from geopandas import GeoDataFrame
from networkx import MultiDiGraph
from shapely import LineString
from tqdm import tqdm

from common.constants import DEFAULT_BUFFER_DIST_MI, MILES_PER_METER
from common.osm.extract import get_osm_version_from_pbf_filename
from common.us_census.tiger.utils import (
    get_buffered_region_gdf,
    get_geography_region_name,
    get_region_boundary_gdf,
    parse_geography_region_name,
)

logger = logging.getLogger(__name__)

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
OSMNX_PICKLE_DIR = os.path.abspath(
    os.path.join(THIS_DIR, "../../data/pickles/osmnx/enriched-osm")
)

ENRICH_VERSION = "0.1.1"

OSMEdgeID: TypeAlias = Tuple[int, int, int]  # (u, v, key)


class RoadClass(Enum):
    """
    RoadClass enum defines road categories as specified in the OpenLR standard.

    Each road category is assigned a numeric value that indicates its
    relative importance within a road network. Lower numbers indicate
    higher-order roads (e.g., 'motorway' is 0) while higher numbers denote
    lower-order roads (e.g., 'residential' is 5). Notably, 'living_street' is
    assigned 5.5 to distinguish it from 'residential' (5) and 'unclassified' (6).

    For more details, see:
      - OpenLR Specification: https://www.openlr.org/
      - SharedStreets JS source: https://github.com/sharedstreets/sharedstreets-js/blob/98f8b78d0107046ed2ac1f681cff11eb5a356474/src/index.ts#L600-L613
    """

    motorway = 0
    trunk = 1
    primary = 2
    secondary = 3
    tertiary = 4
    residential = 5
    living_street = 5.5  # FIXME: In highway_type_analysis_for_way, roadclass is the Floor of the value.
    unclassified = 6
    service = 7
    other = 8


class EnrichedOsmNetworkData(TypedDict, total=True):
    g: nx.MultiDiGraph
    nodes_gdf: gpd.GeoDataFrame
    edges_gdf: gpd.GeoDataFrame


class EnrichedOsmNetworkDataWithRegions(EnrichedOsmNetworkData, total=True):
    region_gdf: gpd.GeoDataFrame
    buffered_region_gdf: gpd.GeoDataFrame


class OsmNetworkMetadata(TypedDict, total=True):
    osm_pbf: PathLike
    geoid: str
    buffer_dist_mi: int
    region_name: str
    osm_version: str


class EnrichedOsmNetworkDataWithFullMetadata(
    EnrichedOsmNetworkDataWithRegions, OsmNetworkMetadata, total=True
):
    pass


# For OSMnx add_edge_speeds
DEFAULT_HWY_SPEEDS = {
    "motorway": 100,
    "trunk": 100,
    "primary": 60,
    "secondary": 60,
    "tertiary": 60,
    "residential": 50,
    "unclassified": 40,
    "living_street": 35,
    "service": 30,
}

BridgeTag = Literal[
    "yes",
    "aqueduct",
    "boardwalk",
    "cable-stayed",
    "cantilever",
    "covered",
    "low_water_crossing",
    "movable",
    "trestle",
    "viaduct",
]


def ensure_scalar(value):
    """Returns the first element if value is a list, otherwise returns value itself."""

    if isinstance(value, list):
        return value[0] if value else None

    return value


def normalize_bridge_value(bridge) -> BridgeTag | None:
    """
    Normalize the bridge tag value from OSM data.

    In OSM, any value other than None implies that the way is considered a bridge.
    However, due to the crowd-sourced nature of OSM, mappers use a wide, unpredictable
    range of values for the bridge tag. As a result, this function makes a best-effort
    attempt to normalize these values into a predictable set of outputs.

    If the input is not a list, the function returns the value directly—unless the
    value is "no", in which case it returns None. For list inputs, it filters out any
    entries that are "no" or None. If any positive (non-"no") value remains, it returns
    "yes" if "yes" is present; otherwise, it returns the first positive value. If no
    positive value is found, it returns None.

    Parameters:
        bridge (Union[str, List[str]]): The raw bridge value(s) from OSM data. This can
            be either a single value (as a string) or a list of strings.

    Returns:
        BridgeTag or None: A normalized bridge tag if a positive value exists, or None
            if the value is "no" or if no positive value is present.
    """
    if not isinstance(bridge, list):
        return bridge if bridge != "no" else None

    positive_values = [x for x in bridge if x not in ["no", None]]

    if not positive_values:
        return None

    return "yes" if "yes" in positive_values else positive_values[0]


def is_surface_paved(surface: Union[str, List[str], None]) -> bool:
    """
    Determine if a given surface type is considered paved according to
    OpenStreetMap (OSM) tagging conventions.

    In OSM, the 'surface' tag describes the physical surface of roads,
    footpaths, and other features. Surfaces are broadly categorized into
    'paved' and 'unpaved' types. This function checks if the provided
    surface value corresponds to a paved surface.

    Parameters:
        surface (str | None): The surface type to evaluate. Examples include
            'asphalt', 'gravel', 'dirt', etc. A value of None indicates that
            the surface type is unspecified.

    Returns:
        bool: True if the surface is considered paved according to OSM
        conventions, False otherwise.

    Notes:
        - Paved surfaces typically include materials like 'asphalt',
          'concrete', 'paving_stones', and 'chipseal'.
        - Unpaved surfaces include materials such as 'gravel', 'dirt',
          'sand', 'mud', and 'earth'.
        - The function assumes that any surface not explicitly listed as
          unpaved is considered paved.
          SEE: https://wiki.openstreetmap.org/wiki/Key:surface#Surface_for_motor_roads

    """
    unpaved_tags = {
        "compacted",
        "dirt",
        "dirt/sand",
        "earth",
        "fine_gravel",
        "gravel",
        "mud",
        "sand",
        "unpaved",
    }

    if surface is None:
        return True

    if isinstance(surface, str):
        return surface.lower() not in unpaved_tags

    if isinstance(surface, list):
        normalized_surfaces = {s.lower() for s in surface if s is not None}
        return not normalized_surfaces.intersection(unpaved_tags)

    raise TypeError("Invalid input type for 'surface'. Expected str, list, or None.")


def compute_length_mi(
    geom: shapely.geometry.base.BaseGeometry,
    geod: pyproj.Geod,
    precision: Optional[int] = 3,
) -> float:
    """
    Compute the geodesic length of a Shapely geometry in miles.

    This function calculates the length of the given geometry using geodesic
    measurements, which account for the Earth's curvature. The length is
    returned in miles, rounded to the specified precision.

    Parameters:
        geom (shapely.geometry.base.BaseGeometry): The Shapely geometry object
            for which the length is to be computed. Supported geometry types
            include LineString and Polygon.
        geod (pyproj.Geod): A pyproj Geod object initialized with a specific
            ellipsoid to perform geodesic calculations.
        precision (int, optional): The number of decimal places to which the
            resulting length should be rounded. Defaults to 3.

    Returns:
        float: The geodesic length of the geometry in miles, rounded to the
        specified precision.

    Raises:
        TypeError: If 'geom' is not a Shapely geometry object.
        AttributeError: If the 'geometry_length' method is not available in
            the provided 'geod' object.

    Notes:
        - Ensure that the input geometry's coordinates are in geographic
          coordinates (longitude, latitude) for accurate geodesic calculations.
        - The 'geometry_length' method was introduced in pyproj version 2.3.0.
          Using an older version may result in an AttributeError.
    """
    if not isinstance(geom, shapely.geometry.base.BaseGeometry):
        raise TypeError("The 'geom' parameter must be a Shapely geometry object.")

    try:
        len_m = geod.geometry_length(geom)
    except AttributeError:
        raise AttributeError(
            "The 'geometry_length' method is not available. Ensure that "
            "the 'geod' object is an instance of pyproj.Geod and that you "
            "are using pyproj version 2.3.0 or later."
        )

    return round(len_m * MILES_PER_METER, precision)


# https://github.com/sharedstreets/sharedstreets-builder/blob/a554983e96010d32b71d7d23504fa88c6fbbad10/src/main/java/io/sharedstreets/tools/builder/osm/model/Way.java#L61-L94
# https://www.openlr.org/
def highway_type_analysis_for_way(way: dict):
    """
    Analyzes the highway type of an OSM way (edge) as generated by the OSMnx
    simplify method. This function determines a road classification based on
    the OpenLR specification by examining the 'highway' attribute of the input
    dictionary. Because OSMnx may represent these attributes as lists, the
    function normalizes them into lists for processing.

    The function processes each highway type by:
      - Stripping and lowering the string.
      - Adjusting the classification if the highway type ends with 'link'
        (by adding a penalty of 0.25).
      - Comparing the resulting roadclass values to determine the minimum (best)
        and maximum (worst) road types.
      - For 'service' highways, it also examines the 'service' attribute to
        differentiate between types (e.g., parking or driveway).

    Decimal parts in the roadclass values are used for ranking and are later
    dropped using math.floor.

    If no valid highway type is found, the function returns a dictionary with
    all values set to None.

    Parameters:
        way (dict): An OSM edge dictionary (from OSMnx simplify) that contains a
            'highway' key (and possibly a 'service' key). Attributes may be lists.

    Returns:
        dict: A dictionary with the following keys:
            - "roadclass": The minimum (best) roadclass (as an integer) found.
            - "edge_min_roadclass": The minimum roadclass encountered for the edge.
            - "edge_max_roadclass": The maximum roadclass encountered for the edge.
            - "roadtype": The highway type string corresponding to the minimum
              roadclass.
            - "edge_highest_highway_type": The highway type associated with the
              minimum roadclass.
            - "edge_lowest_highway_type": The highway type associated with the
              maximum roadclass.
    """

    highway = way["highway"]

    if not isinstance(highway, list):
        highway = [highway]

    # NOTE: Decimal parts are used in ranking highway types, but later dropped using math.floor.
    min_roadclass = 9
    max_roadclass = -1

    highest_highway_type = None
    lowest_highway_type = None

    for hwy in highway:
        if not isinstance(hwy, str):
            continue

        hwy = hwy.strip().lower()

        is_link = hwy.endswith("link")

        cur_roadclass = None

        if hwy.startswith("motorway"):
            cur_roadclass = RoadClass.motorway.value
        elif hwy.startswith("trunk"):
            cur_roadclass = RoadClass.trunk.value
        elif hwy.startswith("primary"):
            cur_roadclass = RoadClass.primary.value
        elif hwy.startswith("secondary"):
            cur_roadclass = RoadClass.secondary.value
        elif hwy.startswith("tertiary"):
            cur_roadclass = RoadClass.tertiary.value
        elif hwy.startswith("residential"):
            cur_roadclass = RoadClass.residential.value
        elif hwy.startswith("living_street"):
            cur_roadclass = RoadClass.living_street.value
        elif hwy.startswith("unclassified"):
            cur_roadclass = RoadClass.unclassified.value
        elif hwy.startswith("service"):
            service = way["service"]

            if not isinstance(service, list):
                service = [service]

            for svc in service:
                if not isinstance(svc, str):
                    cur_roadclass = RoadClass.service.value
                elif (
                    svc.startswith("parking")
                    or svc.startswith("driveway")
                    or svc.startswith("drive-through")
                ):
                    cur_roadclass = RoadClass.other.value
                else:
                    cur_roadclass = RoadClass.service.value

        if cur_roadclass is not None:
            # motorway should be considered higher highway type than motorway-link
            # NOTE: Higher ranked roadways have lower roadclass numbers.
            if is_link:
                cur_roadclass += 0.25

            if cur_roadclass < min_roadclass:
                min_roadclass = cur_roadclass
                highest_highway_type = hwy

            if cur_roadclass > max_roadclass:
                max_roadclass = cur_roadclass
                lowest_highway_type = hwy

    if highest_highway_type is None:
        return {
            "roadclass": None,
            "edge_min_roadclass": None,
            "edge_max_roadclass": None,
            "roadtype": None,
            "edge_highest_highway_type": None,
            "edge_lowest_highway_type": None,
        }

    min_roadclass = math.floor(min_roadclass)
    max_roadclass = math.floor(max_roadclass)

    return {
        # Use the minimum roadclass if there were many.
        "roadclass": min_roadclass,
        "edge_min_roadclass": min_roadclass,
        "edge_max_roadclass": max_roadclass,
        # Use the minimum roadclass if there were many.
        "roadtype": highest_highway_type,
        "edge_highest_highway_type": highest_highway_type,
        "edge_lowest_highway_type": lowest_highway_type,
    }


# From the list of edge_ids, get the most important edge's route number and street/route name.
def get_best_edge_name(
    g: MultiDiGraph,  #
    edge_ids: List[str],  #
    to_omit_edge_full_name: Optional[str] = None,  #
):
    """
    This function selects the best (i.e., most preferred) road name from a list of OSM
    ways (identified by edge_ids) by evaluating several attributes from each edge’s data.
    The selection process works as follows:

    1. For each edge_id, the function retrieves edge data (which may include multiple
    parallel edges) and extracts key attributes:
    - `roadclass`: a numeric indicator where a lower value corresponds to a higher
        priority in the road network.
    - `ref`: typically the official road reference (e.g., highway number).
    - `name`: the common or local name of the road.

    2. If the `ref` or `name` is given as a list (which can occur in because of OSMnx simplification),
    the function selects the first non-empty element, ensuring that the corresponding entries match.

    3. The function then constructs a composite road name (`edge_full_name`) by:
    - Using `ref` as the primary component.
    - Combining `ref` and `name` (with a "/" separator) if both are available.
    - Falling back to `name` if `ref` is missing.

    4. A penalty is computed for each edge candidate to quantify its desirability:
    - The base penalty is the `roadclass` (with lower values being better).
    - If `ref` is missing, an additional penalty of 1 is added.
    - If `name` is missing, an additional penalty of 0.5 is added.
    - If both `ref` and `name` are missing (i.e., no valid road name), a steep
        penalty of 100 is added, and a placeholder using the edge's highest highway
        type is used.

    5. An optional parameter, `to_omit_edge_full_name`, allows the caller to exclude a
    specific road name from the selection.

    6. After processing all provided edge_ids, the function sorts the candidates by their
    penalty values and returns the road name with the lowest penalty, ensuring that the
    most appropriate road name is chosen.

    This approach balances the completeness of the road naming information with the
    inherent quality (and hierarchy) indicated by the road class and available name/ref
    data.
    """

    if not isinstance(edge_ids, list):
        if isinstance(edge_ids, tuple):
            edge_ids = [edge_ids]
        elif isinstance(edge_ids, Iterable):  # For NetworkX Edge Views
            edge_ids = list(edge_ids)

    candidates = []

    for edge_id in edge_ids:
        edges_data = g.get_edge_data(*edge_id)
        edges_data = (
            edges_data.values() if hasattr(edges_data, "values") else [edges_data]
        )

        for edge_data in edges_data:
            roadclass = edge_data["edge_min_roadclass"]
            ref = edge_data["ref"]
            name = edge_data["name"]

            # TODO: Inspect the OSMnx source code for the logic used to create ref/name lists.
            #       Make sure they are parallel lists so that the ref always corresponds to the name.
            if isinstance(ref, list):
                for i, r in enumerate(ref):
                    if r:
                        ref = r

                        if isinstance(name, list) and i in name:
                            name = name[i]

            if isinstance(name, list):
                non_none = [n for n in name if n]
                name = non_none[0] if non_none else None

            edge_full_name = None

            # Prefer lowest roadclass. NOTE: Lower roadclass corresponds to higher road network level.
            penalty = roadclass  # Lowest penalty wins.

            # Prefer ref to name: not having a ref incurs higher penalty than not having a name.
            if not ref:
                penalty += 1
            else:
                edge_full_name = ref

            if not name:
                penalty += 0.5
            else:
                edge_full_name = f"{edge_full_name}/{name}" if edge_full_name else name

            # Having neither a ref nor a name incurs a steep penalty.
            # Any ref and or name will beat this edge regardless of road class.
            # If no edges have ref or name, then lowest road class will win.
            if not edge_full_name:
                penalty += 100
                candidates.append(
                    (penalty, f"<{edge_data['edge_highest_highway_type']}>")
                )
            elif edge_full_name != to_omit_edge_full_name:
                candidates.append((penalty, edge_full_name))

    candidates.sort()

    return candidates[0][1] if candidates else None


# NOTE: Mutates the OSMnx Graph object.
def enrich_osmnx_graph(
    g: MultiDiGraph,  #
    hwy_speeds: Optional[dict] = DEFAULT_HWY_SPEEDS,
) -> None:
    """
    Enrich an OSMnx MultiDiGraph with additional node and edge attributes for
    routing and analysis.

    This function processes a given MultiDiGraph (typically generated by the
    OSMnx simplify method) by performing the following steps:

      1. Node attribute renaming:
         - Renames the "osmid" attribute to "osmid_original" to avoid issues
           with reindexing and exporting (e.g., to GPKG).

      2. Edge attribute processing:
         - Flattens the "maxspeed" attribute if it is a list.
         - Ensures the "ref" attribute exists; if not, it is set to None.
         - Updates edge attributes using highway_type_analysis_for_way to
           determine road classifications based on the OpenLR specification.
         - Processes the "bridge" attribute, converting lists to a single value
           (e.g., "yes" if a positive indicator exists).
         - Computes the edge geometry length in meters using a WGS84 ellipsoid,
           converts it to miles, and stores it in "length_mi".
         - Normalizes the "_intersects_region_" attribute to a boolean value.

      3. Road name assignment:
         - After processing all edges, assigns a "road_name" to each edge using
           get_best_edge_name.
         - Determines "from_name" and "to_name" from adjacent edges while
           omitting the primary road name.

      4. Speed and travel time estimation:
         - Adds edge speed limits using a predefined mapping of highway types
           to speeds.
         - Computes travel times for each edge using OSMnx's
           add_edge_travel_times function.

    Parameters:
        g (MultiDiGraph): An OSMnx graph representing a road network. Edges are
                          produced by the OSMnx simplify method and may have
                          attributes stored as lists.
        hwy_speeds (dict, optional): Custom highway speed mapping passed to OSMnx's add_edge_speeds.

    Returns:
        None

    Notes:
        - This function modifies the input graph in place.
        - Workarounds are applied to address known issues in OSMnx, such as
          duplicate "osmid" fields and edge attribute processing.
    """

    geod = pyproj.Geod(ellps="WGS84")
    precision = 6

    # WARNING: Re-indexing and exporting to GPKG throws the following error:
    #            ValueError: cannot insert osmid, already exists
    #
    # The following workaround is suggested by the library's author:
    #   https://github.com/gboeing/osmnx/issues/638#issuecomment-756948363
    for node, data in g.nodes(data=True):
        if "osmid" in data:
            data["osmid_original"] = data.pop("osmid")

    # NOTE: add_edge_speeds crashes without the following work around
    for u, v, d in g.edges(data=True):
        if "maxspeed" in d and isinstance(d["maxspeed"], list):
            d["maxspeed"] = ensure_scalar(d.get("maxspeed"))

        # For some OSM areas there are no refs defined.
        d["ref"] = d.get("ref")

        d.update(highway_type_analysis_for_way(d))

        d["bridge"] = normalize_bridge_value(d.get("bridge"))

        d["is_paved"] = is_surface_paved(d.get("surface"))

        d["length_mi"] = compute_length_mi(d["geometry"], geod, precision)

        d["_intersects_region_"] = (
            d["_intersects_region_"]
            if isinstance(d["_intersects_region_"], bool)
            else any(d["_intersects_region_"])
        )

    # Because get_best_name depends on properties added in highway_type_analysis_for_way,
    #   and because g.edges must access edges in a non-sequential manner,
    #   we must gather names after the above loop has run highway_type_analysis_for_way
    #   for every edge.
    for u, v, d in g.edges(data=True):
        road_name = get_best_edge_name(g, (u, v))

        d["road_name"] = road_name

        d["from_name"] = get_best_edge_name(
            g, list(g.edges(u)), to_omit_edge_full_name=road_name
        )

        d["to_name"] = get_best_edge_name(
            g, list(g.edges(v)), to_omit_edge_full_name=road_name
        )

    # https://osmnx.readthedocs.io/en/stable/user-reference.html#osmnx.routing.add_edge_speeds
    ox.add_edge_speeds(g, hwy_speeds=hwy_speeds)

    # https://osmnx.readthedocs.io/en/stable/user-reference.html#osmnx.routing.add_edge_travel_times
    ox.add_edge_travel_times(g)


def verify_osm_alignment(
    g: nx.MultiDiGraph,
    G: nx.MultiDiGraph,
):
    """
    Validate alignment between a simplified OSMnx graph and the original Pyrosm graph.

    Performed as the first post-processing step after OSMnx’s `simplify_graph`, this function
    ensures that each simplified edge’s geometry is a non-empty LineString, its `merged_edges`
    form a valid chain, and its geometry coordinates precisely match the original OSM node
    coordinates. Fixes an OSMnx bug where single-node-pair edges may have unreversed geometries
    by reversing them if needed. Designed for disaster planning, it enforces strict correctness
    to prevent data corruption, halting execution on any misalignment.

    Parameters:
        g (nx.MultiDiGraph): Simplified OSMnx graph from `simplify_graph` with `track_merged=True`,
            containing edge attributes like `geometry` (LineString) and optionally `merged_edges`.
        G (nx.MultiDiGraph): Original Pyrosm graph with OSM node coordinates (`x`, `y`) and edge data.

    Raises:
        AssertionError: If geometry is a empty LineString, `merged_edges` is NaN and the
            simplified edge `(u, v)` is not in `G`, merged edges don’t form a chain, endpoints
            mismatch, geometry coordinate count doesn’t align with `merged_edges`, or node
            coordinates don’t match geometry coordinates within 1e-12 tolerance. Any failure
            halts processing to ensure downstream spatial analysis integrity.

    Side Effects:
        - If `merged_edges` is NaN, sets it to `[(u, v)]` in `g`’s edge data, assuming an unsimplified
          single-segment edge from the original OSM way (two nodes).
        - If a single-node-pair edge’s geometry coordinates don’t match node coordinates, reverses
          the geometry using `shapely.reverse` and updates `g`’s edge geometry to align with OSM nodes.
        - These mutations enable subsequent post-processing steps to assume geometry-node alignment.

    Notes:
        - Assumes OSMnx simplification produces a LineString geometry where `len(coords) == len(merged_edges) + 1`.
        - Uses a 1e-12 tolerance for coordinate comparisons, reflecting high precision for EPSG:4326
          (unprojected lat/lon) coordinates from OSM data.
        - Addresses an OSMnx simplification bug where single edges may not be reversed correctly.
        - Prioritizes correctness over robustness, critical for disaster planning where spatial data
          integrity underpins life-saving decisions.
    """
    for u, v, key, data in g.edges(keys=True, data=True):
        assert (
            isinstance(data["geometry"], LineString) and not data["geometry"].is_empty
        ), "INVARIANT BROKEN: edge geometry must be non-empty LineString"

        merged_edges = data.get("merged_edges", None)

        # In some instances, merged_edges were NaN. in these cases we use the simplified segment's (u, v) pair.
        # NOTE: bool(math.nan) == True, but NaN != NaN
        if not merged_edges or merged_edges != merged_edges:
            # ASSUMPTION: If not simplified, then original OSM Way was comprised of ONLY 2 Nodes.
            assert G.has_edge(u, v), (
                "INVARIANT BROKEN: merged_edges=NaN and simplified edge's (u, v) not in Pyrosm MultiDiGraph"
            )

            merged_edges = [(u, v)]
            data["merged_edges"] = merged_edges

        is_single_node_pair_edge = len(merged_edges) == 1

        assert u == merged_edges[0][0], (
            "INVARIANT BROKEN: segment u != first merged_edge u"
        )
        assert v == merged_edges[-1][1], (
            "INVARIANT BROKEN: segment v != last merged_edge v"
        )

        assert all(
            [x[1] == y[0] for x, y in zip(merged_edges[:-1], merged_edges[1:])]
        ), "INVARIANT BROKEN: Merged edges to not form a chain"

        assert len(data["geometry"].coords) == len(merged_edges) + 1, (
            "INVARIANT BROKEN: num geometry coords != num merged_edges + 1"
        )

        node_seq = [u] + [e[1] for e in merged_edges]

        # OSMnx has a bug where lines with just two coordinates may not get reversed during simplification.
        num_attempts = 2 if is_single_node_pair_edge else 1

        for attempt_num in range(num_attempts):
            coords = data["geometry"].coords

            for node_id, coord in zip(node_seq, coords):
                node = G.nodes[node_id]

                node_x = node["x"]
                node_y = node["y"]

                coord_x, coord_y = coord

                try:
                    assert abs(node_x - coord_x) < 1e-12
                    assert abs(node_y - coord_y) < 1e-12
                except Exception as e:
                    if is_single_node_pair_edge and attempt_num == 0:
                        data["geometry"] = shapely.reverse(data["geometry"])
                        break  # break the node_id, coord loop so we can reset the coords
                    else:
                        print(u, v, key)
                        print("    ", node_id, node_x, node_y, coord_x, coord_y)
                        pprint(list(coords))
                        raise e


def clean_geometries(g: nx.MultiDiGraph, G: nx.MultiDiGraph):
    """
    Clean and annotate simplified OSMnx graph edges with OSM way information.

    Performed as the second post-processing step after `local_validate`, this function
    removes duplicate coordinates from edge geometries and annotates each edge with
    `osm_way_along_info`, detailing OSM ways (including bridges and tunnels) along the
    simplified path. Ensures correctness for disaster planning by validating OSM way
    alignment, preserving bridge data, and maintaining geometric integrity, halting on
    any inconsistency.

    Parameters:
        g (nx.MultiDiGraph): Simplified OSMnx graph post-`local_validate`, with edge
            attributes `geometry` (non-empty LineString), `length` (meters), `merged_edges`
            (list of (u, v) tuples), and `osmid` (scalar or list).
        G (nx.MultiDiGraph): Original Pyrosm graph with OSM edge data (e.g., `osmid`, `length`,
            `bridge`, `tunnel`).

    Raises:
        ValueError: If `clean` has already been run (detected by existing `osm_way_along_info`),
            or if start/end ratios fall outside [0, 1] beyond a 1e-6 tolerance.
        AssertionError: If initial geometry has fewer than 2 coordinates, no unique OSM way
            is found for a merged edge, bridges are dropped during cleaning, `osm_way_along_info`
            is empty for edges with length > 1e-6 meters, duplicate coordinates have length ≥ 1e-6,
            single-point geometry has length ≥ 1e-6, ratios or coordinate indices overlap, or
            cumulative length deviates from segment length beyond 1e-6 meters.

    Side Effects:
        - Updates `g`’s edge data with `osm_way_along_info`: a list of dicts containing
          `start_ratio_along`, `end_ratio_along`, `start_coord_idx`, `end_coord_idx`,
          `osmid`, `bridge_tag`, `tunnel_tag`, and `osm_nodes` for each OSM way along the edge.
        - Replaces `data["geometry"]` with a cleaned LineString omitting duplicate coordinates,
          or an empty LineString if reduced to one point with length < 1e-6 meters.

    Notes:
        - Assumes `local_validate` has ensured geometry-node alignment and non-empty LineStrings.
        - Prioritizes bridge-bearing OSM ways, falling back to the first bridge candidate if
          filtering by seen OSMIDs or last way fails, ensuring bridge preservation.
        - Rounds ratios to 15 decimal places and clamps to [0, 1] within 1e-6 tolerance, updating
          `osm_way_along_info` accordingly.
        - Allows empty `osm_way_along_info` and `LineString` for edges with length < 1e-6 meters,
          preserving MultiDiGraph topology (e.g., zero-length connectors).
        - Designed for disaster planning, enforcing strict correctness to ensure reliable spatial data.
    """
    for u, v, key, data in tqdm(g.edges(keys=True, data=True), leave=False):
        if data.get("osm_way_along_info"):
            raise ValueError("clean has already been run on this MultiDiGraph.")

        segment_length_m = data["length"]

        if segment_length_m == 0:
            coords = data["geometry"].coords
            distinct_coords = set(coords)

            assert data["geometry"].is_empty or len(distinct_coords) < 2, (
                "INVARIANT BROKEN: Geometry length == 0, but more than 1 distinct coord. "
                f"u={u}, v={v}, key={key}, coords={coords}"
            )

            data["geometry"] = LineString()
            data["osm_way_along_info"] = []
            continue

        merged_edges = data["merged_edges"]
        osmids = data["osmid"] if isinstance(data["osmid"], list) else [data["osmid"]]

        osm_way_along_info = []
        cumulative_length = 0.0
        cur_coord_idx = 0

        declared_osmids = set(osmids)
        seen_osmids = set()

        orig_coords = list(data["geometry"].coords)
        assert len(orig_coords) > 1, "No empty LineStrings to start."
        cleaned_coords = [orig_coords[0]]

        for edge_idx, (edge_u, edge_v) in enumerate(merged_edges):
            candidate_edges = G.get_edge_data(edge_u, edge_v).values()

            expected_edges = [
                e for e in candidate_edges if e["osmid"] in declared_osmids
            ]

            bridge_candidates = [
                e
                for e in expected_edges
                if isinstance(e.get("bridge"), str) and e["bridge"].lower() != "no"
            ]

            expected_edges = bridge_candidates or expected_edges

            if len(expected_edges) > 1:
                expected_edges = [
                    e for e in expected_edges if e["osmid"] in seen_osmids
                ]

            if len(expected_edges) > 1:
                expected_edges = [
                    e
                    for e in expected_edges
                    if osm_way_along_info
                    and e["osmid"] == osm_way_along_info[-1]["osmid"]
                ]

            if len(expected_edges) == 0:
                if bridge_candidates:
                    expected_edges = bridge_candidates[:1]
                else:
                    raise AssertionError(
                        "INVARIANT BROKEN: Unable to find an OSM Way ID for merged edge."
                    )
            elif len(expected_edges) > 1:
                raise AssertionError(
                    "INVARIANT BROKEN: Unable to find a definite OSM Way ID for merged edge."
                )

            edge = expected_edges[0]

            edge_osmid = edge["osmid"]
            seen_osmids.add(edge_osmid)

            edge_length = edge["length"]

            edge_ref = edge["ref"]
            edge_name = edge["name"]

            edge_bridge_tag = (
                edge["bridge"]
                if isinstance(edge.get("bridge"), str)
                and edge["bridge"].lower() != "no"
                else None
            )
            edge_tunnel_tag = (
                "yes"
                if isinstance(edge.get("tunnel"), str)
                and edge["tunnel"].lower() == "yes"
                else None
            )

            if not osm_way_along_info or osm_way_along_info[-1]["osmid"] != edge_osmid:
                cur_ratio_along = cumulative_length / segment_length_m

                osm_way_along_info.append(
                    {
                        "start_ratio_along": cur_ratio_along,
                        "end_ratio_along": cur_ratio_along,
                        "start_coord_idx": cur_coord_idx,
                        "end_coord_idx": cur_coord_idx,
                        "osmid": edge_osmid,
                        "ref": edge_ref,
                        "name": edge_name,
                        "bridge_tag": edge_bridge_tag,
                        "tunnel_tag": edge_tunnel_tag,
                        # TODO: Verify that the osm_nodes lists align with the cleaned coords.
                        "osm_nodes": [edge_u],
                        # "duplicate_osm_nodes": [],
                    }
                )

            # NOTE: OSM Nodes and coords are not in sync. Need to use start/end coord idxs.
            osm_way_along_info[-1]["osm_nodes"].append(edge_v)

            next_coord = orig_coords[edge_idx + 1]

            if cleaned_coords[-1] == next_coord:
                assert edge_length < 1e-6, (
                    f"Edge ({u}, {v}, {key}): Duplicate coordinate with non-zero length {edge_length}"
                )

                if isinstance(osm_way_along_info[-1]["osm_nodes"][-1], int):
                    osm_way_along_info[-1]["osm_nodes"][-1] = [
                        osm_way_along_info[-1]["osm_nodes"][-1],
                        edge_v,
                    ]
                else:
                    osm_way_along_info[-1]["osm_nodes"][-1].append(edge_v)

                # TODO: Create tuples of whatever length required to capture consecutive duplicate nodes.
                # osm_way_along_info[-1]["duplicate_osm_nodes"].append((edge_u, edge_v))

                continue

            cleaned_coords.append(next_coord)

            cumulative_length += edge_length
            cur_coord_idx += 1

            osm_way_along_info[-1]["end_ratio_along"] = (
                cumulative_length / segment_length_m
            )
            osm_way_along_info[-1]["end_coord_idx"] = cur_coord_idx

        pre_clean_bridge_osmids = {
            info["osmid"] for info in osm_way_along_info if info["bridge_tag"]
        }

        # Consider: It may be worth keeping this information.
        osm_way_along_info = [
            info
            for info in osm_way_along_info
            if info["start_coord_idx"] < info["end_coord_idx"]
        ]
        post_clean_bridge_osmids = {
            info["osmid"] for info in osm_way_along_info if info["bridge_tag"]
        }

        assert pre_clean_bridge_osmids == post_clean_bridge_osmids, (
            "INVARIANT BROKEN: Dropped bridges from OSM Ways along."
        )

        data["osm_way_along_info"] = osm_way_along_info

        if len(orig_coords) > len(cleaned_coords):
            if len(cleaned_coords) == 1:
                assert data["length"] < 1e-6, (
                    f"Edge ({u}, {v}, {key}): Single-point geometry with non-zero length {data['length']}"
                )
                data["geometry"] = LineString()
            else:
                data["geometry"] = LineString(cleaned_coords)

        if segment_length_m > 1e-6:
            assert len(osm_way_along_info) > 0, (
                "INVARIANT BROKEN: osm_way_along_info list is empty for non-zero length edge."
            )

        for bridge_loc in osm_way_along_info:
            bridge_loc["start_ratio_along"] = round(bridge_loc["start_ratio_along"], 15)
            bridge_loc["end_ratio_along"] = round(bridge_loc["end_ratio_along"], 15)

        start_ratio_along = (
            osm_way_along_info[0]["start_ratio_along"] if osm_way_along_info else 0
        )
        if start_ratio_along < 0:
            if start_ratio_along > -1e-6:
                start_ratio_along = 0
                osm_way_along_info[0]["start_ratio_along"] = start_ratio_along
            else:
                raise ValueError(f"Edge ({u}, {v}, {key}): start_ratio_along < 0")

        end_ratio_along = (
            osm_way_along_info[-1]["end_ratio_along"] if osm_way_along_info else 0
        )
        if end_ratio_along > 1:
            if end_ratio_along < 1 + 1e-6:
                end_ratio_along = 1
                osm_way_along_info[-1]["end_ratio_along"] = end_ratio_along
            else:
                raise ValueError(f"Edge ({u}, {v}, {key}): end_ratio_along > 1")

        if osm_way_along_info:
            assert all(
                info_a["end_ratio_along"] <= info_b["start_ratio_along"]
                for info_a, info_b in zip(
                    osm_way_along_info[:-1], osm_way_along_info[1:]
                )
            ), "INVARIANT BROKEN: Overlapping OSM Way ratios along"
            assert all(
                info_a["end_coord_idx"] <= info_b["start_coord_idx"]
                for info_a, info_b in zip(
                    osm_way_along_info[:-1], osm_way_along_info[1:]
                )
            ), "INVARIANT BROKEN: Overlapping bridge ratios along"

        assert abs(cumulative_length - segment_length_m) <= 1e-6, (
            f"Length mismatch for edge ({u}, {v}, {key}): "
            f"cumulative length = {cumulative_length}, segment length = {data['length']}"
        )


def split_into_simple_segments(edges_gdf: gpd.GeoDataFrame):
    """
    Yield simple, non-self-intersecting segment ranges for edge geometries in a GeoDataFrame.

    Performed as a utility function after `clean`, this generator analyzes each edge’s
    `geometry` (a LineString) from `edges_gdf` and yields tuples of (u, v, key, ranges)
    where `ranges` are the coordinate index ranges of simple segments. Designed for
    on-demand spatial analysis in disaster planning, ensuring correctness without
    modifying the input data.

    Parameters:
        edges_gdf (gpd.GeoDataFrame): GeoDataFrame of edges post-`clean`, with columns
            `u`, `v`, `key`, `geometry` (non-empty LineString), and `osm_way_along_info`.

    Yields:
        tuple: (u, v, key, simple_segment_ranges), where `simple_segment_ranges` is a list
            of (start, end) tuples representing coordinate index ranges of simple, non-ring
            LineString segments derived from the edge’s geometry.

    Raises:
        AssertionError: If an edge lacks `osm_way_along_info` (indicating it wasn’t processed
            by `clean`), if the geometry’s coordinate sequence has fewer than 2 distinct
            points, or if any simple segment range does not have a start index less than
            its end index.

    Notes:
        - Assumes `clean` has provided a valid, non-empty LineString in `geometry`, with
          duplicate coordinates removed, length validated against OSM data, and
          `osm_way_along_info` added to every edge.
        - If the geometry is already simple and not a ring, yields a single range covering
          all coordinates.
        - Ring geometries (closed LineStrings) are split into simple segments, which may
          affect downstream analysis expecting intact rings.
        - Designed for disaster planning, ensuring reliable segment data for spatial operations
          without storing results in the input GeoDataFrame.
    """
    for row_loc_idx, row in edges_gdf.iterrows():
        # If the GeoDataFrame has not been pre-processed by clean,
        #   then it may have duplicate consecutive coordinates
        #   that will cause anomalies in the simple segments
        assert len(row_loc_idx) > 3 or row.get("osm_way_along_info") is not None, (
            f"Edge ({row_loc_idx}): Missing osm_way_along_info; not processed by clean."
        )

        geom = row["geometry"]
        coords = list(geom.coords)

        if geom.is_empty:
            yield (row_loc_idx, [])
            continue

        if geom.is_simple and not geom.is_ring:
            yield (row_loc_idx, [(0, len(coords) - 1)])
            continue

        assert len(set(coords)) > 1, (
            "LineString coordinate sequence must have at least 2 distinct coordinates."
        )

        current_segment_coords = [coords[0]]
        simple_segment_ranges = [[0, 0]]

        for coord_idx in range(1, len(coords)):
            next_point = coords[coord_idx]
            candidate_segment = LineString(current_segment_coords + [next_point])

            if candidate_segment.is_simple and not candidate_segment.is_ring:
                simple_segment_ranges[-1][1] = coord_idx
            else:
                simple_segment_ranges.append([coord_idx - 1, coord_idx])
                current_segment_coords = [current_segment_coords[-1], next_point]

        try:
            assert all([d[0] < d[1] for d in simple_segment_ranges])
            assert all(
                a[1] == b[0]
                for a, b in zip(simple_segment_ranges[:-1], simple_segment_ranges[1:])
            )
        except Exception as e:
            pprint(coords)
            raise e

        yield (row_loc_idx, [tuple(s) for s in simple_segment_ranges])


def create_simple_segments_gdf(edges_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    simple_seg_locs = []
    simple_segment_geoms = []

    edges_index_names = edges_gdf.index.names

    for row_loc_idx, simple_segment_ranges in split_into_simple_segments(
        edges_gdf=edges_gdf
    ):
        edge = edges_gdf.loc[row_loc_idx]
        coords = edge.geometry.coords

        for seg_idx, (start_coord_idx, end_coord_idx) in enumerate(
            simple_segment_ranges
        ):
            seg_coords = coords[start_coord_idx : end_coord_idx + 1]
            geom = LineString(seg_coords)

            simple_seg_locs.append((*row_loc_idx, seg_idx))
            simple_segment_geoms.append(geom)

    multi_index = pd.MultiIndex.from_tuples(
        tuples=simple_seg_locs, names=[*edges_index_names, "simple_seg_idx"]
    )

    simple_segments_gdf = gpd.GeoDataFrame(
        geometry=simple_segment_geoms,
        index=multi_index,
        crs=edges_gdf.crs,
    )

    return simple_segments_gdf


def _create_enriched_osmnx_graph(
    osm: pyrosm.OSM,  #
    region_boundary_gdf: Optional[GeoDataFrame] = None,
    include_base_osm_data: bool = False,
) -> EnrichedOsmNetworkData:
    """
    Create and enrich an OSMnx graph from Pyrosm OSM data for road network analysis.

    This function processes OpenStreetMap data using Pyrosm to create a road network
    graph optimized for driving analysis by performing the following steps:
      1. Extract driving network data (nodes and edges) from the OSM object
      2. Add region intersection information to edges if a boundary is provided
      3. Convert to a NetworkX graph with OSMnx compatibility
      4. Simplify the graph structure using OSMnx's simplify_graph
      5. Enrich the graph with additional attributes using enrich_osmnx_graph
      6. Convert the enriched graph to GeoDataFrames for nodes and edges

    Parameters:
        osm (pyrosm.OSM): A Pyrosm OSM object containing the raw OpenStreetMap data
        region_boundary_gdf (GeoDataFrame, optional): A GeoDataFrame containing a polygon
                            boundary representing the original, unbuffered region. Used to
                            determine which edges intersect with the specific region.
                            If None, all edges are marked as intersecting the region.

    Returns:
        dict: A dictionary containing three elements:
            - 'g': The enriched and simplified MultiDiGraph
            - 'nodes_gdf': A GeoDataFrame of all nodes in the graph
            - 'edges_gdf': A GeoDataFrame of all edges in the graph

    Notes:
        - The network_type is explicitly set to "driving" to ensure oneway tags are applied
        - When working with a buffered OSM extract (where the OSM data includes areas outside
          the region of interest), the '_intersects_region_' attribute indicates whether each
          road segment intersects with the original unbuffered region
        - The graph is simplified using OSMnx's simplify_graph function
        - The enrich_osmnx_graph function is called to add additional attributes
        - The returned GeoDataFrames have their indices sorted for consistency
    """

    # https://pyrosm.readthedocs.io/en/latest/reference.html#pyrosm.pyrosm.OSM.get_network
    # NOTE: MUST set the network_type to "driving". The default network_type is "walking" and "oneway" tags do not apply.
    nodes, edges = osm.get_network(nodes=True, network_type="driving")

    # Add the _intersects_region_ column that indicates whether the
    if region_boundary_gdf is not None:
        intersects = edges.intersects(
            other=region_boundary_gdf.to_crs(edges.crs)["geometry"].values[0]
        )
        # Sometime intersects returns a list of Booleans which breaks simple filtering on this column.
        edges["_intersects_region_"] = [
            v if isinstance(v, bool) else any(v) for v in intersects
        ]
    else:
        edges["_intersects_region_"] = True

    # Export the nodes and edges to NetworkX graph
    G = osm.to_graph(
        nodes,  #
        edges,
        graph_type="networkx",
        retain_all=True,
        osmnx_compatible=True,
    )

    # Our road network graph can be represented as two GeoDataFrames
    g = ox.simplify_graph(
        G=G,  #
        track_merged=True,
    )

    enrich_osmnx_graph(g)

    verify_osm_alignment(g=g, G=G)
    clean_geometries(g=g, G=G)

    nodes_gdf, edges_gdf = ox.convert.graph_to_gdfs(g)

    nodes_gdf.sort_index(inplace=True)
    edges_gdf.sort_index(inplace=True)

    if include_base_osm_data:
        return {
            "ENRICH_VERSION": ENRICH_VERSION,
            "g": g,
            "nodes_gdf": nodes_gdf,
            "edges_gdf": edges_gdf,
            "G": G,
        }

    edges_gdf.drop(columns=["merged_edges"], inplace=True)

    return {"g": g, "nodes_gdf": nodes_gdf, "edges_gdf": edges_gdf}


def create_bridge_spans_gdf(edges_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Generate a GeoDataFrame of bridge spans from an edges GeoDataFrame.

    Extracts bridge spans from `edges_gdf` using `generate_bridge_spans_along_info` to
    identify continuous bridge segments, converting these into a GeoDataFrame with
    segmented geometries. Complements `create_non_bridge_spans_gdf` in a disaster planning
    pipeline. Processes edges incrementally to minimize memory usage.

    Parameters:
        edges_gdf (gpd.GeoDataFrame): GeoDataFrame of edges from `clean`, with columns:
            - geometry: LineString representing the edge.
            - osm_way_along_info: List of dicts with `start_coord_idx`, `end_coord_idx`,
              `bridge_tag`, `osmid`, `osm_nodes`, etc.
            - u, v, key: Edge identifiers.

    Returns:
        gpd.GeoDataFrame: A new GeoDataFrame with one row per bridge span, containing:
            - geometry: LineString of the bridge segment.
            - start_coord_idx, end_coord_idx: Indices in the parent edge’s geometry.
            - start_ratio_along, end_ratio_along: Ratios along the parent edge’s length.
            - osmids: List of OSM way IDs in the span.
            - bridge_tags: List of bridge tag values from each segment.
            - osm_nodes: List of OSM node IDs along the span.
            - u, v, key: Parent edge identifiers (in the index with span_idx).

    Raises:
        ValueError: If the multi-index (u, v, key, span_idx) contains duplicate entries,
            indicating an error in span generation or edge data.
    """
    bridge_spans = []

    for (u, v, key), row in edges_gdf.iterrows():
        if not row["osm_way_along_info"]:
            continue

        spans = generate_bridge_spans_along_info(row["osm_way_along_info"])

        for span_idx, span in enumerate(spans):
            span["u"] = u
            span["v"] = v
            span["key"] = key
            span["span_idx"] = span_idx

            geom = row["geometry"]
            coords = geom.coords[span["start_coord_idx"] : span["end_coord_idx"] + 1]
            span["geometry"] = LineString(coords) if len(coords) > 1 else LineString()

            bridge_spans.append(span)

    index_keys = ["u", "v", "key", "span_idx"]
    columns = [
        "geometry",
        "start_coord_idx",
        "end_coord_idx",
        "start_ratio_along",
        "end_ratio_along",
        "osmids",
        "refs",
        "names",
        "bridge_tags",
        "osm_nodes",
    ]

    bridges_gdf = gpd.GeoDataFrame(
        bridge_spans if bridge_spans else None,  #
        columns=index_keys + columns,
        crs=edges_gdf.crs,
    )

    if bridge_spans:
        bridges_gdf.set_index(index_keys, inplace=True, verify_integrity=True)
        bridges_gdf.drop(
            bridges_gdf[bridges_gdf["geometry"].apply(lambda x: x.is_empty)].index,
            inplace=True,
        )
    else:
        empty_index = pd.MultiIndex.from_arrays([[], [], [], []], names=index_keys)
        bridges_gdf.set_index(empty_index, inplace=True)

    return bridges_gdf


# Assuming generate_bridge_spans_along_info remains as provided
def generate_bridge_spans_along_info(osm_way_along_info):
    bridge_spans_along_info = []
    current_span = None

    for segment in osm_way_along_info:
        if segment["bridge_tag"] is not None:
            if current_span is None:
                current_span = {
                    "osmids": [segment["osmid"]],
                    "refs": [segment["ref"]],
                    "names": [segment["name"]],
                    "bridge_tags": [segment["bridge_tag"]],
                    "osm_nodes": segment["osm_nodes"].copy(),
                    "start_ratio_along": segment["start_ratio_along"],
                    "start_coord_idx": segment["start_coord_idx"],
                    "end_ratio_along": segment["end_ratio_along"],
                    "end_coord_idx": segment["end_coord_idx"],
                }
            else:
                current_span["osmids"].append(segment["osmid"])

                current_span["refs"].append(segment["ref"])
                current_span["names"].append(segment["name"])

                current_span["bridge_tags"].append(segment["bridge_tag"])
                current_span["osm_nodes"].extend(segment["osm_nodes"][1:])
                current_span["end_ratio_along"] = segment["end_ratio_along"]
                current_span["end_coord_idx"] = segment["end_coord_idx"]
        else:
            if current_span is not None:
                bridge_spans_along_info.append(current_span)
                current_span = None

    if current_span is not None:
        bridge_spans_along_info.append(current_span)

    return bridge_spans_along_info


def create_nonbridge_spans_gdf(edges_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Create a GeoDataFrame of non-bridge road segments from an edges GeoDataFrame.

    Extracts non-bridge spans from `edges_gdf` using `generate_nonbridge_spans_along_info`
    to identify gaps between bridge segments, converting these into a GeoDataFrame with
    segmented geometries. Complements `generate_bridges_gdf` in a disaster planning pipeline.
    Processes edges incrementally to minimize memory usage.

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

    Raises:
        ValueError: If the multi-index (u, v, key, span_idx) contains duplicate entries,
            indicating an error in span generation or edge data.
    """
    non_bridge_spans = []

    # Iterate directly over edges_gdf without copying
    for (u, v, key), row in edges_gdf.iterrows():
        if not row["osm_way_along_info"]:  # Skip if osm_way_along_info is empty
            continue

        spans = generate_nonbridge_spans_along_info(row["osm_way_along_info"])

        for span_idx, span in enumerate(spans):
            span["u"] = u
            span["v"] = v
            span["key"] = key
            span["span_idx"] = span_idx

            geom = row["geometry"]
            coords = geom.coords[span["start_coord_idx"] : span["end_coord_idx"] + 1]
            span["geometry"] = LineString(coords) if len(coords) > 1 else LineString()

            non_bridge_spans.append(span)

    index_keys = ["u", "v", "key", "span_idx"]
    columns = [
        "geometry",
        "start_coord_idx",
        "end_coord_idx",
        "start_ratio_along",
        "end_ratio_along",
        "osmids",
        "osm_nodes",
    ]

    # Create GeoDataFrame
    spans_df = gpd.GeoDataFrame(
        non_bridge_spans if non_bridge_spans else None,
        columns=index_keys + columns,
        crs=edges_gdf.crs,
    )

    # Set multi-index for both empty and populated cases
    if non_bridge_spans:
        spans_df.set_index(index_keys, inplace=True, verify_integrity=True)
        spans_df.drop(
            spans_df[spans_df["geometry"].apply(lambda x: x.is_empty)].index,
            inplace=True,
        )
    else:
        # For empty case, define an empty multi-index with the same structure
        empty_index = pd.MultiIndex.from_arrays([[], [], [], []], names=index_keys)
        spans_df.set_index(empty_index, inplace=True)

    return spans_df


def generate_nonbridge_spans_along_info(osm_way_along_info):
    """
    Generate a list of non-bridge span dictionaries from OSM way info.

    Groups consecutive non-bridge segments from `osm_way_along_info` into spans, capturing
    their start and end ratios and coordinate indices. Complements `generate_bridge_spans_along_info`
    for disaster planning analysis.

    Parameters:
        osm_way_along_info (list): List of OSM way segments from `clean`, each with keys
            `start_ratio_along`, `end_ratio_along`, `start_coord_idx`, `end_coord_idx`,
            `bridge_tag`, `osmid`, `osm_nodes`.

    Returns:
        list: List of dictionaries, each representing a non-bridge span with:
            - osmids: List of OSM way IDs in the span.
            - osm_nodes: List of OSM node IDs along the span.
            - start_ratio_along, end_ratio_along: Ratios along the parent edge.
            - start_coord_idx, end_coord_idx: Indices in the parent edge’s geometry.
    """
    non_bridge_spans_along_info = []
    current_span = None

    for segment in osm_way_along_info:
        if segment["bridge_tag"] is None:
            if current_span is None:
                current_span = {
                    "osmids": [segment["osmid"]],
                    "osm_nodes": segment["osm_nodes"].copy(),
                    "start_ratio_along": segment["start_ratio_along"],
                    "start_coord_idx": segment["start_coord_idx"],
                    "end_ratio_along": segment["end_ratio_along"],
                    "end_coord_idx": segment["end_coord_idx"],
                }
            else:
                current_span["osmids"].append(segment["osmid"])
                current_span["osm_nodes"].extend(segment["osm_nodes"][1:])
                current_span["end_ratio_along"] = segment["end_ratio_along"]
                current_span["end_coord_idx"] = segment["end_coord_idx"]
        else:
            if current_span is not None:
                non_bridge_spans_along_info.append(current_span)
                current_span = None

    if current_span is not None:
        non_bridge_spans_along_info.append(current_span)

    return non_bridge_spans_along_info


def create_enriched_osmnx_graph(
    osm_pbf: PathLike, include_base_osm_data=False
) -> EnrichedOsmNetworkData:
    # From https://pyrosm.readthedocs.io/en/latest/basics.html?highlight=osmnx#export-to-networkx-osmnx
    osm = pyrosm.OSM(osm_pbf)

    return _create_enriched_osmnx_graph(
        osm=osm,  #
        include_base_osm_data=include_base_osm_data,
    )


def create_enriched_osmnx_graph_using_buffered_region_gdfs(
    osm_pbf: PathLike,  #
    region_gdf: GeoDataFrame,
    buffered_region_gdf: GeoDataFrame,
    include_base_osm_data: bool = False,
) -> EnrichedOsmNetworkDataWithRegions:
    """
    Create an enriched OSMnx graph using an OSM PBF file and both original and buffered region geometries.

    This function streamlines the process of creating a road network graph for a specific region
    with a buffered area to include roads that extend beyond the region boundaries. It performs
    the following steps:
      1. Initialize a Pyrosm OSM object with the provided PBF file, using the buffered region
         as the bounding box to limit data extraction
      2. Create an enriched OSMnx graph using the OSM data and the original (unbuffered) region
         to mark which edges intersect with the region of interest
      3. Return the graph and related GeoDataFrames along with the input region geometries

    Parameters:
        osm_pbf (PathLike): Path to the OpenStreetMap PBF file containing raw OSM data
        region_gdf (GeoDataFrame): A GeoDataFrame containing the polygon geometry of the
                                  original region of interest
        buffered_region_gdf (GeoDataFrame): A GeoDataFrame containing the polygon geometry
                                           of the buffered region used to extract OSM data

    Returns:
        dict: A dictionary containing:
            - 'g': The enriched and simplified MultiDiGraph
            - 'nodes_gdf': A GeoDataFrame of all nodes in the graph
            - 'edges_gdf': A GeoDataFrame of all edges in the graph
            - 'region_gdf': The original region GeoDataFrame
            - 'buffered_region_gdf': The buffered region GeoDataFrame

    Notes:
        - The function uses the buffered region as a bounding box to extract data from the OSM PBF file,
          ensuring that the road network includes roads that extend beyond the original region
        - The '_intersects_region_' attribute in the resulting graph indicates which road segments
          intersect with the original (unbuffered) region
        - The original and buffered region GeoDataFrames are included in the return value for reference
          and further analysis
    """

    filepath = str(osm_pbf)

    osm = pyrosm.OSM(
        filepath=filepath,
        bounding_box=buffered_region_gdf.to_crs(
            "EPSG:4326", inplace=False
        ).geometry.values[0],
    )

    d = _create_enriched_osmnx_graph(
        osm=osm,  #
        region_boundary_gdf=region_gdf,
        include_base_osm_data=include_base_osm_data,
    )

    return d | {"region_gdf": region_gdf, "buffered_region_gdf": buffered_region_gdf}


def parse_osm_region_parameters(
    osm_pbf: PathLike,  #
    geoid: Optional[str],
    buffer_dist_mi: Optional[int],
) -> OsmNetworkMetadata:
    # Determine geoid and buffer distance if not provided.
    if not geoid:
        try:
            parsed = parse_geography_region_name(str(osm_pbf))
            geoid = parsed["geoid"]

            if buffer_dist_mi is None:
                buffer_dist_mi = parsed.get("buffer_dist_mi", DEFAULT_BUFFER_DIST_MI)

        except Exception as e:
            logger.error(f"Failed to parse region name from file: {e}")
            raise

    if buffer_dist_mi is None:
        buffer_dist_mi = DEFAULT_BUFFER_DIST_MI

    region_name = get_geography_region_name(geoid=geoid, buffer_dist_mi=buffer_dist_mi)

    osm_version = get_osm_version_from_pbf_filename(osm_pbf=osm_pbf)

    return dict(
        osm_pbf=osm_pbf,
        geoid=geoid,
        buffer_dist_mi=buffer_dist_mi,
        region_name=region_name,
        osm_version=osm_version,
    )


def get_enriched_osm_pickle_path(osm_pbf: PathLike):
    base = os.path.basename(osm_pbf)
    base_without_extension = os.path.splitext(base)[0]

    pickled_name = base_without_extension + ".pickle"
    pickled_path = os.path.join(OSMNX_PICKLE_DIR, pickled_name)

    return pickled_path


def create_enriched_osmnx_graph_for_region(
    osm_pbf: PathLike,  #
    geoid: Optional[str] = None,
    buffer_dist_mi: Optional[int] = None,
    include_base_osm_data: bool = False,
) -> EnrichedOsmNetworkDataWithFullMetadata:
    """
    Create an enriched OSMnx graph for a geographic region identified by a GEOID.

    This function provides a high-level interface for creating road network graphs
    for specific geographic regions. It performs the following steps:
      1. Identify the region by GEOID, either from parameters or by parsing from the OSM file name
      2. Retrieve the boundary GeoDataFrame for the specified region
      3. Create a buffered version of the region boundary
      4. Generate an enriched OSMnx graph using the original and buffered region boundaries

    Parameters:
        osm_pbf (PathLike): Path to the OpenStreetMap PBF file containing raw OSM data
        geoid (str, optional): Geographic identifier code for the region of interest.
                              If None, the function attempts to parse it from the OSM file name.
        buffer_dist_mi (int, optional): Buffer distance in miles to extend around the region.
                                      If None, the function attempts to parse it from the OSM file name.

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

    Notes:
        - If geoid and buffer_dist_mi are not provided, the function attempts to extract
          them from the OSM PBF filename using parse_geography_region_name
        - The region boundary is retrieved using get_region_boundary_gdf
        - The buffered region is created using get_buffered_region_gdf
        - The '_intersects_region_' attribute in the resulting graph indicates which road
          segments intersect with the original (unbuffered) region
    """

    # Only the full enriched OSM, with the base data, gets cached.
    enriched_pickle_path = (
        get_enriched_osm_pickle_path(osm_pbf=osm_pbf) if include_base_osm_data else None
    )

    if enriched_pickle_path:
        if os.path.exists(enriched_pickle_path):
            with open(enriched_pickle_path, "rb") as f:
                d = pickle.load(f)

                if d["ENRICH_VERSION"] == ENRICH_VERSION:
                    return d
                else:
                    os.remove(enriched_pickle_path)

    parsed_params = parse_osm_region_parameters(
        osm_pbf=osm_pbf, geoid=geoid, buffer_dist_mi=buffer_dist_mi
    )

    geoid = parsed_params["geoid"]
    buffer_dist_mi = parsed_params["buffer_dist_mi"]
    region_name = parsed_params["region_name"]

    region_gdf = get_region_boundary_gdf(geoid)
    region_gdf.to_crs(crs="EPSG:4326", inplace=True)

    buffered_region_gdf = get_buffered_region_gdf(
        region_gdf=region_gdf,  #
        buffer_dist_mi=buffer_dist_mi,
    )

    d = create_enriched_osmnx_graph_using_buffered_region_gdfs(
        osm_pbf=osm_pbf,  #
        region_gdf=region_gdf,
        buffered_region_gdf=buffered_region_gdf,
        include_base_osm_data=include_base_osm_data,
    )

    if d["edges_gdf"].crs != "EPSG:4326":
        raise ValueError(
            f"edges_gdf.crs MUST equal EPSG:4326, got {d['edges_gdf'].crs}"
        )

    d["g"].graph.setdefault("_region_name_", region_name)
    d["nodes_gdf"].attrs["_region_name_"] = region_name
    d["edges_gdf"].attrs["_region_name_"] = region_name
    d["buffered_region_gdf"].attrs["_region_name_"] = region_name

    if "G" in d:
        d["G"].graph.setdefault("_region_name_", region_name)

    enriched_osm = d | dict(
        region_name=region_name,
        geoid=geoid,
        buffer_dist_mi=buffer_dist_mi,
    )

    if enriched_pickle_path:
        os.makedirs(
            os.path.dirname(enriched_pickle_path),
            exist_ok=True,
        )

        with open(enriched_pickle_path, "wb") as f:
            pickle.dump(obj=enriched_osm, file=f)

    return enriched_osm
