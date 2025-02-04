import math
from enum import Enum
from collections.abc import Iterable

import pyrosm
import osmnx as ox
import shapely
import pyproj

MILES_PER_METER = 0.0006213712

class Direction(Enum):
    UPSTREAM = -1
    DOWNSTREAM = 1

# See: https://github.com/sharedstreets/sharedstreets-js/blob/98f8b78d0107046ed2ac1f681cff11eb5a356474/src/index.ts#L600-L613
class RoadClass(Enum):
    motorway = 0
    trunk = 1
    primary = 2
    secondary = 3
    tertiary = 4
    residential = 5
    living_street = 5.5
    unclassified = 6
    service = 7
    other = 8

# This is just a helper function for highway_type_analysis_for_way
def flatten(k, v):
    v = v[k] if isinstance(v[k], list) else [v[k]]
    v = [x for x in v if isinstance(x, str)]

    return v[0] if v else None
# https://github.com/sharedstreets/sharedstreets-builder/blob/a554983e96010d32b71d7d23504fa88c6fbbad10/src/main/java/io/sharedstreets/tools/builder/osm/model/Way.java#L61-L94
# https://www.openlr.org/
def highway_type_analysis_for_way(way):
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

        is_link = hwy.endswith('link')

        cur_roadclass = None

        if (hwy.startswith("motorway")):
            cur_roadclass = RoadClass.motorway.value
        elif (hwy.startswith("trunk")):
            cur_roadclass = RoadClass.trunk.value
        elif (hwy.startswith("primary")):
            cur_roadclass = RoadClass.primary.value
        elif (hwy.startswith("secondary")):
            cur_roadclass = RoadClass.secondary.value
        elif (hwy.startswith("tertiary")):
            cur_roadclass = RoadClass.tertiary.value
        elif (hwy.startswith("residential")):
            cur_roadclass = RoadClass.residential.value
        elif (hwy.startswith("living_street")):
            cur_roadclass = RoadClass.living_street.value
        elif (hwy.startswith("unclassified")):
            cur_roadclass = RoadClass.unclassified.value
        elif (hwy.startswith("service")):
            service = way["service"]

            if not isinstance(service, list):
                service = [service]

            for svc in service:
                if not isinstance(svc, str):
                    cur_roadclass = RoadClass.service.value
                elif (
                    svc.startswith('parking')
                    or svc.startswith('driveway')
                    or svc.startswith('drive-through')
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
            "edge_lowest_highway_type": None
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
        "edge_lowest_highway_type": lowest_highway_type
    }

# From the list of edge_ids, get the most important edge's route number and street/route name.
def get_best_edge_name(g, edge_ids, to_omit_edge_full_name=None):
    if not isinstance(edge_ids, list):
        if isinstance(edge_ids, tuple):
            edge_ids = [edge_ids]
        elif isinstance(edge_ids, Iterable): # For NetworkX Edge Views
            edge_ids = list(edge_ids)

    candidates = []
    
    for edge_id in edge_ids:
        edges_data = g.get_edge_data(*edge_id)
        edges_data = edges_data.values() if hasattr(edges_data, 'values') else [edges_data]

        for edge_data in edges_data:
            roadclass = edge_data['edge_min_roadclass']
            ref = edge_data['ref']
            name = edge_data['name']

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
            penalty = roadclass # Lowest penalty wins.

            # Prefer ref to name: not having a ref incurs higher penalty than not having a name.
            if not ref:
                penalty += 1
            else:
                edge_full_name = ref 

            if not name:
                penalty += 0.5
            else:
                edge_full_name = f'{edge_full_name}/{name}' if edge_full_name else name

            # Having neither a ref nor a name incurs a steep penalty.
            # Any ref and or name will beat this edge regardless of road class.
            # If no edges have ref or name, then lowest road class will win.
            if not edge_full_name:
                penalty += 100
                candidates.append((penalty, f'<{edge_data["edge_highest_highway_type"]}>'))
            elif edge_full_name != to_omit_edge_full_name:
                candidates.append((penalty, edge_full_name))

    candidates.sort()

    return candidates[0][1] if candidates else None

# NOTE: Mutates the OSMnx Graph object.
def enrich_osmnx_graph(g):
    geod = pyproj.Geod(ellps="WGS84")
    precision = 6

    # WARNING: Reindexing and exporting to GPKG throws the following error:
    #            ValueError: cannot insert osmid, already exists
    #
    # The following workaround is suggested by the library's author:
    #   https://github.com/gboeing/osmnx/issues/638#issuecomment-756948363
    for node, data in g.nodes(data=True):
        if 'osmid' in data:
            data['osmid_original'] = data.pop('osmid')

    # NOTE: add_edge_speeds crashes without the following work around
    for (u, v, d) in g.edges(data=True):
        if not (isinstance(d['maxspeed'], str) or d['maxspeed'] is None):
            d['maxspeed'] = flatten('maxspeed', d)
        
        # For some OSM areas there are no refs defined.
        d['ref'] = d['ref'] if 'ref' in d else None
        
        d.update(highway_type_analysis_for_way(d))

        geom = d['geometry']

        len_m = geod.geometry_length(
            shapely.wkt.loads(
                shapely.wkt.dumps(
                    geom,
                    rounding_precision=precision
                )
            )
        )

        len_mi = len_m * MILES_PER_METER

        d['length_mi'] = round(len_mi, precision)

    # Because get_best_name depends on properties added in highway_type_analysis_for_way,
    #   and because g.edges must access edges in a non-sequential manner,
    #   we must gather names after the above loop has run highway_type_analysis_for_way
    #   for every edge.
    for (u, v, d) in g.edges(data=True):
        road_name = get_best_edge_name(g, (u, v))

        d['road_name'] = road_name
        
        d['from_name'] = get_best_edge_name(
            g,
            list(g.edges(u)),
            to_omit_edge_full_name=road_name
        )

        d['to_name'] = get_best_edge_name(
            g,
            list(g.edges(v)),
            to_omit_edge_full_name=road_name
        )

    # https://osmnx.readthedocs.io/en/stable/user-reference.html#osmnx.routing.add_edge_speeds
    ox.add_edge_speeds(g)

    # https://osmnx.readthedocs.io/en/stable/user-reference.html#osmnx.routing.add_edge_travel_times
    ox.add_edge_travel_times(g)


def create_enriched_osmnx_graph(osm_pbf):
    # From https://pyrosm.readthedocs.io/en/latest/basics.html?highlight=osmnx#export-to-networkx-osmnx
    osm = pyrosm.OSM(osm_pbf)

    # https://pyrosm.readthedocs.io/en/latest/reference.html#pyrosm.pyrosm.OSM.get_network
    # NOTE: MUST set the network_type to "driving". The default network_type is "walking" and "oneway" tags do not apply.
    nodes, edges = osm.get_network(
        nodes=True,
        network_type='driving'
    )

    # Export the nodes and edges to NetworkX graph
    G = osm.to_graph(
        nodes,
        edges,
        graph_type="networkx",
        retain_all=True,
        osmnx_compatible=True
    )

    # Our road network graph can be represented as two GeoDataFrames
    g = ox.simplify_graph(G)

    enrich_osmnx_graph(g)

    nodes_gdf, edges_gdf = ox.convert.graph_to_gdfs(g)

    return {
        "g": g,
        "nodes_gdf": nodes_gdf,
        "edges_gdf": edges_gdf
    }

def find_nodes_along_road(g, edge_id, direction, num_nodes=3):
    # We do not want the same edge of its opposite direction pair.
    omitted_u_v_pairs = {
        edge_id[:2],
        edge_id[:2][::-1]
    }

    # If we are moving upstream, we are interested in working backwards from the edge origin.
    #   Therefore, because edge origins are the u of the (u, v) pairs, we need idx == 0.
    # If we are moving downstream, we are interested in working forward from the edge destination.
    #   Therefore, because edge destinations are the v of the (u, v) pairs, we need idx == 1.
    node_edge_id_idx = 0 if direction == Direction.UPSTREAM else 1

    edge_data = g.get_edge_data(*edge_id)
    
    # We prefer to use the ref (route number) if available. If not available, use street name.
    identifier_key = 'ref' if edge_data['ref'] else 'name'

    # FIXME: If neither a road number nor name is provided, skip.
    if not identifier_key:
        raise BaseException('Edge has neither "ref" nor "name".')

    identifier_val = edge_data[identifier_key]

    nodes_along = []
    cur_node = edge_id[node_edge_id_idx]
    
    for i in range(num_nodes):
        incident_edges = (
            g.in_edges(cur_node, data=True)
            if direction == Direction.UPSTREAM
            else g.out_edges(cur_node, data=True)
        )

        incident_edges = [
            e for e in incident_edges
            if (
                e[2][identifier_key] == identifier_val
                and
                e[:2] not in omitted_u_v_pairs
            )
        ]

        if not incident_edges:
            break

        # TODO: Take notice when returns more than one.
        incident_edge = incident_edges[0]

        omitted_u_v_pairs.add(incident_edge[:2])
        omitted_u_v_pairs.add(incident_edge[:2][::-1])

        nodes_along.append(incident_edge[node_edge_id_idx])

        cur_node = incident_edge[node_edge_id_idx]

    return nodes_along
