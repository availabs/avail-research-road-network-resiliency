from enum import Enum
from typing import List, Optional, Tuple

from networkx import MultiDiGraph


class Direction(Enum):
    UPSTREAM = -1
    DOWNSTREAM = 1


def find_nodes_along_road(
    g: MultiDiGraph,
    edge_id: Tuple[int, int, int],
    direction: Direction,
    num_nodes: Optional[int] = 3,
) -> List[int]:
    """
    Finds nodes along a road by traversing adjacent edges in a specified direction.
    Starting from the given edge, the function follows edges that share a common
    road identifier (preferably the 'ref' attribute, else the 'name') and collects nodes
    along the road. The traversal is performed in either an upstream or downstream
    direction, as specified by the 'direction' parameter, and continues until the
    desired number of nodes (num_nodes) is reached or no further matching edges
    are found.

    Parameters:
        g (MultiDiGraph): The graph representing the road network.
        edge_id (Tuple[int, int, int]): Identifier for the starting edge, given as a
            tuple (u, v, key).
        direction (Direction): Direction to traverse. Use UPSTREAM to follow incoming
            edges (from the edge origin) and DOWNSTREAM to follow outgoing edges
            (from the edge destination).
        num_nodes (int, optional): Maximum number of nodes to retrieve along the road.
            Defaults to 3.

    Returns:
        List: A list of node identifiers encountered along the road.

    Raises:
        BaseException: If the starting edge lacks both a 'ref' and a 'name' attribute.

    Notes:
        To avoid traversing the same edge in opposite directions, the function
        maintains a set of omitted node pairs. The incident edge chosen is the first
        one matching the road identifier. If multiple edges are available, only the
        first one is considered.
    """

    # We do not want the same edge of its opposite direction pair.
    omitted_u_v_pairs = {edge_id[:2], edge_id[:2][::-1]}

    # If we are moving upstream, we are interested in working backwards from the edge origin.
    #   Therefore, because edge origins are the u of the (u, v) pairs, we need idx == 0.
    # If we are moving downstream, we are interested in working forward from the edge destination.
    #   Therefore, because edge destinations are the v of the (u, v) pairs, we need idx == 1.
    node_edge_id_idx = 0 if direction == Direction.UPSTREAM else 1

    edge_data = g.get_edge_data(*edge_id)

    # We prefer to use the ref (route number) if available. If not available, use street name.
    identifier_key = "ref" if edge_data["ref"] else "name"

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
            e
            for e in incident_edges
            if (
                e[2][identifier_key] == identifier_val
                and e[:2] not in omitted_u_v_pairs
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
