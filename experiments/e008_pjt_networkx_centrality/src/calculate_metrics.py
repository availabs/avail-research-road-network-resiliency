# -*- coding: utf-8 -*-
"""
Module for calculating various graph metrics using NetworkX.

This module defines network metrics as an Enum and provides functions to compute
these metrics for a given NetworkX graph. The core functions are designed to
calculate the metric and return the result, leaving persistence (like pickling)
to the calling code.
"""

import logging
import os
from enum import Enum
from typing import Any, Dict, List, Set, Tuple, Union

import networkx as nx

# Set up basic logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class NetworkMetric(Enum):
    """
    Enumeration of supported network metrics.

    This enum provides standardized names for different graph metrics
    that can be calculated using this module. Using an Enum helps prevent
    typos and ensures consistency when requesting metrics.

    Attributes:
        NODE_BETWEENNESS_CENTRALITY: Measures the extent to which a node lies
            on paths between other nodes. Uses shortest paths weighted by
            'travel_time' if available.
        CLOSENESS_CENTRALITY: Measures how close a node is to all other nodes
            in the graph. Based on shortest path lengths.
        EDGE_BETWEENNESS_CENTRALITY: Measures the extent to which an edge lies
            on paths between other nodes. Uses shortest paths weighted by
            'travel_time' if available.
        LAPLACIAN_CENTRALITY: Measures the importance of a node based on the
            change in the graph's Laplacian energy when the node is removed.
            Uses edge weights ('travel_time' if available).
        LOUVAIN_COMMUNITIES: Community detection algorithm based on modularity
            optimization. Calculated using 'travel_time' weights if available.
    """

    NODE_BETWEENNESS_CENTRALITY = "node_betweenness_centrality"
    CLOSENESS_CENTRALITY = "closeness_centrality"
    EDGE_BETWEENNESS_CENTRALITY = "edge_betweenness_centrality"
    LOUVAIN_COMMUNITIES = "louvain_communities"

    @classmethod
    def get_all_metric_values(cls) -> List[str]:
        """Returns a list of all string values defined in the Enum."""
        return [metric.value for metric in cls]


def string_to_network_metric(metric_str: str) -> NetworkMetric:
    """
    Convert a string metric name to its corresponding NetworkMetric enum member.

    This function provides robust conversion from user input strings to the
    standardized NetworkMetric enum types.

    Args:
        metric_str (str): The string representation of the metric name (case-sensitive).
            Should match one of the values defined in the NetworkMetric Enum.

    Returns:
        NetworkMetric: The enum member corresponding to the input string.

    Raises:
        ValueError: If the `metric_str` does not match any of the defined
            NetworkMetric enum values. The error message lists valid options.

    Example:
        >>> string_to_network_metric("closeness_centrality")
        <NetworkMetric.CLOSENESS_CENTRALITY: 'closeness_centrality'>

        >>> string_to_network_metric("invalid_metric")
        ValueError: Invalid metric name 'invalid_metric'. Valid options are: [...]
    """
    try:
        return NetworkMetric(metric_str)
    except ValueError:
        valid_metrics = NetworkMetric.get_all_metric_values()
        raise ValueError(
            f"Invalid metric name '{metric_str}'. Valid options are: {valid_metrics}"
        )


def get_pickle_file_path(
    metrics_pickle_dir: Union[str, os.PathLike],
    metric: NetworkMetric,
    region_name: str,
) -> str:
    """
    Generate the standard pickle file path for a given metric and region.

    This function enforces a consistent naming convention for storing metric
    results as pickle files. The filename pattern is:
    `<metrics_pickle_dir>/<metric_value>.<region_name>.pickle`

    Args:
        metrics_pickle_dir (Union[str, os.PathLike]): The base directory where
            metric pickle files should be stored.
        metric (NetworkMetric): The enum member representing the specific metric.
        region_name (str): A string identifier for the geographic region or
            dataset the metric pertains to (e.g., 'county_36001', 'test_area').
            This is used to distinguish metric files for different datasets.

    Returns:
        str: The full, absolute or relative path (depending on input directory)
             to the pickle file designated for storing the specified metric
             for the given region.

    Example:
        >>> get_pickle_file_path("data/metric_pickles",
        ...                      NetworkMetric.NODE_BETWEENNESS_CENTRALITY,
        ...                      "albany_county")
        'data/metric_pickles/node_betweenness_centrality.albany_county.pickle'
    """
    if not isinstance(metric, NetworkMetric):
        raise TypeError(
            f"metric must be a NetworkMetric enum member, not {type(metric)}"
        )
    filename = f"{metric.value}.{region_name}.pickle"
    return os.path.join(metrics_pickle_dir, filename)


# --------------------------------------------------------------------------- #
# Specific Metric Computation Functions                                       #
# --------------------------------------------------------------------------- #
# These functions act as wrappers around the core computation logic,          #
# providing specific arguments (like weights) for each standard metric.       #
# They primarily focus on calling _compute_networkx_metric_core and returning #
# the raw result.                                                             #
# --------------------------------------------------------------------------- #


def compute_node_betweenness_centrality(
    g: nx.MultiDiGraph,
) -> Dict[Any, float]:
    """
    Computes node betweenness centrality for the graph.

    Node betweenness centrality of a node `v` is the sum of the fraction of all-pairs
    shortest paths that pass through `v`. It uses the 'travel_time' edge attribute
    as the weight for shortest path calculations if available.

    Args:
        g (nx.MultiDiGraph): The graph for which to compute centrality. Node IDs
            can be of any hashable type (int, str, tuple, etc.).

    Returns:
        Dict[Any, float]: A dictionary where keys are node IDs and values are
            their corresponding betweenness centrality scores (floats between
            0 and 1).

    Raises:
        Exception: Propagates exceptions from the underlying NetworkX function.
    """
    return nx.betweenness_centrality(
        G=g,  #
        weight="travel_time",
    )


def compute_closeness_centrality(g: nx.MultiDiGraph) -> Dict[Any, float]:
    """
    Computes closeness centrality for each node in the graph.

    Closeness centrality of a node `u` is the reciprocal of the sum of the shortest
    path distances from `u` to all `n-1` other nodes. Since the sum of distances
    depends on the number of nodes in the connected part of the graph, the closeness
    centrality is normalized by the number of nodes reachable from `u`. It uses the
    'travel_time' edge attribute as the distance for shortest path calculations if available.


    Args:
        g (nx.MultiDiGraph): The graph for which to compute centrality.

    Returns:
        Dict[Any, float]: A dictionary where keys are node IDs and values are
            their closeness centrality scores (floats between 0 and 1).

    Raises:
        Exception: Propagates exceptions from the underlying NetworkX function.
    """
    return nx.closeness_centrality(
        G=g,  #
        distance="travel_time",
    )


def compute_edge_betweenness_centrality(
    g: nx.MultiDiGraph,
) -> Dict[Tuple, float]:
    """
    Computes edge betweenness centrality for the graph.

    Edge betweenness centrality of an edge `e` is the sum of the fraction of all-pairs
    shortest paths that pass through `e`. It uses the 'travel_time' edge attribute
    as the weight for shortest path calculations if available.

    Note: For MultiDiGraphs, NetworkX calculates edge betweenness based on
    pairs of nodes, considering all edges between those nodes if they contribute
    to shortest paths. The result dictionary keys are typically tuples like `(u, v)`,
    representing the node pair. Check NetworkX documentation for specifics on how
    multi-edges are handled in the version you are using if precise per-edge
    centrality is needed.

    Args:
        g (nx.MultiDiGraph): The graph for which to compute centrality.

    Returns:
        Dict[Tuple, float]: A dictionary where keys are edge tuples (typically `(u, v)`)
            and values are their corresponding edge betweenness centrality scores.

    Raises:
        Exception: Propagates exceptions from the underlying NetworkX function.
    """
    return nx.edge_betweenness_centrality(
        G=g,  #
        weight="travel_time",
    )


def compute_louvain_communities(
    g: nx.MultiDiGraph,
) -> List[Set[Any]]:
    """
    Computes communities using the Louvain algorithm.

    The Louvain method is an efficient algorithm for community detection based on
    optimizing modularity. It uses 'travel_time' as the edge weight if available.

    Args:
        g (nx.MultiDiGraph): The graph to partition into communities.

    Returns:
        List[Set[Any]]: A list where each element is a set of node IDs belonging
            to a detected community. The list represents the best partition found
            by the algorithm in terms of modularity.

    Raises:
        Exception: Propagates exceptions from the underlying NetworkX function.
                   Note: The Louvain implementation might depend on the optional
                   `python-louvain` library being installed. Check your environment
                   if errors occur.
    """
    # Note: Ensure the environment has the necessary library if nx.louvain_communities requires it.
    # NetworkX >= 2.6 typically includes a native implementation or handles the dependency.
    return nx.algorithms.community.louvain_communities(
        G=g,  #
        weight="travel_time",
    )
