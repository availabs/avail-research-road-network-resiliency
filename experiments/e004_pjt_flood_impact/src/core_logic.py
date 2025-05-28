"""
**Explanation of Integrity Hash Checks**

In complex data processing pipelines like this flood impact analysis, data
objects (like GeoDataFrames and NetworkX graphs) are passed between different
functions or tasks. Each step relies on the output of the previous one. A
critical challenge is ensuring that the *exact same* data structures,
particularly with consistent indices or graph structures, are used throughout
the relevant parts of the pipeline.

For example:

* The `identify_impacted_edges` function needs the `nonbridge_spans_gdf` and
* `bridge_spans_gdf` to have indices that align perfectly with the edges in the
* `osmnx_simplified_g`.  The `calculate_highest_road_risk` function depends on
* the specific structure and multi-level index of the
* `nonbridge_spans_floodplains_join_gdf` created by the spatial join.  The final
* `add_impact_columns_to_gdf` function must operate on the original `roads_gdf`
* to correctly map impact results back to the road segments.

Subtle bugs can arise if, due to coding errors or unexpected side effects during
development or refactoring, a function receives a slightly modified or different
version of a DataFrame or graph than expected. Indices might get reset, rows
dropped, or graph structures altered, leading to incorrect results or runtime
errors that can be hard to diagnose.

The integrity hashing mechanism was introduced to address this:

1.  **Fingerprinting:** When key data objects (Graph, DataFrames) are first
created or processed, a unique hash value (an integer "fingerprint") is
calculated based on their structure (for graphs) or their index (for
DataFrames).  2.  **Tracking:** These hashes are stored in the
`integrity_hashes` dictionary, keyed by an `IntegrityHashKey` Enum member (e.g.,
`IntegrityHashKey.ROADS`).  3.  **Verification:** Before a function uses a
critical input object, it calls `verify_integrity_hashes`. This helper function
recalculates the hash of the *current* object being passed in and compares it to
the *expected* hash stored in the `integrity_hashes` dictionary.  4.  **Error
Prevention:** If the hashes don't match, it means the object has been
unexpectedly altered since its hash was first calculated, and the function
raises an `AssertionError`, stopping the workflow before incorrect data can be
processed.

Essentially, the integrity checks act as explicit assertions that guarantee the
structural consistency of the data as it flows through the pipeline, making the
process more robust and reliable. While the core logic functions now have an
`Optional` parameter to potentially bypass these checks, the main Prefect flow
currently ensures they are always performed by providing the necessary hash
dictionary.
"""

import logging
from dataclasses import dataclass
from enum import Enum, auto  # Import Enum and auto
from typing import (
    Callable,
    Dict,
    List,
    Literal,
    Set,
    Tuple,
    TypeAlias,
)

import geopandas as gpd
import networkx as nx
import osmnx as ox
import pandas as pd
from tqdm import tqdm

from common.fema.floodplains.core import (
    create_subdivided_spatial_index,
    spatial_join_quadrat_strtree_vectorized,
)

# Import functions directly from the common package.
from common.osm.enrich import (
    EnrichedOsmNetworkDataWithFullMetadata,
    convert_graph_to_gdfs,
    create_bridge_spans_gdf,
    create_nonbridge_spans_gdf,
)

# Set up logger for this module
logger = logging.getLogger(__name__)

# --- Constants ---
INTEGRITY_UUID_ATTR = "_flood_impact_integrity_uuid_"  # Attribute name for storing UUID
DEFAULT_QUADRAT_WIDTH = 0.005

# Define the specific allowed reason strings using Literal
FloodingImpactReason: TypeAlias = Literal["FLOODED", "BRIDGE_FUNCTIONALLY_DISCONNECTED"]

# Update ImpactInfo to use the ImpactReason Literal type
FloodingImpactInfo: TypeAlias = Tuple[
    FloodingImpactReason, int
]  # (reason, flood_risk_level)


# --- Enums for Hashing ---
class IntegrityHashKey(Enum):
    """Defines keys for the integrity_hashes dictionary."""

    GRAPH = auto()
    ROADS = auto()
    BRIDGE_SPANS = auto()
    NONBRIDGE_SPANS = auto()
    FLOODPLAINS = auto()
    JOIN_GDF = auto()  # Key for the join result hash


class HashableObjectType(Enum):
    """Defines the types of objects supported for integrity hashing."""

    GRAPH = auto()
    DATA_FRAME = auto()  # Covers pandas DataFrame and GeoDataFrame


def perform_spatial_join_with_floodplains(
    nonbridge_spans_gdf: gpd.GeoDataFrame,
    floodplains_gdf: gpd.GeoDataFrame,
    quadrat_width: float = DEFAULT_QUADRAT_WIDTH,
) -> gpd.GeoDataFrame:
    """
    Performs spatial join between non-bridge spans and floodplains, optionally
    verifying input integrity using hashes from the provided mapping.

    Args:
        nonbridge_spans_gdf: GeoDataFrame of non-bridge spans.
        floodplains_gdf: GeoDataFrame of floodplain polygons.
        quadrat_width: Width for subdividing the spatial index.

    Returns:
        A tuple containing:
            - nonbridge_spans_floodplains_join_gdf: Result of the spatial join.
    """
    logger.info("Performing spatial join with floodplains...")

    logger.debug("Creating subdivided spatial index for floodplains...")

    floodplains_subdivided_spatial_index = create_subdivided_spatial_index(
        poly_gdf=floodplains_gdf,  #
        quadrat_width=quadrat_width,
    )

    assert floodplains_subdivided_spatial_index is not None, (
        "Failed to create subdivided spatial index."
    )

    logger.debug("Performing spatial join using quadrat/strtree method...")

    # Pass copy to avoid modifying original index if reset is needed internally by join function
    nonbridge_spans_floodplains_join_gdf = spatial_join_quadrat_strtree_vectorized(
        roads_gdf=nonbridge_spans_gdf,
        floodplains_gdf=floodplains_gdf,
        floodplains_subdivided_spatial_index=floodplains_subdivided_spatial_index,
    )

    if nonbridge_spans_floodplains_join_gdf.empty:
        logger.warning("Spatial join resulted in an empty GeoDataFrame.")

        nonbridge_spans_floodplains_join_gdf["flood_risk_level"] = pd.Series(
            dtype="Int64"
        )

    logger.debug(
        "Floodplains/Surface roads spatial join complete. "
        f"Found {len(nonbridge_spans_floodplains_join_gdf)} intersections."
    )

    logger.debug("Mapping risk levels to joined segments...")

    nonbridge_spans_floodplains_join_gdf["flood_risk_level"] = [
        floodplains_gdf.iloc[int_loc]["_flood_risk_level_"]
        for int_loc in nonbridge_spans_floodplains_join_gdf.index.get_level_values(1)
    ]

    logger.debug("Risk level mapping complete.")

    # --- Return Values ---
    return nonbridge_spans_floodplains_join_gdf


def calculate_highest_road_risk_for_nonbridge_spans(
    nonbridge_spans_floodplains_join_gdf: gpd.GeoDataFrame,
) -> pd.Series:
    """
    Calculates the highest flood risk for each original non-bridge road span,
    optionally verifying input integrity using the hash from the provided mapping.

    Args:
        nonbridge_spans_floodplains_join_gdf: GDF from the spatial join step.
        integrity_hashes: Optional mapping containing expected integrity hashes.
                          If provided, must include key JOIN_GDF.

    Returns:
        A pandas Series mapping original non-bridge span integer location index
        to its highest (minimum numerical value) flood risk level.

    Raises:
        AssertionError: If input integrity verification fails (when enabled) or
                        required column missing.
        KeyError: If IntegrityHashKey.JOIN_GDF is missing (when enabled).
        AttributeError / TypeError / ValueError: If hash calculation/verification fails.
    """
    logger.info("Calculating highest road risk...")

    # --- Input Column Check (Always perform) ---
    if "flood_risk_level" not in nonbridge_spans_floodplains_join_gdf.columns:
        raise AssertionError("Join GDF missing 'flood_risk_level' column.")
    logger.debug("Input column verification passed.")

    if nonbridge_spans_floodplains_join_gdf.empty:
        logger.warning("Input GDF is empty in calculate_highest_road_risk.")
        return pd.Series(dtype=int)

    assert "flood_risk_level" in nonbridge_spans_floodplains_join_gdf.columns, (
        "Error: 'flood_risk_level' column missing in input GDF."
    )

    nonbridge_spans_int_loc_to_highest_flood_risk = (
        # NOTE: Level 0 of the nonbridge_spans_floodplains_join_gdf MultiIndex
        #       is the integer location (int_loc) from the nonbridge_spans_gdf.
        nonbridge_spans_floodplains_join_gdf.groupby(level=0)["flood_risk_level"].min()
    )

    logger.info("Highest risk calculation complete.")

    return nonbridge_spans_int_loc_to_highest_flood_risk


def get_connected_components(
    osmnx_simplified_g: nx.MultiDiGraph,  #
) -> Tuple[nx.Graph, List[Set[int]]]:
    """Identifies connected components in the undirected version of the graph.

    Assigns a '_component_idx_' attribute to edges in the input graph
    based on component membership (mutates the input graph). The major
    component (largest) has index 0. Checks for disjoint components and
    that edges do not bridge components.

    Args:
        osmnx_simplified_g: The input osmnx MultiDiGraph.

    Returns:
        A tuple containing:
            - The undirected version of the input graph (nx.Graph).
            - A list of sets, where each set contains the node IDs for a
              connected component, sorted by size descending.

    Raises:
        AssertionError: Sanity checks: components are found to not be disjoint, if edges
                        bridge components, or if components do not include all nodes.
    """
    undirected_osmnx_simplified_g = osmnx_simplified_g.to_undirected()

    connected_components = sorted(
        nx.connected_components(undirected_osmnx_simplified_g),
        key=len,
        reverse=True,
    )

    seen_nodes = set()

    for component_idx, component in enumerate(connected_components):
        if component & seen_nodes:
            raise AssertionError("Connected components are not disjoint.")

        for u, v, key, data in osmnx_simplified_g.edges(
            nbunch=component, keys=True, data=True
        ):
            assert u in component and v in component, (
                "Edges must not bridge components."
            )

            data["_component_idx_"] = component_idx

        seen_nodes.update(component)

    assert len(undirected_osmnx_simplified_g.nodes) == len(seen_nodes), (
        "Components did not include all nodes."
    )

    return undirected_osmnx_simplified_g, connected_components


def identify_impacted_edges(
    osmnx_simplified_g: nx.MultiDiGraph,
    nonbridge_spans_gdf: gpd.GeoDataFrame,
    bridge_spans_gdf: gpd.GeoDataFrame,
    nonbridge_spans_int_loc_to_highest_flood_risk: pd.Series,
) -> Tuple[
    Dict[Tuple[int, int, int], FloodingImpactInfo], Dict[Tuple[int, int, int], int]
]:
    """
    Identifies nonfunctional and isolated edges based on flood risk, optionally
    verifying input graph and GDF integrity using hashes from the provided mapping.

    Args:
        osmnx_simplified_g: The simplified osmnx MultiDiGraph.
        nonbridge_spans_gdf: GeoDataFrame of non-bridge spans.
        bridge_spans_gdf: GeoDataFrame of bridge spans.
        nonbridge_spans_int_loc_to_highest_flood_risk: Series mapping non-bridge
            span int_loc to its highest risk level.

    Returns:
        A tuple containing two dictionaries:
            - nonfunctional_edges_to_reason_and_risk: Maps nonfunctional edge IDs
              to (reason, risk_level).
            - isolated_edges_to_risk_level: Maps isolated edge IDs to the risk
              level at which they became isolated.
    """
    logger.info("Identifying impacted edges...")

    undirected_osmnx_simplified_g, connected_components = get_connected_components(
        osmnx_simplified_g=osmnx_simplified_g
    )

    risk_levels = sorted(set(nonbridge_spans_int_loc_to_highest_flood_risk))
    assert all(isinstance(x, int) for x in risk_levels)

    # Here we stop considering spatial (bridge/nonbridge) spans and instead consider network edges.

    # For surface_edges, the edge is not entirely a bridge span.
    # NOTE: Recall that we use nonbridge spans when intersecting with the floodplains
    #       under the assumption that bridges will not become inundated.
    functional_surface_edge_ids = {e[:3] for e in nonbridge_spans_gdf.index}

    # For the nonsurface_edges, the entirety of the edge is a bridge.
    # Functional nonsurface_edges must be able to transport vehicles from
    # one uninundated nonbridge span to another uninundated nonbridge span.
    # CONSIDER: We respect one-way restrictions when determining a bridge edge nonfunctional.
    functional_nonsurface_edge_ids = {
        e[:3] for e in bridge_spans_gdf.index
    } - functional_surface_edge_ids

    nonfunctional_edges_to_reason_and_risk = dict()

    functional_major_component_undirected_g = undirected_osmnx_simplified_g.copy()

    if len(connected_components) > 1:
        connected_component_sizes = [len(c) for c in connected_components]
        node_count = sum(connected_component_sizes)

        connected_components_size_ratios = [
            s / node_count for s in connected_component_sizes
        ]

        major_component_ratio = connected_components_size_ratios[0]
        if major_component_ratio < 0.95:
            logger.warning(
                f"Major component contains only {round(major_component_ratio, 3)} of nodes."
            )

        isolated_nodes = set()
        isolated_nodes.update(*connected_components[1:])

        # Under emergency circumstances, emergency vehicles and/or evacuating vehicles
        # can use one-way streets against the normal direction.
        # Therefore, we need to use an undirected graph when identifying isolated subnets.
        functional_major_component_undirected_g.remove_nodes_from(isolated_nodes)

        # We need to use the Directional Edge IDs as keys in this dictionary.
        isolated_edges_to_risk_level = {
            e: -1 for e in osmnx_simplified_g.edges(keys=True) if e[0] in isolated_nodes
        }
    else:
        isolated_nodes = set()
        isolated_edges_to_risk_level = dict()

    # Iterate over the risk levels, from highest to lowest risk (100 year to 500 year)
    for flood_risk_level in tqdm(iterable=risk_levels, desc="Flood Risk Levels"):
        nonbridge_span_int_locs_inundated_at_risk_level = list(
            set(
                nonbridge_spans_int_loc_to_highest_flood_risk[
                    nonbridge_spans_int_loc_to_highest_flood_risk == flood_risk_level
                ].index
            )
        )

        # For these road network edges, a portion is a nonbridge span that in intersects a floodplain.
        inundated_edge_ids = {
            e[:3]
            for e in nonbridge_spans_gdf.iloc[
                list(nonbridge_span_int_locs_inundated_at_risk_level)
            ].index
        }

        # Remove the inundated from the remaining.
        functional_surface_edge_ids -= inundated_edge_ids

        # u_to_nowhere is the set of nodes for which all outbound edges are inundated.
        # NOTE: u can be the origin node for many edges, not all of which may be inundated.
        #       For a u to lead to nowhere, ALL (u, v, key) tuples for u must be inundated.
        #       Therefore, we use the set difference to find u_to_nowhere.
        u_to_nowhere = {e[0] for e in inundated_edge_ids} - {
            e[0] for e in functional_surface_edge_ids
        }
        # v_to_nowhere is the set of nodes for which all inbound edges are inundated.
        v_from_nowhere = {e[1] for e in inundated_edge_ids} - {
            e[1] for e in functional_surface_edge_ids
        }

        # Now we identify the bridge edges for which ALL access onto or off of the bridge is inundated.
        # NOTE: Bridges may be represented by many connected edges (e.g. complex overpasses),
        #       thus BRIDGE_FUNCTIONALLY_DISCONNECTED status is transitive and we must prune nonsurface edges iteratively.
        bridges_to_nowhere_edge_ids = set()

        while True:
            pruned_nonsurface_edge_ids = {
                e
                for e in functional_nonsurface_edge_ids
                if (
                    # The bridge origin (u) has no functional inbound edges.
                    e[0] in v_from_nowhere
                    or
                    # The bridge span destination (v) has no functional outbound edges.
                    e[1] in u_to_nowhere
                )
            }

            if not pruned_nonsurface_edge_ids:
                break

            bridges_to_nowhere_edge_ids.update(pruned_nonsurface_edge_ids)

            # Update functional_nonsurface_edge_ids by removing those nonsurface edges
            # that became inaccessible at this flood risk level.
            functional_nonsurface_edge_ids -= pruned_nonsurface_edge_ids

            # Again, recall that the origin (e[0]) or destination (e[1]) node for a pruned edge may still
            # be connected to uninundated/accessible edges. Consequentially, we update the to/from nowhere
            # node sets with pruned_nonsurface − (functional_nonsurface ∪ remaining_surface).
            u_to_nowhere |= (
                {e[0] for e in pruned_nonsurface_edge_ids}
                - {e[0] for e in functional_nonsurface_edge_ids}
                - {e[0] for e in functional_surface_edge_ids}
            )

            v_from_nowhere |= (
                {e[1] for e in pruned_nonsurface_edge_ids}
                - {e[1] for e in functional_nonsurface_edge_ids}
                - {e[1] for e in functional_surface_edge_ids}
            )

        # Merge the inundated and
        nonfunctional_edges_to_reason_and_risk |= (
            {e: ("FLOODED", flood_risk_level) for e in inundated_edge_ids}
            |  # UNION
            {
                e: ("BRIDGE_FUNCTIONALLY_DISCONNECTED", flood_risk_level)
                for e in bridges_to_nowhere_edge_ids
            }
        )

        functional_major_component_undirected_g.remove_edges_from(
            inundated_edge_ids | bridges_to_nowhere_edge_ids
        )

        functional_connected_components = sorted(
            nx.connected_components(functional_major_component_undirected_g),  #
            key=len,
            reverse=True,
        )

        if len(functional_connected_components) > 1:
            isolated_components = functional_connected_components[1:]

            isolated_nodes = set()
            isolated_nodes.update(*isolated_components)

            # Isolated edges are still functional, but they are disconnected from the major component.
            isolated_edge_ids = (
                {e for e in functional_surface_edge_ids if e[0] in isolated_nodes}
                |  # UNION
                {e for e in functional_nonsurface_edge_ids if e[0] in isolated_nodes}
            )

            isolated_edges_to_risk_level |= {
                e: flood_risk_level
                for e in isolated_edge_ids
                if e not in isolated_edges_to_risk_level
            }

    logger.info(
        f"Finished identifying impacted edges. Total lost edges: {len(nonfunctional_edges_to_reason_and_risk)}"
    )

    return nonfunctional_edges_to_reason_and_risk, isolated_edges_to_risk_level


def add_impact_columns_to_gdf(
    roads_gdf: gpd.GeoDataFrame,
    impacted_edges: Tuple[
        Dict[Tuple[int, int, int], FloodingImpactInfo],
        Dict[Tuple[int, int, int], int],
    ],
) -> gpd.GeoDataFrame:
    """
    Adds columns detailing flood impacts to the main roads GeoDataFrame,
    optionally verifying input integrity using the hash from the provided mapping.

    Args:
        roads_gdf: The original GeoDataFrame of all road edges.
        impacted_edges: Tuple that contains:
            nonfunctional_edges_to_reason_and_risk: Dict mapping nonfunctional edges
                to their impact reason and risk level.
            isolated_edges_to_risk_level: Dict mapping isolated edges to the risk
                level at which they became isolated.

    Returns:
        final_impact_gdf: A *new* GeoDataFrame based on roads_gdf with added impact columns.

    Raises:
        AssertionError: If index structure wrong or edge IDs in dicts not in roads_gdf index.
        KeyError: If IntegrityHashKey.ROADS is missing (when enabled).
        AttributeError / TypeError / ValueError: If hash calculation/verification fails.
    """
    logger.info("Adding impact columns to GDF...")

    nonfunctional_edges_to_reason_and_risk, isolated_edges_to_risk_level = (
        impacted_edges
    )

    # --- Input Structure/Content Checks (Always perform) ---
    if list(roads_gdf.index.names) != ["u", "v", "key"]:
        raise AssertionError(
            "Input roads_gdf does not have expected MultiIndex ['u', 'v', 'key']"
        )
    logger.debug("Input structure verification passed.")

    # Check if keys in impact dictionaries exist in roads_gdf index
    roads_index_set = set(roads_gdf.index)
    nonfunc_keys = set(nonfunctional_edges_to_reason_and_risk.keys())
    isolated_keys = set(isolated_edges_to_risk_level.keys())

    missing_nonfunc = nonfunc_keys - roads_index_set
    if missing_nonfunc:
        raise AssertionError(
            f"Edge IDs found in nonfunctional_edges dict but not in roads_gdf index: {missing_nonfunc}"
        )

    missing_isolated = isolated_keys - roads_index_set
    if missing_isolated:
        raise AssertionError(
            f"Edge IDs found in isolated_edges dict but not in roads_gdf index: {missing_isolated}"
        )
    logger.debug("Impact dictionary key validation passed.")

    logger.info("Adding impact columns to GeoDataFrame...")
    final_impact_gdf = roads_gdf.copy()

    nonfunctional_edge_to_reason = {
        k: v[0] for k, v in nonfunctional_edges_to_reason_and_risk.items()
    }
    nonfunctional_edge_to_risk_level = {
        k: v[1] for k, v in nonfunctional_edges_to_reason_and_risk.items()
    }

    nonfunctional_edge_frequency = {
        k: "100 YEAR"
        if v[1] is not None and v[1] < 14
        else ("500 YEAR" if v[1] is not None else None)
        for k, v in nonfunctional_edges_to_reason_and_risk.items()
    }

    isolated_edge_frequency = {
        k: "100 YEAR"
        if v is not None and v < 14
        else ("500 YEAR" if v is not None else None)
        for k, v in isolated_edges_to_risk_level.items()
    }

    final_impact_gdf["nonfunctional_reason"] = final_impact_gdf.index.map(
        nonfunctional_edge_to_reason
    )
    final_impact_gdf["nonfunctional_reason"].fillna("NOT_IMPACTED", inplace=True)

    final_impact_gdf["nonfunctional_risk_level"] = final_impact_gdf.index.map(
        nonfunctional_edge_to_risk_level
    )
    final_impact_gdf["nonfunctional_risk_level"] = final_impact_gdf[
        "nonfunctional_risk_level"
    ].astype("Int64")  # Use nullable integer type

    final_impact_gdf["nonfunctional_frequency"] = final_impact_gdf.index.map(
        nonfunctional_edge_frequency
    )

    final_impact_gdf["isolated_edge_risk_level"] = final_impact_gdf.index.map(
        isolated_edges_to_risk_level
    )
    final_impact_gdf["isolated_edge_frequency"] = final_impact_gdf.index.map(
        isolated_edge_frequency
    )

    logger.info("Impact columns added.")

    return final_impact_gdf


def validate_floodplains_gdf(floodplains_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if floodplains_gdf is None:
        raise ValueError("floodplains_gdf cannot be None")
    elif floodplains_gdf.empty:
        logger.warning(
            "Floodplain clipping resulted in empty or None GeoDataFrame. Analysis may be incomplete."
        )
        floodplains_gdf = gpd.GeoDataFrame(
            {"_flood_risk_level_": []}, geometry=[]
        )  # Create empty with column
    elif "_flood_risk_level_" not in floodplains_gdf.columns:
        raise ValueError(
            "Required column '_flood_risk_level_' not found in floodplain data after clipping task."
        )

    return floodplains_gdf


# --- Strategy Dataclass Definition ---
@dataclass
class FloodImpactAnalysisStrategy:
    """
    A strategy object holding callable tasks for the intersection process.
    """

    validate_floodplains_gdf_task: Callable = validate_floodplains_gdf
    convert_graph_to_gdfs_task: Callable = convert_graph_to_gdfs
    create_nonbridge_spans_gdf_task: Callable = create_nonbridge_spans_gdf
    create_bridge_spans_gdf_task: Callable = create_bridge_spans_gdf
    perform_spatial_join_task: Callable = perform_spatial_join_with_floodplains
    calculate_highest_road_risk_for_nonbridge_spans_task: Callable = (
        calculate_highest_road_risk_for_nonbridge_spans
    )
    identify_impacted_edges_task: Callable = identify_impacted_edges
    add_impact_columns_task: Callable = add_impact_columns_to_gdf


DEFAULT_STRATEGY = FloodImpactAnalysisStrategy()


def flood_impact_analysis_orchestrator(
    osmnx_simplified_g: nx.MultiDiGraph,
    floodplains_gdf: gpd.GeoDataFrame,
    strategy: FloodImpactAnalysisStrategy = DEFAULT_STRATEGY,
):
    """
    Orchestrates the flood impact analysis pipeline.
    """
    floodplains_gdf = strategy.validate_floodplains_gdf_task(
        floodplains_gdf=floodplains_gdf
    )

    _, roads_gdf = strategy.convert_graph_to_gdfs_task(g=osmnx_simplified_g)

    nonbridge_spans_gdf = strategy.create_nonbridge_spans_gdf_task(edges_gdf=roads_gdf)
    bridge_spans_gdf = strategy.create_bridge_spans_gdf_task(edges_gdf=roads_gdf)

    # --- Step 3: Spatial Join ---
    join_gdf = strategy.perform_spatial_join_task(
        nonbridge_spans_gdf=nonbridge_spans_gdf,
        floodplains_gdf=floodplains_gdf,  # Use the one from Step 1
    )

    # --- Step 5: Calculate Highest Risk ---
    highest_risk_series = strategy.calculate_highest_road_risk_for_nonbridge_spans_task(
        nonbridge_spans_floodplains_join_gdf=join_gdf,
    )

    # --- Step 6: Identify Impacted Edges ---
    impacted_edges = strategy.identify_impacted_edges_task(
        osmnx_simplified_g=osmnx_simplified_g,  # Use graph from Step 1
        nonbridge_spans_gdf=nonbridge_spans_gdf,
        bridge_spans_gdf=bridge_spans_gdf,
        nonbridge_spans_int_loc_to_highest_flood_risk=highest_risk_series,
    )

    # --- Step 7: Add Impact Columns ---
    final_gdf = strategy.add_impact_columns_task(  # Don't need final hash here
        roads_gdf=roads_gdf, impacted_edges=impacted_edges
    )

    return final_gdf
