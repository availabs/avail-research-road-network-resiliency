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
import uuid
import weakref
from dataclasses import dataclass
from enum import Enum, auto  # Import Enum and auto
from typing import (
    Any,
    Dict,
    List,
    Literal,
    Mapping,
    Optional,
    Set,
    Tuple,
    TypeAlias,
    Union,
)

import geopandas as gpd
import networkx as nx
import osmnx as ox
import pandas as pd

from common.fema.floodplains.core import (
    create_subdivided_spatial_index,
    spatial_join_quadrat_strtree_vectorized,
)

# Import functions directly from the common package.
from common.osm.enrich import (
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


# --- Type Alias for supported objects ---
SupportedObjectHashingTypes: TypeAlias = Union[
    pd.DataFrame,
    gpd.GeoDataFrame,
    nx.Graph,
    nx.DiGraph,
    nx.MultiGraph,
    nx.MultiDiGraph,
]

# Cache for calculated integrity hash using WeakKeyDictionary
# Keys are the actual objects (weakly referenced)
_integrity_hash_cache: weakref.WeakKeyDictionary = weakref.WeakKeyDictionary()


# --- Helper Function to Get or Assign Object UUID ---
def _get_or_assign_object_uuid(data_object: SupportedObjectHashingTypes) -> uuid.UUID:
    """
    Retrieves the integrity UUID from an object, assigning a new one if not present.
    Handles both GeoDataFrames (.attrs) and NetworkX graphs (.graph dict).

    Args:
        data_object: The DataFrame, GeoDataFrame, or Graph.

    Returns:
        The existing or newly assigned UUID.

    Raises:
        AttributeError: If the object is None or doesn't support attributes
                        (.attrs for GDF, .graph for NX graph).
        TypeError: If the object is not a supported type.
    """
    if data_object is None:
        raise AttributeError("Cannot assign UUID to a None object.")

    obj_type = get_hashable_object_type(data_object)  # Reuse type checker

    if obj_type == HashableObjectType.DATA_FRAME:
        if not hasattr(data_object, "attrs"):
            # Should not happen with modern GeoPandas/Pandas, but safeguard
            raise AttributeError("DataFrame object lacks .attrs dictionary.")
        # Use .setdefault() for atomic check and assignment
        return data_object.attrs.setdefault(INTEGRITY_UUID_ATTR, uuid.uuid4())

    elif obj_type == HashableObjectType.GRAPH:
        if not hasattr(data_object, "graph") or not isinstance(data_object.graph, dict):
            raise AttributeError(
                "NetworkX graph object lacks .graph dictionary attribute."
            )
        # Use .setdefault() for atomic check and assignment
        return data_object.graph.setdefault(INTEGRITY_UUID_ATTR, uuid.uuid4())

    # Should be unreachable due to get_hashable_object_type check, but safeguard
    raise TypeError(
        f"Object type {type(data_object)} does not support UUID attribute storage."
    )


# --- Helper Function to Determine Object Type for Hashing ---
def get_hashable_object_type(data_object: Any) -> HashableObjectType:
    """
    Determines the hashable type category of the input object.

    Args:
        data_object: The object to categorize.

    Returns:
        The corresponding HashableObjectType Enum member.

    Raises:
        TypeError: If the data_object is not a supported type for hashing.
    """
    if isinstance(data_object, (nx.Graph, nx.DiGraph, nx.MultiGraph, nx.MultiDiGraph)):
        return HashableObjectType.GRAPH
    elif isinstance(data_object, (pd.DataFrame, gpd.GeoDataFrame)):
        return HashableObjectType.DATA_FRAME
    else:
        raise TypeError(
            f"Unsupported object type for integrity hashing: {type(data_object)}"
        )

    # --- Unified Helper Function for Single Hash Calculation (Memoized with WeakKeyDictionary) ---


# --- Unified Helper Function for Single Hash Calculation (Memoized with UUID via WeakKeyDictionary) ---
def calculate_integrity_hash(data_object: SupportedObjectHashingTypes) -> int:
    """
    Calculates a consistent integer hash for a single supported data object.
    Uses memoization via a WeakKeyDictionary keyed by a UUID stored within the
    object's attributes (.attrs or .graph). Cache is also explicitly cleared by
    initialize_flood_impact_data.
    - For DataFrames/GeoDataFrames: Hashes the index.
    - For NetworkX Graphs: Hashes the structure (nodes and edges).

    Args:
        data_object: The object to hash (DataFrame, GeoDataFrame, or Graph).

    Returns:
        An integer representing the calculated hash.

    Raises:
        TypeError: If the data_object is not of a supported type, or if hashing fails
                   due to non-hashable elements within the object (e.g., index, nodes).
        AttributeError: If the object is None or lacks necessary attributes (e.g., index).
    """
    if data_object is None:
        logger.error("Attempted to calculate hash of a None object.")
        raise AttributeError("Input data_object cannot be None.")

    # Get (or assign) the object's persistent UUID for cache keying
    object_uuid = _get_or_assign_object_uuid(data_object)

    # Check cache first using the object's UUID
    if object_uuid in _integrity_hash_cache:
        logger.debug(
            f"Cache hit for integrity hash calculation (object uuid: {object_uuid})"
        )
        return _integrity_hash_cache[object_uuid]

    logger.debug(
        f"Cache miss for integrity hash calculation (object uuid: {object_uuid}). Calculating..."
    )
    object_type = get_hashable_object_type(data_object)  # Determine type

    calculated_hash: Optional[int] = None
    try:
        if object_type == HashableObjectType.DATA_FRAME:
            # Hash the index for DataFrames/GeoDataFrames
            if not hasattr(data_object, "index") or data_object.index is None:
                raise AttributeError(
                    "DataFrame/GeoDataFrame object or its index is None."
                )
            calculated_hash = int(
                pd.util.hash_pandas_object(data_object.index, index=False).sum()
            )

        elif object_type == HashableObjectType.GRAPH:
            # Hash the structure (nodes, edges) for Graphs
            sorted_nodes = tuple(sorted(data_object.nodes()))
            if isinstance(data_object, (nx.MultiGraph, nx.MultiDiGraph)):
                sorted_edges = tuple(sorted(data_object.edges(keys=True)))
            else:
                sorted_edges = tuple(sorted(data_object.edges()))
            graph_representation = (sorted_nodes, sorted_edges)
            calculated_hash = hash(graph_representation)

        # Store in cache (using UUID as key) and return
        if calculated_hash is not None:
            _integrity_hash_cache[object_uuid] = calculated_hash  # Store using UUID key
            logger.debug(
                f"Calculated and cached hash for object uuid {object_uuid}: {calculated_hash}"
            )
            return calculated_hash
        else:
            raise TypeError(
                f"Failed to calculate hash for supported type: {object_type.name}"
            )

    except TypeError as e:
        logger.error(
            f"Error calculating hash for {object_type.name} (potential unhashable elements?): {e}"
        )
        raise
    except AttributeError as e:
        logger.error(f"Error calculating hash for {object_type.name}: {e}")
        raise


# --- Helper Function for Calculating Multiple Hashes ---
def calculate_integrity_hashes(
    objects_to_hash: Dict[IntegrityHashKey, SupportedObjectHashingTypes],
) -> Dict[IntegrityHashKey, int]:
    """
    Calculates integrity hashes for multiple data objects provided in a dictionary.

    Iterates through the input dictionary, calling calculate_integrity_hash for each object.

    Args:
        objects_to_hash: Dictionary mapping IntegrityHashKey Enum members to the
                         actual data objects (Graph, DataFrame, GeoDataFrame) to hash.

    Returns:
        A dictionary mapping the same IntegrityHashKey Enum members to their
        calculated integer hash values.

    Raises:
        TypeError: If any object in `objects_to_hash` is not of a supported type.
        AttributeError: If hashing fails due to missing attributes (e.g., None object).
    """
    logger.debug(
        f"Calculating integrity hashes for keys: {[k.name for k in objects_to_hash.keys()]}"
    )
    calculated_hashes: Dict[IntegrityHashKey, int] = {}
    for key, data_object in objects_to_hash.items():
        object_name = key.name
        logger.debug(f"-- Calculating hash for: {object_name}")
        try:
            calculated_hashes[key] = calculate_integrity_hash(data_object)
        except (AttributeError, TypeError, ValueError) as e:
            # Catch errors from the single calculation function
            logger.error(f"Failed to calculate hash for {object_name}: {e}")
            raise  # Re-raise to indicate failure
    logger.debug("Finished calculating integrity hashes.")
    return calculated_hashes


# --- Helper Function for Verifying Multiple Hashes ---
def verify_integrity_hashes(
    actual_objects: Dict[IntegrityHashKey, Any],
    expected_hashes: Mapping[IntegrityHashKey, int],
) -> None:
    """
    Verifies the integrity hashes for multiple data objects against a mapping
    of expected hashes.

    Iterates through the keys provided in `actual_objects`, calculates the hash
    for each corresponding object using calculate_integrity_hash(), and compares
    it to the value found in `expected_hashes`.

    Args:
        actual_objects: Dictionary mapping IntegrityHashKey Enum members to the
                        actual data objects (Graph, DataFrame, GeoDataFrame) to verify.
        expected_hashes: Mapping containing the pre-calculated hash values expected
                         for the keys present in `actual_objects`.

    Raises:
        AssertionError: If any calculated hash does not match the expected hash.
        KeyError: If an expected key from `actual_objects` is not found in `expected_hashes`.
        TypeError: If any object in `actual_objects` is not of a supported type for hashing.
        AttributeError: If hashing fails due to missing attributes (e.g., None object).
    """
    logger.debug(
        f"Verifying integrity hashes for keys: {[k.name for k in actual_objects.keys()]}"
    )

    for key, data_object in actual_objects.items():
        object_name = key.name  # Use Enum member name for description
        logger.debug(f"-- Verifying: {object_name}")

        try:
            # Lookup the expected hash first
            expected_hash = expected_hashes[key]
        except KeyError:
            logger.error(
                f"Integrity check failed: Expected hash key '{object_name}' not found in dictionary."
            )
            raise KeyError(
                f"Expected hash key '{object_name}' not found in expected_hashes dictionary."
            )

        current_hash = None
        try:
            # Calculate current hash using the unified single-object function
            current_hash = calculate_integrity_hash(data_object)

            # Compare calculated hash with the expected hash
            if current_hash != expected_hash:
                # Determine hash type string for clearer error message (optional)
                try:
                    obj_type_enum = get_hashable_object_type(data_object)
                    hash_type_str = (
                        "Graph structure"
                        if obj_type_enum == HashableObjectType.GRAPH
                        else "Index"
                    )
                except TypeError:
                    hash_type_str = "Object"  # Fallback

                raise AssertionError(
                    f"{object_name} {hash_type_str} integrity hash mismatch! "
                    f"Expected {expected_hash}, got {current_hash}"
                )
            logger.debug(f"-- Verified successfully: {object_name}")

        except (AttributeError, TypeError, ValueError, AssertionError) as e:
            # Catch specific errors from hashing/verification or type checks
            logger.error(f"Integrity check failed for {object_name}: {e}")
            # Re-raise the caught exception to indicate a critical failure
            raise
    logger.debug("All requested integrity hashes verified successfully.")


# --- Dataclass for Initial Data ---
@dataclass
class InitialImpactData:
    """
    Holds the initial data structures and their integrity hashes (in a mapping
    keyed by IntegrityHashKey Enum) for the flood impact analysis pipeline.
    """

    osmnx_simplified_g: nx.MultiDiGraph
    roads_gdf: gpd.GeoDataFrame
    bridge_spans_gdf: gpd.GeoDataFrame
    nonbridge_spans_gdf: gpd.GeoDataFrame
    floodplains_gdf: gpd.GeoDataFrame
    integrity_hashes: Optional[Mapping[IntegrityHashKey, int]]  # Use Mapping Type Hint
    # context_uuid removed


# --- Refactored Function Skeletons ---


def initialize_flood_impact_data(
    osmnx_simplified_g: nx.MultiDiGraph,
    floodplains_gdf: gpd.GeoDataFrame,
    generate_integrity_hashes: bool = True,  # New parameter
) -> InitialImpactData:
    """
    Initializes necessary GeoDataFrames and graph for flood impact analysis.
    Optionally bundles them with their integrity hashes keyed by the
    IntegrityHashKey Enum. **Clears the integrity hash memoization cache**
    at the start of execution.

    Args:
        osmnx_simplified_g: The simplified osmnx MultiDiGraph for the region.
        floodplains_gdf: GeoDataFrame of floodplain polygons, must include
                         a '_flood_risk_level_' column.
        generate_integrity_hashes: If True (default), calculate and include
                                   integrity hashes. If False, the
                                   integrity_hashes field in the returned
                                   dataclass will be None.

    Returns:
        An InitialImpactData dataclass instance containing the initial graph, GDFs,
        and optionally a mapping of their integrity hashes.

    Raises:
        AssertionError: If input floodplains_gdf is missing the required column.
        ValueError: If input graph or floodplains_gdf is None.
        AttributeError / TypeError / ValueError: If hash calculation fails (when enabled).
    """
    logger.info("Initializing flood impact data...")
    if generate_integrity_hashes:
        logger.info("Integrity hash generation enabled.")
    else:
        logger.info("Integrity hash generation disabled.")

    # --- Input Validation ---
    if osmnx_simplified_g is None:
        raise ValueError("Input osmnx_simplified_g cannot be None.")
    if floodplains_gdf is None:
        raise ValueError("Input floodplains_gdf cannot be None.")
    if "_flood_risk_level_" not in floodplains_gdf.columns:
        raise AssertionError(
            "Input floodplains_gdf MUST have the _flood_risk_level_ column"
        )
    logger.debug("Initial input validation passed.")

    # --- Core Logic (Placeholder - Keep existing logic here) ---
    # This logic runs regardless of hash generation
    roads_gdf = ox.convert.graph_to_gdfs(G=osmnx_simplified_g, nodes=False)
    bridge_spans_gdf = create_bridge_spans_gdf(edges_gdf=roads_gdf)
    nonbridge_spans_gdf = create_nonbridge_spans_gdf(edges_gdf=roads_gdf)

    # --- Calculate Hashes (Conditional) ---
    integrity_hashes_dict: Optional[Dict[IntegrityHashKey, int]] = None

    if generate_integrity_hashes:
        # This will also assign UUIDs to the objects if not already present
        initial_objects_to_hash = {
            IntegrityHashKey.GRAPH: osmnx_simplified_g,
            IntegrityHashKey.ROADS: roads_gdf,
            IntegrityHashKey.BRIDGE_SPANS: bridge_spans_gdf,
            IntegrityHashKey.NONBRIDGE_SPANS: nonbridge_spans_gdf,
            IntegrityHashKey.FLOODPLAINS: floodplains_gdf,
        }
        # Errors will propagate up from calculate_integrity_hashes
        integrity_hashes_dict = calculate_integrity_hashes(initial_objects_to_hash)
        logger.debug("Initial integrity hashes calculated using plural function.")

    # --- Return Value (Dataclass instance) ---
    # The dictionary will conform to the Optional[Mapping] type hint
    return InitialImpactData(
        osmnx_simplified_g=osmnx_simplified_g,
        roads_gdf=roads_gdf,
        bridge_spans_gdf=bridge_spans_gdf,
        nonbridge_spans_gdf=nonbridge_spans_gdf,
        floodplains_gdf=floodplains_gdf,
        integrity_hashes=integrity_hashes_dict,
    )


def perform_spatial_join_with_floodplains(
    nonbridge_spans_gdf: gpd.GeoDataFrame,
    floodplains_gdf: gpd.GeoDataFrame,
    quadrat_width: float = DEFAULT_QUADRAT_WIDTH,
    integrity_hashes: Optional[Mapping[IntegrityHashKey, int]] = None,  # Moved to last
) -> Tuple[gpd.GeoDataFrame, Optional[int]]:  # Return Optional hash
    """
    Performs spatial join between non-bridge spans and floodplains, optionally
    verifying input integrity using hashes from the provided mapping.

    Args:
        nonbridge_spans_gdf: GeoDataFrame of non-bridge spans.
        floodplains_gdf: GeoDataFrame of floodplain polygons.
        quadrat_width: Width for subdividing the spatial index.
        integrity_hashes: Optional mapping containing expected integrity hashes.
                          If provided, must include keys NONBRIDGE_SPANS and FLOODPLAINS.

    Returns:
        A tuple containing:
            - nonbridge_spans_floodplains_join_gdf: Result of the spatial join.
            - join_gdf_integrity_hash: Hash of the resulting join GDF's index,
                                       or None if integrity_hashes was None.

    Raises:
        AssertionError: If input integrity verification fails (when enabled).
        KeyError: If required keys are missing from integrity_hashes (when enabled).
        AttributeError / TypeError / ValueError: If hash calculation/verification fails.
    """
    logger.info("Performing spatial join with floodplains...")

    # --- Optional Verification ---
    if integrity_hashes is not None:
        logger.debug("Integrity hash verification enabled.")
        objects_to_verify = {
            IntegrityHashKey.NONBRIDGE_SPANS: nonbridge_spans_gdf,
            IntegrityHashKey.FLOODPLAINS: floodplains_gdf,
        }
        # Errors propagate from verify_integrity_hashes
        verify_integrity_hashes(objects_to_verify, integrity_hashes)
    else:
        logger.debug("Integrity hash verification skipped.")

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

    # --- Calculate Hash for Output (only if verification was enabled) ---

    join_gdf_integrity_hash: Optional[int] = None
    if integrity_hashes is not None:
        # Errors propagate from calculate_integrity_hash
        join_gdf_integrity_hash = calculate_integrity_hash(
            nonbridge_spans_floodplains_join_gdf
        )
        logger.debug(
            f"Integrity hash calculated for join GDF: {join_gdf_integrity_hash}"
        )

    # --- Return Values ---
    return nonbridge_spans_floodplains_join_gdf, join_gdf_integrity_hash


def calculate_highest_road_risk_for_nonbridge_spans(
    nonbridge_spans_floodplains_join_gdf: gpd.GeoDataFrame,
    integrity_hashes: Optional[Mapping[IntegrityHashKey, int]] = None,  # Moved to last
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

    # --- Optional Verification ---
    if integrity_hashes is not None:
        logger.debug("Integrity hash verification enabled.")
        objects_to_verify = {
            IntegrityHashKey.JOIN_GDF: nonbridge_spans_floodplains_join_gdf
        }
        # Errors propagate from verify_integrity_hashes
        verify_integrity_hashes(objects_to_verify, integrity_hashes)
    else:
        logger.debug("Integrity hash verification skipped.")

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
    integrity_hashes: Optional[Mapping[IntegrityHashKey, int]] = None,  # Moved to last
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
        integrity_hashes: Optional mapping containing expected integrity hashes.
                          If provided, must include keys GRAPH, NONBRIDGE_SPANS,
                          and BRIDGE_SPANS.

    Returns:
        A tuple containing two dictionaries:
            - nonfunctional_edges_to_reason_and_risk: Maps nonfunctional edge IDs
              to (reason, risk_level).
            - isolated_edges_to_risk_level: Maps isolated edge IDs to the risk
              level at which they became isolated.

    Raises:
        AssertionError: If input integrity verification fails (when enabled).
        KeyError: If required keys are missing from integrity_hashes (when enabled).
        AttributeError / TypeError / ValueError: If hash calculation/verification fails.
    """
    logger.info("Identifying impacted edges...")

    # --- Optional Verification ---
    if integrity_hashes is not None:
        logger.debug("Integrity hash verification enabled.")
        objects_to_verify = {
            IntegrityHashKey.GRAPH: osmnx_simplified_g,
            IntegrityHashKey.NONBRIDGE_SPANS: nonbridge_spans_gdf,
            IntegrityHashKey.BRIDGE_SPANS: bridge_spans_gdf,
        }
        # Errors propagate from verify_integrity_hashes
        verify_integrity_hashes(objects_to_verify, integrity_hashes)
    else:
        logger.debug("Integrity hash verification skipped.")

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
    for flood_risk_level in risk_levels:
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
    nonfunctional_edges_to_reason_and_risk: Dict[
        Tuple[int, int, int], FloodingImpactInfo
    ],
    isolated_edges_to_risk_level: Dict[Tuple[int, int, int], int],
    integrity_hashes: Optional[Mapping[IntegrityHashKey, int]] = None,  # Moved to last
) -> Tuple[gpd.GeoDataFrame, Optional[int]]:  # Return Optional hash
    """
    Adds columns detailing flood impacts to the main roads GeoDataFrame,
    optionally verifying input integrity using the hash from the provided mapping.

    Args:
        roads_gdf: The original GeoDataFrame of all road edges.
        nonfunctional_edges_to_reason_and_risk: Dict mapping nonfunctional edges
            to their impact reason and risk level.
        isolated_edges_to_risk_level: Dict mapping isolated edges to the risk
            level at which they became isolated.
        integrity_hashes: Optional mapping containing expected integrity hashes.
                          If provided, must include key ROADS.

    Returns:
        A tuple containing:
            - final_impact_gdf: A *new* GeoDataFrame based on roads_gdf with
                                added impact columns.
            - final_impact_gdf_integrity_hash: Hash of the final GDF index,
                                               or None if integrity_hashes was None.

    Raises:
        AssertionError: If input integrity verification fails (when enabled),
                        index structure wrong, edge IDs in dicts not in roads_gdf index,
                        or index hash changes unexpectedly during processing.
        KeyError: If IntegrityHashKey.ROADS is missing (when enabled).
        AttributeError / TypeError / ValueError: If hash calculation/verification fails.
    """
    logger.info("Adding impact columns to GDF...")

    # --- Optional Verification ---
    expected_roads_integrity_hash: Optional[int] = None  # Store expected hash if needed
    if integrity_hashes is not None:
        logger.debug("Integrity hash verification enabled.")
        # Extract expected hash first to use later in post-condition check
        try:
            expected_roads_integrity_hash = integrity_hashes[IntegrityHashKey.ROADS]
        except KeyError:
            logger.error(
                "Integrity check failed: Expected hash key 'ROADS' not found in dictionary."
            )
            raise KeyError(
                "Expected hash key 'ROADS' not found in integrity_hashes dictionary."
            )

        objects_to_verify = {IntegrityHashKey.ROADS: roads_gdf}
        # Errors propagate from verify_integrity_hashes
        verify_integrity_hashes(objects_to_verify, integrity_hashes)
    else:
        logger.debug("Integrity hash verification skipped.")

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

    # --- Calculate Hash for Output & Final Verification (only if verification was enabled) ---
    final_impact_gdf_integrity_hash: Optional[int] = None
    if integrity_hashes is not None:
        # Errors propagate from calculate_integrity_hash
        final_impact_gdf_integrity_hash = calculate_integrity_hash(final_impact_gdf)
        # Verify index hasn't changed unexpectedly (using the stored expected hash)
        if final_impact_gdf_integrity_hash != expected_roads_integrity_hash:
            raise AssertionError(
                f"Index integrity hash changed unexpectedly during impact column addition! "
                f"Expected {expected_roads_integrity_hash}, got {final_impact_gdf_integrity_hash}"
            )
        logger.debug(
            f"Integrity hash calculated and verified for final GDF: {final_impact_gdf_integrity_hash}"
        )

    # --- Return Values ---
    return final_impact_gdf, final_impact_gdf_integrity_hash
