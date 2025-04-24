# src/ris_processing_pipeline.py

"""
Core processing pipeline logic for matching NYSDOT RIS (Roadway Inventory System)
data to an OSMnx (OpenStreetMap) graph using OSRM map matching, and enriching
the results with various RIS attributes.

This module defines functions representing distinct stages of the pipeline,
intended to be called sequentially by an orchestrator (like Prefect or a
worker script).

Based on logic from notebook and utilizes
src.osrm_matching_logic.
"""

import logging
import pathlib
from string import Template
from textwrap import dedent
from typing import Any, List, Optional, TypedDict, Union

# Third-party libraries
import duckdb
import geopandas as gpd
import networkx as nx
import pandas as pd  # Assuming pandas is used based on the operations
import pyproj
from tqdm import tqdm

import common.osrm.osrm_matching_logic as osrm_matching_logic
from common.nysdot.structures.nysdot_bridges import get_clipped_nysdot_bridges_data
from common.nysdot.structures.nysdot_large_culverts import (
    get_clipped_large_culvert_data,
)
from common.osm.enrich import create_enriched_osmnx_graph_for_region

# --- Logger Setup ---
# Configure logging in the calling script/orchestrator (e.g., Prefect flow, worker script)
# Example configuration:
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Constants ---
# Consider defining constants for column names, default values, etc. here
# Example:
METERS_PER_MILE = 1609.34

DEFAULT_TARGET_CRS = "EPSG:4326"
DEFAULT_OSRM_PROFILE = "driving"
DEFAULT_REVERSED_MATCH_PENALTY = 0.925  # Penalty for reversed DIRECTION=0 matches

county_geoid_to_ris_county_code = {
    "36001": 1,
    "36003": 2,
    "36005": 3,
    "36007": 4,
    "36009": 5,
    "36011": 6,
    "36013": 7,
    "36015": 8,
    "36017": 9,
    "36019": 10,
    "36021": 11,
    "36023": 12,
    "36025": 13,
    "36027": 14,
    "36029": 15,
    "36031": 16,
    "36033": 17,
    "36035": 18,
    "36037": 19,
    "36039": 20,
    "36041": 21,
    "36043": 22,
    "36045": 23,
    "36047": 24,
    "36049": 25,
    "36051": 26,
    "36053": 27,
    "36055": 28,
    "36057": 29,
    "36059": 30,
    "36061": 31,
    "36063": 32,
    "36065": 33,
    "36067": 34,
    "36069": 35,
    "36071": 36,
    "36073": 37,
    "36075": 38,
    "36077": 39,
    "36079": 40,
    "36081": 41,
    "36083": 42,
    "36085": 43,
    "36087": 44,
    "36089": 45,
    "36091": 46,
    "36093": 47,
    "36095": 48,
    "36097": 49,
    "36099": 50,
    "36101": 51,
    "36103": 52,
    "36105": 53,
    "36107": 54,
    "36109": 55,
    "36111": 56,
    "36113": 57,
    "36115": 58,
    "36117": 59,
    "36119": 60,
    "36121": 61,
    "36123": 62,
}

# --- Pipeline Functions ---


def load_ris_data(
    ris_source_path: Union[str, pathlib.Path],
    target_crs: str = DEFAULT_TARGET_CRS,
    layer: Optional[str] = "LRSN_Milepoint",
    county_geoid: Optional[str] = None,
    where_filters: Optional[List[str]] = None,  # Example: Filter for current records
    **kwargs: Any,  # Allow passing other read_file options
) -> gpd.GeoDataFrame:
    """
    Loads NYSDOT RIS (Roadway Inventory System) linear referenced route segments
    (typically 'LRSN_Milepoint' layer) from a GIS data source.

    Applies filtering based on county and optionally provided SQL WHERE clauses.
    Projects the resulting data to the specified target CRS (default EPSG:4326).
    Discards 3D/Measured geometry information to avoid potential issues with downstream tools.

    Args:
        ris_source_path:
            Path to the RIS data source (e.g., GDB zip file, GPKG).

        target_crs:
            The target Coordinate Reference System (CRS) as an EPSG string
            (e.g., "EPSG:4326"). Defaults to WGS84.

        layer:
            The specific layer name within the data source to read.
            Defaults to 'LRSN_Milepoint'.

        county_geoid:
            The Census County FIPS GEOID (e.g., "36001" for Albany County) used
            to filter the RIS features by the corresponding NYSDOT county code.
            If None, no county filter is applied.

        where_filters:
            An optional list of additional SQL-like WHERE clauses to apply when
            reading the data. These are combined with the default (TO_DATE IS NULL)
            filter and the county filter (if applicable) using AND logic.

        **kwargs:
            Additional keyword arguments passed directly to `gpd.read_file`.

    Returns:
        A GeoDataFrame containing the filtered and projected RIS LRSN_Milepoint data.
        Columns typically include ROUTE_ID, FROM_MEASURE, TO_MEASURE, DIRECTION,
        COUNTY, geometry, etc..

    Raises:
        FileNotFoundError:
            If `ris_source_path` does not exist.
        ValueError:
            If `layer` is not specified or `county_geoid` is invalid.
        Exception:
            Propagates exceptions from file reading (`gpd.read_file`) or
            coordinate reprojection (`to_crs`).
    """

    ris_source_path = pathlib.Path(ris_source_path)
    if not ris_source_path.exists():
        raise FileNotFoundError(f"RIS data source not found: {ris_source_path}")

    where_clauses = ["( TO_DATE IS NULL )"]

    if county_geoid:
        if county_geoid not in county_geoid_to_ris_county_code:
            raise ValueError(f"Invalid county_geoid: {county_geoid}")

        ris_county_code = county_geoid_to_ris_county_code[county_geoid]

        where_clauses.append(f"( COUNTY = {ris_county_code} )")

    logger.debug(f"Loading RIS data from: {ris_source_path}")

    if not layer:
        raise ValueError("GIS layer name is required.")
    else:
        logger.debug(f"Using layer: {layer}")

    if where_filters:
        where_clauses.extend([f"({w})" for w in where_filters])

    where_statement = f"( {' AND '.join(where_clauses)} )"

    logger.debug(f"Applying where clauses: {where_statement}")

    try:
        ris_mp_gdf = gpd.read_file(
            filename=ris_source_path,
            layer=layer,
            force_2d=True,  # Discard Z/M values which can cause issues
            where=where_statement,
            **kwargs,
        )
        logger.info(f"Loaded {len(ris_mp_gdf)} RIS features.")

        # Project to target CRS
        logger.debug(f"Reprojecting RIS data to {target_crs}...")
        ris_mp_gdf = ris_mp_gdf.to_crs(target_crs)
        logger.debug("Reprojection complete.")

        return ris_mp_gdf

    except Exception as e:
        logger.error(f"Error loading or processing RIS data: {e}", exc_info=True)
        raise


def perform_osrm_matching(
    ris_gdf: gpd.GeoDataFrame,
    osm_graph: nx.MultiDiGraph,
    osrm_host: str,
    geod: pyproj.Geod,
    osrm_profile: str = DEFAULT_OSRM_PROFILE,
) -> pd.DataFrame:
    """
    Performs OSRM map matching for each RIS road segment against the OSM graph.

    Iterates through each geometry in the `ris_gdf`.

    Based on the RIS 'DIRECTION' code, which signifies primary direction,
    reverse direction, or undivided roadways, it determines whether the OSRM
    matching should use the coordinates as-is, reversed, or potentially
    attempt both directions.

    It calls the OSRM `/match` service via `osrm_matching_logic`,
    validates the response, joins the result with the `osmnx_graph` to
    calculate detailed segment information (distances, offsets),
    adjusts distances based on reversal logic, and compiles results into a DataFrame.

    Args:
        ris_gdf:
            GeoDataFrame of RIS road segments (e.g., from `load_ris_data`).
            Must contain 'geometry', 'DIRECTION', and 'ROUTE_ID' columns.

        osm_graph:
            The OSM MultiDiGraph representation of the OSM road network.
            This graph MUST be derived from the same PBF as the one used by the OSRM instance
            It's used for path validation and node coordinate lookups during the join process.

        osrm_host:
            The base URL of the running OSRM service (e.g., "http://127.0.0.1:5000").

        geod:
            An initialized `pyproj.Geod` object for accurate geodesic calculations.

        osrm_profile:
            The OSRM routing profile to use (e.g., "driving"). Defaults to "driving".

    Returns:
        A pandas DataFrame containing detailed information for each successfully
        matched *OSM segment* resulting from the map matching process.
        Each row represents a portion of an OSM edge (u, v) that corresponds to
        part of a matched RIS route. Columns include:
        - `feature_id`: Index of the original RIS feature.
        - `matching_idx`: Index of the OSRM matching object (0 if no splits).
        - `leg_idx`: Index of the OSRM leg within the matching.
        - `leg_segment_idx`: Index of the segment within the OSRM leg annotation.
        - `osm_u`, `osm_v`: Node IDs of the matched OSM edge segment.
        - `ris_route_id`: ROUTE_ID from the matched RIS feature.
        - `start_dist_along_ris_m`, `end_dist_along_ris_m`: Calculated start/end
          distance (meters) along the original RIS feature's geometry.
        - `start_dist_along_ris_mi`, `end_dist_along_ris_mi`: Calculated start/end
          distance (miles) along the original RIS feature's geometry, adjusted
          for direction reversal.
        - `ris_direction`: Original DIRECTION code from RIS.
        - `reversed_input`: Boolean indicating if the coordinates were reversed
          for this OSRM match call.
        - `matching_confidence`: Confidence score (0-1) from OSRM.
        - `segment_length_m`: Calculated geodesic length (meters) of this
          specific matched OSM segment portion.

    Raises:
        ValueError:
            If essential columns are missing from `ris_gdf` or if feature length
            cannot be determined when needed for reversed distance adjustments.

        Propagates exceptions from `osrm_matching_logic` functions during critical
        failures (API errors, validation errors, join errors). Logs warnings for
        non-critical processing issues for individual features or segments.
    """

    join_data = []
    logger.debug(f"Starting OSRM matching for {len(ris_gdf)} RIS features...")

    # Initialize Geod object for distance calculations
    # geod = pyproj.Geod(ellps="WGS84") # Use WGS84 for standard calculations

    # Iterate through RIS features with a progress bar
    for feature_idx, feature_row in tqdm(
        ris_gdf.iterrows(), total=len(ris_gdf), desc="Matching RIS features"
    ):
        # Using feature_idx from iterrows() as feature_id if original index is simple
        # If ris_gdf has a meaningful unique ID column, use that instead.
        feature_id = feature_idx

        try:
            geom = feature_row.geometry
            feature_properties = feature_row.drop(
                "geometry"
            ).to_dict()  # Convert Series to dict
            ris_direction_raw = feature_properties.get("DIRECTION")
            ris_route_id = feature_properties.get("ROUTE_ID", f"Unknown_{feature_id}")

            # Handle potential None or non-integer direction values
            try:
                ris_direction = (
                    int(ris_direction_raw) if pd.notna(ris_direction_raw) else None
                )
            except (ValueError, TypeError):
                logger.debug(
                    f"Feature {feature_id}: Invalid DIRECTION '{ris_direction_raw}'. Treating as None."
                )
                ris_direction = None

            if geom is None or geom.is_empty:
                logger.debug(
                    f"Feature {feature_id} (RouteID: {ris_route_id}): Skipping due to null or empty geometry."
                )
                continue

            # Flatten coordinates
            try:
                flattened_coords = (
                    osrm_matching_logic.flatten_feature_coordinates_logic(geometry=geom)
                )
                if not flattened_coords or len(flattened_coords) < 2:
                    logger.debug(
                        f"Feature {feature_id} (RouteID: {ris_route_id}): Skipping, not enough coordinates ({len(flattened_coords)})."
                    )
                    continue
            except TypeError as e:
                logger.warning(
                    f"Feature {feature_id} (RouteID: {ris_route_id}): Skipping due to invalid geometry type. Error: {e}"
                )
                continue

            # Determine coordinate reversal based on RIS direction code
            # See notebook for interpretation
            request_reversed_flags = []
            if ris_direction == 0 or ris_direction is None:
                # Primary Undivided or Unknown: Try both directions? Notebook tried both.
                request_reversed_flags = [False, True]
            elif ris_direction == 1:
                # Primary Divided: Use original direction
                request_reversed_flags = [False]
            elif ris_direction in (2, 3):
                # Reverse Divided or Reverse No Inventory: Reverse coordinates
                request_reversed_flags = [True]
            else:
                logger.error(
                    f"Feature {feature_id} (RouteID: {ris_route_id}): Unrecognized ris_direction: {ris_direction}. Skipping."
                )
                continue

            # --- Attempt matching (potentially trying reversed coordinates) ---
            successful_match_found = False
            for reverse_input in request_reversed_flags:
                # Break loop if successful match already processed for this feature
                if successful_match_found and len(request_reversed_flags) > 1:
                    logger.debug(
                        f"Feature {feature_id}: Skipping reversed check as forward match succeeded."
                    )
                    break

                try:
                    # --- Call OSRM Match API ---
                    osrm_api_result = osrm_matching_logic.call_osrm_api_logic(
                        host=osrm_host,
                        coordinates=flattened_coords,
                        reverse_input=reverse_input,
                        osm_subnet_name=osrm_profile,
                        # Add any other OSRM options here if needed, e.g., gaps='split'
                    )

                    if not osrm_api_result or not osrm_api_result.response_dict:
                        logger.debug(
                            f"Feature {feature_id} (RouteID: {ris_route_id}, reversed={reverse_input}): OSRM call returned no result."
                        )
                        continue  # Try next reversal option or next feature

                    # --- Build Metadata ---
                    match_meta = osrm_matching_logic.build_match_metadata_logic(
                        feature_id=feature_id,
                        feature_properties=feature_properties,
                        feature_geometry=geom,
                        osm_subnet_name=osrm_profile,
                        flattened_coords=flattened_coords,  # Pass original flattened coords
                        osrm_api_result=osrm_api_result,
                        geod=geod,
                    )

                    # --- Validate Response Structure ---
                    osrm_matching_logic.validate_osrm_structure_logic(
                        match_meta=match_meta
                    )

                    # --- Extract & Validate Node Paths ---
                    node_chains = osrm_matching_logic.extract_osm_node_chains_logic(
                        match_meta=match_meta
                    )
                    if not node_chains:
                        logger.debug(
                            f"Feature {feature_id} (RouteID: {ris_route_id}, reversed={reverse_input}): No node chains extracted from OSRM response."
                        )
                        # This might indicate a valid match with no route, or an issue.
                        # Depending on requirements, might continue or investigate.
                        continue

                    osrm_matching_logic.validate_node_paths_logic(
                        match_meta=match_meta,
                        osm_graph=osm_graph,
                        node_chains=node_chains,
                    )

                    # --- Join with OSM Graph ---
                    joined_matchings = osrm_matching_logic.join_with_osm_graph_logic(
                        match_meta=match_meta, osm_graph=osm_graph, geod=geod
                    )

                    # --- Process Joined Results ---
                    feature_results = []
                    for matching_idx, matching in enumerate(joined_matchings):
                        if matching.legs:  # Only process if there are legs
                            successful_match_found = (
                                True  # Mark success for this feature
                            )

                        for leg_idx, leg in enumerate(matching.legs):
                            # Check if leg processing had errors
                            if leg.status == "Error":
                                logger.debug(
                                    f"Feature {feature_id} (RouteID: {ris_route_id}, reversed={reverse_input}), "
                                    f"Matching {matching_idx}, Leg {leg_idx}: Skipping due to processing error: {leg.error_message}"
                                )
                                continue

                            # Extract data for each segment within the leg
                            for leg_segment_idx, leg_segment in enumerate(
                                leg.leg_segments
                            ):
                                # Skip segments with zero or invalid calculated distance
                                if (
                                    pd.isna(leg_segment.leg_segment_distance_m)
                                    or leg_segment.leg_segment_distance_m <= 0
                                ):
                                    continue

                                # Calculate start/end distance along the original RIS feature (in miles)
                                # leg_start_distance_along_matched_feature_m is distance on feature to tracepoint STARTING the leg
                                # leg_segment_start/end_distance_along_leg_m is distance FROM leg start TO segment start/end
                                start_dist_m = (
                                    leg.leg_start_distance_along_matched_feature_m
                                    + leg_segment.leg_segment_start_distance_along_leg_m
                                )
                                end_dist_m = (
                                    leg.leg_start_distance_along_matched_feature_m
                                    + leg_segment.leg_segment_end_distance_along_leg_m
                                )

                                start_dist_mi = (
                                    start_dist_m / METERS_PER_MILE
                                    if pd.notna(start_dist_m)
                                    else float("nan")
                                )
                                end_dist_mi = (
                                    end_dist_m / METERS_PER_MILE
                                    if pd.notna(end_dist_m)
                                    else float("nan")
                                )

                                # Store segment data
                                row_data = dict(
                                    feature_id=feature_id,  # Add original feature identifier
                                    matching_idx=matching_idx,
                                    leg_idx=leg_idx,
                                    leg_segment_idx=leg_segment_idx,
                                    osm_u=leg_segment.u,
                                    osm_v=leg_segment.v,
                                    ris_route_id=ris_route_id,
                                    # Store calculated distances
                                    start_dist_along_ris_m=start_dist_m,
                                    end_dist_along_ris_m=end_dist_m,
                                    start_dist_along_ris_mi=start_dist_mi,
                                    end_dist_along_ris_mi=end_dist_mi,
                                    # Store context
                                    ris_direction=ris_direction,
                                    reversed_input=reverse_input,
                                    matching_confidence=matching.confidence,
                                    # Include segment distance for potential use
                                    segment_length_m=leg_segment.leg_segment_distance_m,
                                )
                                feature_results.append(row_data)

                    # --- Adjust distances for reversed input ---
                    if feature_results and reverse_input:
                        feature_geom_len_m = match_meta.feature_geometry_length_m

                        feature_geom_len_mi = (
                            feature_geom_len_m / METERS_PER_MILE
                            if pd.notna(feature_geom_len_m)
                            else float("nan")
                        )

                        if pd.isna(feature_geom_len_mi):
                            raise ValueError(
                                f"Feature {feature_id}: Cannot adjust distances for reversed input "
                                f"(RouteID: {ris_route_id}, reversed={reverse_input}) as feature length is unknown."
                            )

                        if ris_direction in (2, 3):
                            # For reverse directions, flip the distance measure from the end
                            for res in feature_results:
                                res["start_dist_along_ris_mi"] = (
                                    feature_geom_len_mi - res["start_dist_along_ris_mi"]
                                )
                                res["end_dist_along_ris_mi"] = (
                                    feature_geom_len_mi - res["end_dist_along_ris_mi"]
                                )

                                # Also flip meters if needed
                                if pd.notna(res["start_dist_along_ris_m"]):
                                    res["start_dist_along_ris_m"] = (
                                        feature_geom_len_m
                                        - res["start_dist_along_ris_m"]
                                    )
                                if pd.notna(res["end_dist_along_ris_m"]):
                                    res["end_dist_along_ris_m"] = (
                                        feature_geom_len_m - res["end_dist_along_ris_m"]
                                    )

                        else:  # Assumed DIRECTION 0 was reversed
                            # Swap start/end for reversed primary direction match
                            # NOTE: This implies from > to.
                            for res in feature_results:
                                # Swap miles
                                start_mi = res["start_dist_along_ris_mi"]
                                res["start_dist_along_ris_mi"] = res[
                                    "end_dist_along_ris_mi"
                                ]
                                res["end_dist_along_ris_mi"] = start_mi

                                # Swap meters
                                start_m = res["start_dist_along_ris_m"]
                                res["start_dist_along_ris_m"] = res[
                                    "end_dist_along_ris_m"
                                ]
                                res["end_dist_along_ris_m"] = start_m

                    # Append results for this feature/reversal attempt
                    if feature_results:
                        join_data.extend(feature_results)

                # --- Catch errors during processing for a single reversal attempt ---
                except (
                    osrm_matching_logic.OsrmApiError,
                    osrm_matching_logic.OsrmResponseError,
                    osrm_matching_logic.OsrmValidationError,
                    osrm_matching_logic.OsrmJoinError,
                ) as e:
                    logger.debug(
                        f"Feature {feature_id} (RouteID: {ris_route_id}, reversed={reverse_input}): "
                        f"Failed OSRM processing stage. Error: {type(e).__name__} - {e}"
                    )
                    # Continue to next reversal attempt (if any) or next feature
                except Exception as e_inner:
                    # TODO: Keep a log of these errors for later processing.
                    logger.error(
                        f"Feature {feature_id} (RouteID: {ris_route_id}, reversed={reverse_input}): "
                        f"Unexpected error during OSRM processing. Error: {type(e_inner).__name__} - {e_inner}",
                        exc_info=True,  # Include traceback for unexpected errors
                    )
                    raise
                    # Continue to next reversal attempt (if any) or next feature

        # --- Catch errors at the feature level (e.g., geometry processing) ---
        except Exception as e_outer:
            logger.error(
                f"Feature {feature_id} (RouteID: {ris_route_id}): Failed processing. "
                f"Error: {type(e_outer).__name__} - {e_outer}",
                exc_info=True,
            )
            raise
            # Continue to the next feature

    logger.debug(
        f"OSRM matching loop completed. Collected {len(join_data)} matched segments."
    )
    if not join_data:
        logger.warning("No matched segments were collected.")
        # Return empty DataFrame with expected columns if needed by downstream steps
        # Define expected_columns list here
        # return pd.DataFrame(columns=expected_columns)
        return pd.DataFrame()  # Or simply return empty

    # Create DataFrame from collected data
    osrm_match_results_df = pd.DataFrame(data=join_data)

    return osrm_match_results_df


def osrm_matches_to_osmnx_edges(
    osrm_match_results_df: pd.DataFrame,
    osmnx_graph_simplified: nx.MultiDiGraph,
) -> pd.DataFrame:
    """
    Post-processes the raw map matching results to select the single best RIS
    route match for each *simplified* OSMnx graph edge.

    The process involves:
    1. Aggregating matched segments (`match_df` rows) onto the simplified edges
       they belong to (using the `merged_edges` attribute from `osmnx_graph_simplified`).
    2. Calculating the proportion of each simplified edge covered by matches from
       a specific RIS route (`ratio_covered`).
    3. Calculating a score based on coverage, OSRM confidence, and a penalty
       heuristic for potentially ambiguous reversed matches.
    4. Grouping by simplified edge, RIS route, and reversal flag to get total
       coverage and score for each potential match candidate for an edge.
    5. Selecting the single best candidate (highest score) for each simplified edge.
    6. Attempting to fill in matches for the reverse direction of simplified edges
       if only one direction was matched directly, inferring milepoint ranges.

    Args:
        osrm_match_results_df:
            The raw matching results DataFrame from `perform_osrm_matching`.
            Requires columns like `osm_u`, `osm_v`, `ris_route_id`, `ris_direction`,
            `reversed_input`, `start_dist_along_ris_mi`, `end_dist_along_ris_mi`,
            `matching_confidence`.

        osmnx_graph_simplified:
            The *simplified* OSMnx graph (`g` in the notebook). Must contain edge
            attributes `merged_edges` (listing original u,v pairs) and `length_mi`.

    Returns:
        A pandas DataFrame indexed by the simplified OSMnx edge tuple (u, v, key).
        Each row represents the selected best RIS match information for that edge,
        including `route_id`, estimated milepoint range (`min_from_mi`, `max_to_mi`),
        `ratio_covered`, `score`, `reversed_input`, and `match_type` ('direct' or
        'filled_reverse').
    """

    if osrm_match_results_df.empty:
        logger.warning("Input match_df is empty. Skipping post-processing.")
        return pd.DataFrame()

    logger.debug(
        f"Starting post-processing for {len(osrm_match_results_df)} matched segments..."
    )

    # --- Prepare Input Data ---
    # Set index for efficient lookup
    # Ensure start/end distances are numeric, coerce errors to NaN
    osrm_match_results_df["start_dist_along_ris_mi"] = pd.to_numeric(
        osrm_match_results_df["start_dist_along_ris_mi"], errors="coerce"
    )
    osrm_match_results_df["end_dist_along_ris_mi"] = pd.to_numeric(
        osrm_match_results_df["end_dist_along_ris_mi"], errors="coerce"
    )
    osrm_match_results_df["matching_confidence"] = pd.to_numeric(
        osrm_match_results_df["matching_confidence"], errors="coerce"
    )

    match_df_indexed = osrm_match_results_df.set_index(["osm_u", "osm_v"]).sort_index()

    # --- Iterate through Simplified Graph Edges ---
    # Collect data for matched segments corresponding to simplified edges
    index_list = []
    data_list = []
    simplified_edges = list(osmnx_graph_simplified.edges(keys=True, data=True))
    logger.debug(f"Iterating through {len(simplified_edges)} simplified OSMnx edges...")

    for u, v, k, edge_data in tqdm(simplified_edges, desc="Post-processing matches"):
        # merged_edges likely refers to the sequence of original OSM edges
        # that constitute this simplified edge.
        merged_edges = edge_data.get("merged_edges")

        # This would mean that the osmnx_graph_simplified was not run through enrich
        # using the
        assert merged_edges, (
            f"INVARIANT BROKEN: Edge ({u}, {v}, {k}) has no 'merged_edges' attribute."
        )

        # Get simplified edge length in miles
        edge_length_mi = edge_data.get("length_mi")

        if edge_length_mi is None or pd.isna(edge_length_mi) or edge_length_mi <= 0:
            logger.warning(
                f"Edge ({u}, {v}, {k}): Skipping due to invalid length_mi: {edge_length_mi}"
            )
            continue

        # Check each constituent original edge for matches
        for mu, mv in merged_edges:
            try:
                # Use .loc which can return Series or DataFrame
                if (mu, mv) in match_df_indexed.index:
                    matches = match_df_indexed.loc[
                        [(mu, mv)]
                    ]  # Use list to ensure DataFrame return

                    # Process each potential match for this original segment
                    for _, match_row in matches.iterrows():
                        index_list.append((u, v, k))  # Index by the simplified edge

                        ris_direction = match_row.get("ris_direction")
                        reversed_input = match_row.get("reversed_input", False)
                        start_mi = match_row.get("start_dist_along_ris_mi")
                        end_mi = match_row.get("end_dist_along_ris_mi")
                        confidence = match_row.get("matching_confidence", 0.0)

                        # Ensure start/end mi are valid numbers
                        if pd.isna(start_mi) or pd.isna(end_mi):
                            logger.debug(
                                f"Edge ({u},{v},{k}) segment ({mu},{mv}): Skipping match for route {match_row.get('ris_route_id')} due to NaN distances."
                            )
                            index_list.pop()  # Remove the index added for this row
                            continue

                        # Use min/max to handle potential reversed order from previous step
                        from_mi = min(start_mi, end_mi)
                        to_mi = max(start_mi, end_mi)

                        # Calculate ratio covered
                        segment_ris_length = to_mi - from_mi
                        ratio_covered = (
                            min(segment_ris_length / edge_length_mi, 1.0)
                            if edge_length_mi > 0
                            else 0.0
                        )
                        # Clamp ratio between 0 and 1
                        ratio_covered = max(0.0, min(1.0, ratio_covered))

                        # Apply penalty heuristic from notebook
                        # Penalize matches where RIS DIRECTION was 0 (Undivided Primary)
                        # but the match was found using reversed coordinates.
                        reverse_penalty_multiplier = 1.0
                        if ris_direction == 0 and reversed_input:
                            reverse_penalty_multiplier = (
                                DEFAULT_REVERSED_MATCH_PENALTY  # e.g., 0.95
                            )

                        # Score combines coverage and confidence, with penalty
                        score = ratio_covered * confidence * reverse_penalty_multiplier

                        data_list.append(
                            dict(
                                route_id=match_row.get("ris_route_id"),
                                from_mi=from_mi,
                                to_mi=to_mi,
                                ratio_covered=ratio_covered,
                                score=score,
                                reversed_input=reversed_input,
                            )
                        )
            except KeyError:
                # Expected if an original edge (mu, mv) had no matches
                continue
            except Exception as e_match:
                logger.warning(
                    f"Error processing matches for edge ({u},{v},{k}) segment ({mu},{mv}): {e_match}",
                    exc_info=False,
                )

    if not index_list:
        logger.warning(
            "No matches found corresponding to simplified graph edges during post-processing."
        )
        return pd.DataFrame()

    # Create DataFrame of all potential matches aggregated to simplified edges
    osmnx_edge_matches_df = pd.DataFrame(
        data=data_list,
        index=pd.MultiIndex.from_tuples(index_list, names=["u", "v", "key"]),
    )

    logger.debug(
        f"Aggregated {len(osmnx_edge_matches_df)} potential matches onto simplified edges."
    )

    return osmnx_edge_matches_df


def select_best_ris_for_osmnx_edge(
    osmnx_edge_matches_df: pd.DataFrame,
    osmnx_graph_simplified: nx.MultiDiGraph,
):
    """
    Selects the single best matching RIS route for each simplified OSMnx edge.

    This function takes aggregated match data (where raw OSRM matches have
    been associated with simplified OSMnx edges) and applies a scoring
    mechanism to choose the most suitable RIS route ID and corresponding
    milepoint range for each simplified edge (u, v, key).

    The selection process involves:
    1. Grouping all potential matches for a simplified edge by the associated
       RIS route ID and whether the original coordinates were reversed during
       matching.
    2. Aggregating milepoint ranges (min/max) and summing the calculated
       coverage ratios and scores for each group. The score incorporates
       OSRM confidence, coverage proportion, and potentially a penalty heuristic
       for certain reversed matches.
    3. Selecting the group (i.e., the specific RIS route and direction match)
       with the highest aggregated score as the "best" match for that
       simplified edge.
    4. Attempting to fill in match information for simplified edges that were
       not directly matched, by checking if their reverse edge (v, u, key)
       had a direct match. If so, it infers the RIS information for the
       unmatched edge, reversing the milepoint range and adjusting direction
       indicators[cite: 36].

    Args:
        osmnx_edge_matches_df (pd.DataFrame):
            DataFrame containing potential matches aggregated onto simplified
            OSMnx edges. Must be indexed by ('u', 'v', 'key') and contain
            columns like 'route_id', 'reversed_input', 'from_mi', 'to_mi',
            'ratio_covered', 'score', 'ris_direction', 'matching_confidence'.
            This typically comes from an aggregation step after
            `perform_osrm_matching`.

        osmnx_graph_simplified (nx.MultiDiGraph):
            The simplified OSMnx graph whose edges (u, v, key) form the basis
            for selection and reverse-edge filling. Used to identify all edges
            that *should* potentially have a match.

    Returns:
        pd.DataFrame:
            A DataFrame indexed by the simplified OSMnx edge tuple (u, v, key).
            Each row contains the selected best RIS match information for that edge,
            including columns like 'route_id', 'min_from_mi', 'max_to_mi',
            'ratio_covered', 'score', 'reversed_input', and 'match_type'
            (indicating 'direct' or 'filled_reverse'). The index is guaranteed
            to be unique.

    """

    # --- Group and Select Best Match per Edge ---
    logger.debug("Grouping matches and selecting best match per edge...")

    # Group by simplified edge index, route_id, and reversed flag
    merged_matched_grouped_df = osmnx_edge_matches_df.groupby(
        [osmnx_edge_matches_df.index, "route_id", "reversed_input"]
    ).agg(
        min_from_mi=("from_mi", "min"),
        max_to_mi=("to_mi", "max"),
        # Sum ratio_covered as segments might cover different parts of the edge
        ratio_covered=("ratio_covered", "sum"),
        # Sum score as well (assuming higher cumulative score is better for same route/direction)
        score=("score", "sum"),
    )

    merged_matched_grouped_df["penalty"] = (
        merged_matched_grouped_df["score"] - 1
    ).abs()

    # Clamp ratio_covered again after summing
    merged_matched_grouped_df["ratio_covered"] = merged_matched_grouped_df[
        "ratio_covered"
    ].clip(0.0, 1.0)

    # Calculate penalty based on deviation of score from 1.0 (ideal)
    # Lower penalty is better.
    # Notebook added penalty for reversed_input here, but score already includes it.
    # Let's use score directly: higher score is better.
    # merged_matched_grouped_df['penalty'] = (merged_matched_grouped_df["score"] - 1.0).abs()

    # Reset index to prepare for selecting best match per edge (u,v,k)
    df_grouped_reset = merged_matched_grouped_df.reset_index()
    # The original index (u,v,k) is now in a column named 'level_0' by default
    df_grouped_reset = df_grouped_reset.rename(columns={"level_0": "edge_index"})
    df_grouped_reset = df_grouped_reset.set_index("edge_index")

    # Sort by penalty (ascending) so the first kept duplicate is the best
    df_sorted = df_grouped_reset.sort_values("penalty", ascending=True)

    # Keep only the best match (lowest penalty) for each simplified edge index
    df_best_matches = df_sorted[~df_sorted.index.duplicated(keep="first")]
    # Restore the MultiIndex (u, v, key)
    df_best_matches.index = pd.MultiIndex.from_tuples(
        df_best_matches.index, names=["u", "v", "key"]
    )

    logger.debug(f"Selected {len(df_best_matches)} best matches for simplified edges.")

    # --- Fill Reverse Edges ---
    logger.debug("Attempting to fill matches for reverse edges...")
    osm_edge_idx_set = set(osmnx_graph_simplified.edges(keys=True))
    matched_idx_set = set(df_best_matches.index)
    missing_idx = osm_edge_idx_set - matched_idx_set

    fill_index_list = []
    fill_data_list = []

    for u, v, key in missing_idx:
        reverse_edge = (v, u, key)
        if reverse_edge in matched_idx_set:
            try:
                # Get data from the matched reverse edge
                reverse_match_data = df_best_matches.loc[reverse_edge]

                # Create data for the forward edge, reversing milepoints
                fill_index_list.append((u, v, key))
                fill_data_list.append(
                    dict(
                        route_id=reverse_match_data["route_id"],
                        # Swap min/max milepoints
                        min_from_mi=reverse_match_data["max_to_mi"],
                        max_to_mi=reverse_match_data["min_from_mi"],
                        ratio_covered=reverse_match_data["ratio_covered"],
                        score=None,  # Score is not directly applicable to filled edge
                        reversed_input=not reverse_match_data[
                            "reversed_input"
                        ],  # Flip flag
                        # Add a flag indicating this was filled
                        match_type="filled_reverse",
                    )
                )
            except Exception as e_fill:
                logger.debug(
                    f"Error filling reverse edge for ({u},{v},{k}) from ({v},{u},{k}): {e_fill}"
                )

    filled_matches_df = pd.DataFrame(
        data=fill_data_list,
        index=pd.MultiIndex.from_tuples(fill_index_list, names=["u", "v", "key"]),
    )
    logger.debug(f"Created {len(filled_matches_df)} filled matches for reverse edges.")

    # --- Combine Best Matches and Filled Matches ---
    # Add match_type to best matches
    df_best_matches["match_type"] = "direct"

    selected_osmnx_matches_df = pd.concat(
        [df_best_matches, filled_matches_df], verify_integrity=True
    )

    logger.debug(
        f"Post-processing complete. Final dataframe has {len(selected_osmnx_matches_df)} entries."
    )

    return selected_osmnx_matches_df


def index_selected_matches(
    selected_osmnx_matches_df: pd.DataFrame,
):
    """
    Assigns a unique sequential index (`_idx_`) to the selected matches DataFrame.

    This function takes the DataFrame resulting from selecting the best RIS match
    for each simplified OSMnx edge (which is indexed by u, v, key) and
    transforms it to have a standard sequential integer index.

    It performs the following steps:
    1. Resets the existing MultiIndex ('u', 'v', 'key'), moving these index
       levels into regular columns.
    2. Creates a new sequential `pandas.RangeIndex` named '_idx_'.
    3. Resets this new '_idx_' index as well, making '_idx_' a regular column.
    The resulting DataFrame has a default RangeIndex and contains '_idx_', 'u',
    'v', and 'key' as columns along with the original match data. This '_idx_'
    column serves as a unique identifier for each selected edge match, useful
    for subsequent joining operations (like enrichment).

    Args:
        selected_osmnx_matches_df (pd.DataFrame):
            The DataFrame returned by `select_best_ris_for_osmnx_edge`,
            indexed by the simplified OSMnx edge tuple ('u', 'v', 'key').

    Returns:
        pd.DataFrame:
            A DataFrame with the same data as the input, but with the original
            ('u', 'v', 'key') index moved into columns, a new sequential integer
            column '_idx_', and a default `pandas.RangeIndex`.

    """

    indexed_selected_matches_df = selected_osmnx_matches_df.reset_index()

    indexed_selected_matches_df.index = pd.RangeIndex(
        len(indexed_selected_matches_df),  #
        name="_idx_",
    )

    indexed_selected_matches_df = indexed_selected_matches_df.reset_index()

    return indexed_selected_matches_df


# --- Type Definition for Enrichment Args ---
class _ExecuteEnrichQueryArgs(TypedDict):
    indexed_selected_matches_df: pd.DataFrame
    ris_gpkg_path: Union[str, pathlib.Path]
    sql_template: Template


# --- Enrichment Query Execution Helper ---
def _execute_enrichment_query(
    indexed_selected_matches_df: pd.DataFrame,
    ris_gpkg_path: Union[str, pathlib.Path],
    sql_template: Template,
) -> pd.DataFrame:
    """
    Executes a DuckDB SQL query template to enrich matched segments.

    Connects to the RIS GeoPackage, registers the processed matches DataFrame,
    substitutes placeholders in the SQL template, executes the query, and
    returns the result as a DataFrame.

    Args:
        indexed_selected_matches_df:
            DataFrame of processed/selected matches, must contain '_idx_',
            'route_id', 'min_from_mi', 'max_to_mi'.

        ris_gpkg_path:
            Path to the RIS GeoPackage containing event tables.

        sql_template:
            A string.Template object containing the SQL query with placeholders
            like $indexed_selected_matches, $join_on_clause, $overlap_clause.

    Returns:
        A pandas DataFrame containing the results of the enrichment query.

    Raises:
        FileNotFoundError: If ris_gpkg_path does not exist.
        duckdb.Error: If SQL execution or DataFrame registration fails.
        Exception: For other unexpected errors.
    """

    if indexed_selected_matches_df.empty:
        logger.debug("Input indexed_selected_matches_df is empty. Skipping enrichment.")
        return pd.DataFrame()

    ris_gpkg_path = pathlib.Path(ris_gpkg_path)

    if not ris_gpkg_path.exists():
        raise FileNotFoundError(f"RIS GeoPackage not found: {ris_gpkg_path}")

    logger.debug(
        f"Enriching {len(indexed_selected_matches_df)} matched edges using RIS data from {ris_gpkg_path}"
    )

    indexed_selected_matches_table_name = "indexed_selected_matches"

    join_on_clause = """ON (
                            ( a.route_id == b.ROUTE_ID )
                            AND
                            ( GREATEST(a.min_from_mi, a.max_to_mi) > b.FROM_MEASURE )
                            AND
                            ( LEAST(a.min_from_mi, a.max_to_mi) < b.TO_MEASURE )
                        )"""

    overlap_clause = """(
                          LEAST(
                              b.TO_MEASURE,
                              GREATEST(a.min_from_mi, a.max_to_mi)
                          )
                          -
                          GREATEST(
                              b.FROM_MEASURE,
                              LEAST(a.min_from_mi, a.max_to_mi)
                          )
                        )"""

    sql = sql_template.safe_substitute(
        indexed_selected_matches=indexed_selected_matches_table_name,
        join_on_clause=join_on_clause,
        overlap_clause=overlap_clause,
    )

    with duckdb.connect(ris_gpkg_path) as con:
        # con.register("indexed_selected_matches", osrm_join_results_df)
        with con.cursor() as cur:
            try:
                cur.register(
                    indexed_selected_matches_table_name,  #
                    indexed_selected_matches_df,
                )

                df = cur.sql(sql).df()

                return df

            except duckdb.Error as e_db:
                logger.error(f"DuckDB error during enrichment: {e_db}", exc_info=True)
                raise
            except Exception as e_enrich:
                logger.error(
                    f"Unexpected error during enrichment: {e_enrich}", exc_info=True
                )
                raise
            finally:
                con.close()


def get_ris_fclass_df_for_osrm_matches(
    indexed_selected_matches_df: pd.DataFrame,
    ris_gpkg_path: Union[str, pathlib.Path],
) -> pd.DataFrame:
    """
    Retrieves NYSDOT Functional Classification attributes overlapping with matched
    OSM segments.

    Joins the processed matches with the RIS Functional Class event table based
    on route ID and overlapping milepoint measures. Includes decoding of FHWA
    class based on NYSDOT code suffix. Returns all overlaps.

    Args:
        indexed_selected_matches_df:
            DataFrame from `index_selected_matches` containing '_idx_', 'route_id',
            'min_from_mi', 'max_to_mi'.

        ris_gpkg_path:
            Path to the RIS GeoPackage containing 'Ev_RIS_Functional_Class' and
            'RIS_FUNCTIONAL_CLASS_TYPE' tables.

    Returns:
        DataFrame with '_idx_' and corresponding overlapping Functional Class
        attributes (nysdot code, fhwa code, federal aid flag, hierarchy).
        May contain multiple rows per '_idx_' if multiple segments overlap. Includes
        'ris_fclass_overlap_dist_mi'.
    """

    sql_template = Template("""
        SELECT DISTINCT
            a._idx_,

            $overlap_clause       AS ris_fclass_overlap_dist_mi,

            b.FUNCTIONAL_CLASS    AS ris_fclass_nysdot_fclass,
            d.FHWA_F_CLASS        AS ris_fclass_fhwa_fclass,
            c.FEDERAL_AID_FLAG    AS ris_fclass_federal_aid_flag,
            c.CLASS_HIERARCHY     AS ris_fclass_class_hierarchy

            FROM $indexed_selected_matches AS a
            INNER JOIN Ev_RIS_Functional_Class AS b
                $join_on_clause
            LEFT OUTER JOIN RIS_FUNCTIONAL_CLASS_TYPE AS c
                ON (b.FUNCTIONAL_CLASS = c.FUNCTIONAL_CLASS_TYPE_ID)
            LEFT OUTER JOIN (
                SELECT
                    nysdot_code_suffix,
                    FHWA_F_CLASS
                FROM VALUES
                    (1, 1),
                    (2, 2),
                    (4, 3),
                    (6, 4),
                    (7, 5),
                    (8, 6),
                    (9, 7)  AS t(nysdot_code_suffix, FHWA_F_CLASS)
            ) AS d
                ON ( (c.FUNCTIONAL_CLASS::INT % 10) == d.nysdot_code_suffix )

            WHERE ( b.TO_DATE IS NULL )
    """)

    return _execute_enrichment_query(
        indexed_selected_matches_df=indexed_selected_matches_df,
        ris_gpkg_path=ris_gpkg_path,
        sql_template=sql_template,
    )


def get_ris_strahnet_df_for_osrm_matches(
    indexed_selected_matches_df: pd.DataFrame,
    ris_gpkg_path: Union[str, pathlib.Path],
) -> pd.DataFrame:
    """
    Retrieves NYSDOT STRAHNET (Strategic Highway Network) attributes overlapping
    with matched OSM segments.

    Joins based on route ID and milepoint overlap, decoding the description
    from the domain dictionary. Returns all overlaps.

    Args:
        indexed_selected_matches_df:
            DataFrame from `index_selected_matches` containing '_idx_', 'route_id',
            'min_from_mi', 'max_to_mi'.

        ris_gpkg_path:
            Path to the RIS GeoPackage containing 'Ev_RIS_StraHNet',
            'RIS_DOMAIN_DICTIONARY_TYPE', and 'RIS_DOMAIN_DICTIONARY' tables.

    Returns:
        DataFrame with '_idx_' and corresponding overlapping STRAHNET attributes
        (code, description). May contain multiple rows per '_idx_'. Includes
        'ris_strahnet_overlap_dist_mi'.
    """

    sql_template = Template("""
        SELECT DISTINCT
            a._idx_,

            $overlap_clause     AS ris_strahnet_overlap_dist_mi,

            b.STRAHNET          AS ris_strahnet_code,
            d.DESCRIPTION       AS ris_strahnet_description

        FROM $indexed_selected_matches AS a
          INNER JOIN Ev_RIS_StraHNet AS b
            $join_on_clause
          LEFT OUTER JOIN RIS_DOMAIN_DICTIONARY_TYPE AS c
            ON ( c.DOMAIN_INDEX == 'STRAHNET' )
          LEFT OUTER JOIN RIS_DOMAIN_DICTIONARY AS d
            ON (
              ( c.DOMAIN_DICTIONARY_TYPE_ID == d.DOMAIN_DICTIONARY_TYPE_ID )
              AND
              ( b.STRAHNET == d.DOMAIN_DICTIONARY_ID )
            )

        WHERE ( b.TO_DATE IS NULL )
    """)

    return _execute_enrichment_query(
        indexed_selected_matches_df=indexed_selected_matches_df,
        ris_gpkg_path=ris_gpkg_path,
        sql_template=sql_template,
    )


def get_ris_truck_rte_df_for_osrm_matches(
    indexed_selected_matches_df: pd.DataFrame,
    ris_gpkg_path: Union[str, pathlib.Path],
) -> pd.DataFrame:
    """
    Retrieves NYSDOT Truck Route designation attributes overlapping with matched OSM segments.

    Joins based on route ID and milepoint overlap, decoding the description
    from the domain dictionary. Returns all overlaps.

    Args:
        indexed_selected_matches_df:
            DataFrame from `index_selected_matches` containing '_idx_', 'route_id',
            'min_from_mi', 'max_to_mi'.

        ris_gpkg_path:
            Path to the RIS GeoPackage containing 'Ev_RIS_Trk_Rte',
            'RIS_DOMAIN_DICTIONARY_TYPE', and 'RIS_DOMAIN_DICTIONARY' tables.

    Returns:
        DataFrame with '_idx_' and corresponding overlapping Truck Route
        attributes (code, description). May contain multiple rows per '_idx_'.
        Includes 'ris_trk_rte_overlap_dist_mi'.
    """

    sql_template = Template("""
        SELECT DISTINCT
            a._idx_,

            $overlap_clause     AS ris_trk_rte_overlap_dist_mi,

            b.TRUCK_ROUTE       AS ris_trk_rte_code,
            d.DESCRIPTION       AS ris_trk_rte_code_description

          FROM $indexed_selected_matches AS a
            INNER JOIN Ev_RIS_Trk_Rte AS b
              $join_on_clause
            LEFT OUTER JOIN RIS_DOMAIN_DICTIONARY_TYPE AS c
              ON ( c.DOMAIN_INDEX == 'Tandem Truck Route' )
            LEFT OUTER JOIN RIS_DOMAIN_DICTIONARY AS d
              ON (
                ( c.DOMAIN_DICTIONARY_TYPE_ID == d.DOMAIN_DICTIONARY_TYPE_ID )
                AND
                ( b.TRUCK_ROUTE == d.DOMAIN_DICTIONARY_ID )
              )
          WHERE ( b.TO_DATE IS NULL )
    """)

    return _execute_enrichment_query(
        indexed_selected_matches_df=indexed_selected_matches_df,
        ris_gpkg_path=ris_gpkg_path,
        sql_template=sql_template,
    )


def get_ris_bridges_df_for_osrm_matches(
    indexed_selected_matches_df: pd.DataFrame,
    ris_gpkg_path: Union[str, pathlib.Path],
) -> pd.DataFrame:
    """
    Retrieves NYSDOT Bridge Identification Numbers (BINs) for bridges whose
    inventory location overlaps with matched OSM segments.

    Joins based on route ID and milepoint overlap with the RIS bridge event table.
    Returns all overlapping bridge BINs. Further joining with detailed bridge
    data (using the BIN) is required to get attributes like condition.

    Args:
        indexed_selected_matches_df:
            DataFrame from `index_selected_matches` containing '_idx_', 'route_id',
            'min_from_mi', 'max_to_mi'.

        ris_gpkg_path:
            Path to the RIS GeoPackage containing the 'Ev_Str_Bridge' table.

    Returns:
        DataFrame with '_idx_', the overlapping distance ('ris_bridge_overlap_dist_mi')[cite: 62],
        and the Bridge Identification Number ('ris_bridge_bin')[cite: 63].
        May contain multiple rows per '_idx_' if multiple bridge inventory segments overlap.
    """

    sql_template = Template("""
        SELECT DISTINCT
            a._idx_,

            $overlap_clause     AS ris_bridge_overlap_dist_mi,

            b.BIN               AS ris_bridge_bin

          FROM $indexed_selected_matches AS a
            INNER JOIN Ev_Str_Bridge AS b
              $join_on_clause
          WHERE ( b.TO_DATE IS NULL )
    """)

    return _execute_enrichment_query(
        indexed_selected_matches_df=indexed_selected_matches_df,
        ris_gpkg_path=ris_gpkg_path,
        sql_template=sql_template,
    )


def get_matches_ris_large_culverts_df(
    indexed_selected_matches_df: pd.DataFrame,
    ris_gpkg_path: Union[str, pathlib.Path],
) -> pd.DataFrame:
    """
    Retrieves NYSDOT Culvert Identification Numbers (CINs) and milepoint locations
    for large culverts located within the milepoint range of matched OSM segments.

    Joins based on route ID and checks if the culvert's point measure falls
    within the matched segment's milepoint range. Returns all located culverts.
    Further joining with detailed culvert data (using CIN) is needed for attributes.

    Args:
        indexed_selected_matches_df:
            DataFrame from `index_selected_matches` containing '_idx_', 'route_id',
            'min_from_mi', 'max_to_mi'.

        ris_gpkg_path:
            Path to the RIS GeoPackage containing the 'Ev_Str_LargeCulvert' table.

    Returns:
        DataFrame with '_idx_', the culvert's milepoint location ('ris_large_culvert_along_mi')[cite: 78],
        and the Culvert Identification Number ('ris_large_culvert_cin')[cite: 79].
        May contain multiple rows per '_idx_' if multiple culverts fall within the range.
    """

    sql_template = Template("""
        SELECT DISTINCT
            a._idx_,

            b.MEASURE     AS ris_large_culvert_along_mi,
            b.CIN         AS ris_large_culvert_cin

        FROM $indexed_selected_matches AS a
          INNER JOIN Ev_Str_LargeCulvert AS b
            ON (
              (a.route_id == b.ROUTE_ID)
              AND
              (
                b.MEASURE
                BETWEEN
                    LEAST(a.min_from_mi, a.max_to_mi)
                    AND
                    GREATEST (a.min_from_mi, a.max_to_mi)
              )
            )
        WHERE ( b.TO_DATE IS NULL )
    """)

    return _execute_enrichment_query(
        indexed_selected_matches_df=indexed_selected_matches_df,
        ris_gpkg_path=ris_gpkg_path,
        sql_template=sql_template,
    )


def get_ris_counts_df_for_osrm_matches(
    indexed_selected_matches_df: pd.DataFrame,
    ris_gpkg_path: Union[str, pathlib.Path],
) -> pd.DataFrame:
    """
    Retrieves NYSDOT traffic count statistics (AADT, truck percentages, etc.)
    overlapping with matched OSM segments.

    Joins based on route ID and milepoint overlap with the RIS count statistics
    event table ('Ev_TRADAS_NYSCountStats'). Returns all overlaps. Uses
    `DISTINCT ON (_idx_, FROM_MEASURE)` and `ORDER BY` clause based on notebook
    comment to potentially select the most relevant/recent count record if
    multiple non-current records overlap[cite: 108, 111]. Field names and descriptions
    are mapped based on the PDF data dictionary.

    Args:
        indexed_selected_matches_df:
            DataFrame from `index_selected_matches` containing '_idx_', 'route_id',
            'min_from_mi', 'max_to_mi'.

        ris_gpkg_path:
            Path to the RIS GeoPackage containing the 'Ev_TRADAS_NYSCountStats' table.

    Returns:
        DataFrame with '_idx_' and corresponding overlapping traffic count statistics.
        May contain multiple rows per '_idx_' if multiple count segments overlap.
        Includes 'ris_counts_overlap_dist_mi'[cite: 92].
    """

    sql_template = Template("""
        SELECT DISTINCT ON  (_idx_, FROM_MEASURE)
            a._idx_,

            $overlap_clause                   AS ris_counts_overlap_dist_mi,

            -- b.FederalDirection                AS ris_counts_federal_direction,
            b.FullCount                       AS ris_counts_full_count,
            b.OnewayFlag                      AS ris_counts_oneway_flag,
            b.CalculationYear                 AS ris_counts_calculation_year,
            b.AADT                            AS ris_counts_aadt,
            b.DHV                             AS ris_counts_dhv,
            b.DDHV                            AS ris_counts_ddhv,
            b.SU_AADT                         AS ris_counts_su_aadt,
            b.CU_AADT                         AS ris_counts_cu_aadt,
            b.KFactor                         AS ris_counts_k_factor,
            b.DFactor                         AS ris_counts_d_factor,
            b.AvgTruckPercent                 AS ris_counts_avg_truck_percent,
            b.AvgSUPercent                    AS ris_counts_avg_su_percent,
            b.AvgCUPercent                    AS ris_counts_avg_cu_percent,
            b.AvgMotorcyclePercent            AS ris_counts_avg_motorcycle_percent,
            b.AvgCarPercent                   AS ris_counts_avg_car_percent,
            b.AvgLightTruckPercent            AS ris_counts_avg_light_truck_percent,
            b.AvgBusPercent                   AS ris_counts_avg_bus_percent,
            b.AvgWeekday_F5_7                 AS ris_counts_avg_weekday_f5_7,
            b.AxleFactor                      AS ris_counts_axle_factor,
            b.SU_Peak                         AS ris_counts_su_peak,
            b.CU_PEAK                         AS ris_counts_cu_peak,
            b.AvgKFactor                      AS ris_counts_avg_k_factor,
            b.AvgDFactor                      AS ris_counts_avg_d_factor,
            b.TruckAADT                       AS ris_counts_truck_aadt,
            b.MORNING_HIGHEST_VALUE           AS ris_counts_morning_highest_value,
            b.AFTERNOON_HIGHEST_VALUE         AS ris_counts_afternoon_highest_value,
            b.EVENING_HIGHEST_VALUE           AS ris_counts_evening_highest_value
          FROM $indexed_selected_matches AS a
            INNER JOIN Ev_TRADAS_NYSCountStats AS b
              $join_on_clause
          WHERE ( b.TO_DATE IS NULL )
          -- NOTE: The values look much more complete for DD_ID=0.0.
          ORDER BY _idx_, FROM_MEASURE, DD_ID, FROM_DATE DESC
    """)

    return _execute_enrichment_query(
        indexed_selected_matches_df=indexed_selected_matches_df,
        ris_gpkg_path=ris_gpkg_path,
        sql_template=sql_template,
    )


def enrich_with_ris_attributes(
    selected_osmnx_matches_df: pd.DataFrame,
    nysdot_bridges_df: pd.DataFrame,
    nysdot_large_culverts_df: pd.DataFrame,
    ris_gpkg_path: Union[str, pathlib.Path],
) -> pd.DataFrame:
    """
    Combines the base processed OSRM matches with various enrichment DataFrames
    obtained from RIS event tables and supplementary NYSDOT structure data.

    Performs a series of left merges based on the '_idx_' column (unique identifier
    for each processed match segment) or appropriate keys (BIN/CIN for structures).
    Handles renaming of columns from supplementary datasets to avoid collisions.

    Args:
        selected_osmnx_matches_df:
            Base DataFrame from `index_selected_matches`.

        ris_fclass_df, ris_strahnet_df, ris_truck_route_df, ris_bridges_df,
        ris_large_culverts_df, ris_counts_df:
            Optional DataFrames returned by the respective `get_ris_*_df_for_osrm_matches`
            functions. Expected to contain '_idx_' and specific RIS attributes.

        nysdot_bridges_df:
            Optional pre-loaded DataFrame with detailed bridge attributes, including 'BIN'.

        nysdot_large_culverts_df:
            Optional pre-loaded DataFrame with detailed culvert attributes, including 'BIN' (used as CIN key).

    Returns:
        A single pandas DataFrame containing the original match information joined
        with all available enrichment attributes. The index is set to ('u', 'v', 'key').
        Rows may still be duplicated per (u, v, key) if multiple overlapping RIS
        attributes were returned by the enrichment queries (e.g., multiple count
        segments overlapping). Further selection logic might be needed downstream.

    Raises:
        ValueError: If required key columns (e.g., '_idx_', 'ris_bridge_bin', 'ris_large_culvert_cin')
                    are missing from input DataFrames when attempting joins.
    """

    indexed_selected_matches_df = index_selected_matches(
        selected_osmnx_matches_df=selected_osmnx_matches_df
    )

    ris_fclass_df = get_ris_fclass_df_for_osrm_matches(
        indexed_selected_matches_df=indexed_selected_matches_df,  #
        ris_gpkg_path=ris_gpkg_path,
    )

    ris_strahnet_df = get_ris_strahnet_df_for_osrm_matches(
        indexed_selected_matches_df=indexed_selected_matches_df,  #
        ris_gpkg_path=ris_gpkg_path,
    )

    ris_truck_route_df = get_ris_truck_rte_df_for_osrm_matches(
        indexed_selected_matches_df=indexed_selected_matches_df,  #
        ris_gpkg_path=ris_gpkg_path,
    )

    ris_bridges_df = get_ris_bridges_df_for_osrm_matches(
        indexed_selected_matches_df=indexed_selected_matches_df,  #
        ris_gpkg_path=ris_gpkg_path,
    )

    ris_large_culverts_df = get_matches_ris_large_culverts_df(
        indexed_selected_matches_df=indexed_selected_matches_df,  #
        ris_gpkg_path=ris_gpkg_path,
    )

    ris_counts_df = get_ris_counts_df_for_osrm_matches(
        indexed_selected_matches_df=indexed_selected_matches_df,  #
        ris_gpkg_path=ris_gpkg_path,
    )

    with duckdb.connect() as con:
        with con.cursor() as cur:
            cur.register("indexed_selected_matches", indexed_selected_matches_df)
            cur.register("ris_fclass", ris_fclass_df)
            cur.register("ris_strahnet", ris_strahnet_df)
            cur.register("ris_truck_route", ris_truck_route_df)
            cur.register("ris_bridges", ris_bridges_df)
            cur.register("ris_large_culverts", ris_large_culverts_df)
            cur.register("ris_counts", ris_counts_df)
            cur.register("nysdot_bridges", nysdot_bridges_df)
            cur.register("nysdot_large_culverts", nysdot_large_culverts_df)

            ris_join_result_df = cur.sql(
                dedent("""
                    SELECT DISTINCT
                        a._idx_,
                        a.u,
                        a.v,
                        a.key,

                        a.route_id      AS ris_route_id,
                        a.min_from_mi   AS ris_min_from_mi,
                        a.max_to_mi     AS ris_max_to_mi,

                        b.* EXCLUDE(_idx_),
                        c.* EXCLUDE(_idx_),
                        d.* EXCLUDE(_idx_),
                        e.* EXCLUDE(_idx_),
                        f.* EXCLUDE(_idx_),
                        g.* EXCLUDE(_idx_)
                      FROM indexed_selected_matches AS a
                        LEFT OUTER JOIN (
                            SELECT DISTINCT ON (_idx_)
                                *
                              FROM ris_fclass
                              ORDER BY _idx_, ris_fclass_overlap_dist_mi DESC
                        ) AS b USING (_idx_)
                        LEFT OUTER JOIN (
                            SELECT DISTINCT ON (_idx_)
                                *
                              FROM ris_strahnet
                              ORDER BY _idx_, ris_strahnet_overlap_dist_mi DESC
                        ) AS c USING (_idx_)
                        LEFT OUTER JOIN (
                            SELECT DISTINCT ON (_idx_)
                                *
                              FROM ris_truck_route
                              ORDER BY _idx_, ris_trk_rte_overlap_dist_mi DESC
                        ) AS d USING (_idx_)
                        LEFT OUTER JOIN (
                            SELECT DISTINCT ON (_idx_)
                                a._idx_,
                                b.ris_bridge_bin,
                                c.Carried           AS ris_nysdot_bridge_carried,
                                c.Crossed           AS ris_nysdot_bridge_crossed,
                                c.PrimaryOwn        AS ris_nysdot_bridge_primary_owner,
                                c.PrimaryMai        AS ris_nysdot_bridge_primary_maintainer,
                                c.GTMSStruct        AS ris_nysdot_bridge_gtms_structure,
                                c.GTMSMateri        AS ris_nysdot_bridge_gtms_material,
                                c.NumberOfSp        AS ris_nysdot_bridge_number_of_spans,
                                c.ConditionR        AS ris_nysdot_bridge_condition_rating,
                                c.BridgeLeng        AS ris_nysdot_bridge_bridge_length,
                                c.DeckAreaSq        AS ris_nysdot_bridge_deck_area_sq_ft,
                                c.AADT              AS ris_nysdot_bridge_aadt,
                                c.YearBuilt         AS ris_nysdot_bridge_year_built,
                                c.PostedLoad        AS ris_nysdot_bridge_posted_load,
                                c.NBI_DeckCo        AS ris_nysdot_bridge_nbi_deck_condition,
                                c.NBI_Substr        AS ris_nysdot_bridge_nbi_substructure_condition,
                                c.NBI_Supers        AS ris_nysdot_bridge_nbi_superstructure_condition,
                                c.FHWA_Condi        AS ris_nysdot_bridge_fhwa_condition,
                              FROM indexed_selected_matches AS a
                                INNER JOIN ris_bridges AS b
                                  USING (_idx_)
                                INNER JOIN nysdot_bridges AS c
                                  ON ( b.ris_bridge_bin = c.BIN )
                              ORDER BY _idx_, ris_nysdot_bridge_condition_rating NULLS LAST
                        ) AS e USING (_idx_)
                        LEFT OUTER JOIN (
                            SELECT DISTINCT ON (_idx_)
                                a._idx_,
                                b.ris_large_culvert_cin,
                                c.Crossed         AS ris_nysdot_large_culvert_crossed,
                                c.PrimaryOwn      AS ris_nysdot_large_culvert_primary_owner,
                                c.PrimaryMai      AS ris_nysdot_large_culvert_primary_maintainer,
                                c.GTMSStruct      AS ris_nysdot_large_culvert_gtms_structure,
                                c.GTMSMateri      AS ris_nysdot_large_culvert_gtms_material,
                                c.NumberOfSp      AS ris_nysdot_large_culvert_number_of_spans,
                                c.ConditionR      AS ris_nysdot_large_culvert_condition_rating,
                                c.TypeMaxSpa      AS ris_nysdot_large_culvert_type_max_span,
                                c.YearBuilt       AS ris_nysdot_large_culvert_year_built,
                                c.AbutmentTy      AS ris_nysdot_large_culvert_abutment_type,
                                c.StreamBedM      AS ris_nysdot_large_culvert_stream_bed_material,
                                c.Maintenanc      AS ris_nysdot_large_culvert_maintenance_responsibility_primary,
                                c.AbutmentHe      AS ris_nysdot_large_culvert_abutment_height,
                                c.CulvertSke      AS ris_nysdot_large_culvert_culvert_skew,
                                c.OutToOutWi      AS ris_nysdot_large_culvert_out_to_out_width,
                                c.NumberOfSp      AS ris_nysdot_large_culvert_number_of_spans,
                                c.SpanLength      AS ris_nysdot_large_culvert_span_length,
                                c.StructureL      AS ris_nysdot_large_culvert_structure_length,
                                c.GeneralRec      AS ris_nysdot_large_culvert_general_recommendation,
                                c.REDC            AS ris_nysdot_large_culvert_regional_economic_development_council
                              FROM indexed_selected_matches AS a
                                INNER JOIN ris_large_culverts AS b
                                  USING (_idx_)
                                INNER JOIN nysdot_large_culverts AS c
                                  ON ( b.ris_large_culvert_cin = c.BIN )
                              ORDER BY _idx_, ris_nysdot_large_culvert_condition_rating NULLS LAST
                        ) AS f USING (_idx_)
                        LEFT OUTER JOIN (
                            SELECT DISTINCT ON (_idx_)
                                *
                              FROM ris_counts
                              ORDER BY _idx_, ris_counts_overlap_dist_mi DESC
                        ) AS g USING (_idx_)
                """)
            ).df()

        enriched_osmnx_edges_df = ris_join_result_df.set_index(
            keys=["u", "v", "key"],  #
            verify_integrity=True,
        )

        return enriched_osmnx_edges_df


def perform_osmnx_conflation(
    osm_pbf: str,
    ris_path: str,
    nysdot_bridges_path: str,
    nysdot_large_culverts_path: str,
    osrm_host: str,
):
    """
    Orchestrates the OSMnx and RIS conflation process.

    Args:
        geoid: Geographic identifier (e.g., county GEOID).
        osm_pbf: Path to the OSM PBF file.
        ris_path: Path to the RIS milepoint snapshot file.
        nysdot_bridges_path: Path to the NYSDOT bridges shapefile.
        nysdot_large_culverts_path: Path to the NYSDOT large culverts shapefile.
        osrm_host: OSRM server host address.
    """
    enriched_osm = create_enriched_osmnx_graph_for_region(
        osm_pbf=osm_pbf,  #
        include_base_osm_data=True,
    )

    G = enriched_osm["G"]
    g = enriched_osm["g"]
    geoid = enriched_osm["geoid"]
    roads_gdf = enriched_osm["edges_gdf"]
    buffered_region_gdf = enriched_osm["buffered_region_gdf"]

    # Initialize pyproj Geod
    # FIXME: Use a more accurate projection.
    geod = pyproj.Geod(ellps="WGS84", sphere=True)

    # [10] Load RIS data
    ris_gdf = load_ris_data(
        ris_source_path=ris_path,
        county_geoid=geoid,
    )

    # [11] Perform OSRM matching
    osrm_match_results_df = perform_osrm_matching(
        ris_gdf=ris_gdf,
        osm_graph=G,
        osrm_host=osrm_host,
        geod=geod,
    )

    # [13] Convert OSRM matches to OSMnx edges
    osmnx_edge_matches_df = osrm_matches_to_osmnx_edges(
        osrm_match_results_df=osrm_match_results_df, osmnx_graph_simplified=g
    )

    # [14] Select best RIS matches for OSMnx edges
    selected_osmnx_matches_df = select_best_ris_for_osmnx_edge(
        osmnx_edge_matches_df=osmnx_edge_matches_df, osmnx_graph_simplified=g
    )

    # [15] Get clipped NYSDOT bridges data
    nysdot_bridges_df = get_clipped_nysdot_bridges_data(
        nysdot_bridges_source=nysdot_bridges_path,
        buffered_region_gdf=buffered_region_gdf,
        filter_out_rail_bridges=True,
    ).drop(columns="geometry")

    # [16] Get clipped NYSDOT large culverts data
    nysdot_large_culverts_df = get_clipped_large_culvert_data(
        gis_source=nysdot_large_culverts_path,
        buffered_region_gdf=buffered_region_gdf,
    ).drop(columns="geometry")

    # [17] Enrich with RIS attributes
    enriched_with_ris_df = enrich_with_ris_attributes(
        selected_osmnx_matches_df=selected_osmnx_matches_df,
        nysdot_bridges_df=nysdot_bridges_df,
        nysdot_large_culverts_df=nysdot_large_culverts_df,
        ris_gpkg_path=ris_path,
    )

    # [18] Join roads geometry with final conflation results
    roads_geoms_gdf = roads_gdf[roads_gdf["_intersects_region_"]].drop(
        columns=roads_gdf.columns.difference(["geometry"])
    )

    osrm_conflation_gdf = roads_geoms_gdf.join(
        other=enriched_with_ris_df,  #
        how="inner",
    )

    # Return the resulting GeoDataFrames and DataFrames
    return dict(
        osrm_conflation_gdf=osrm_conflation_gdf,
        enriched_osm=enriched_osm,
        ris_gdf=ris_gdf,
    )
