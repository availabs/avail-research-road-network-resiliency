# osrm_matching_logic.py
"""
Core logic for
    - OSRM map matching,
    - response validation,
    - joining matched paths with an OSM graph,
    - and performing quality assurance checks.

This module aims to encapsulate the interaction with the OSRM `/match` service
and the subsequent processing of its response.

The logic leverages insights from the OSRM backend source code to accurately
interpret the response structure and perform calculations.

Based on analysis of OSRM source files.
- https://github.com/Project-OSRM/osrm-backend/blob/master/src/engine/routing_algorithms/map_matching.cpp
- https://github.com/Project-OSRM/osrm-backend/blob/master/src/engine/plugins/match.cpp
- https://github.com/Project-OSRM/osrm-backend/blob/master/src/server/service/match_service.cpp

The following describes the response structure for a map matching request.

Attributes:
    code (str): Status code. "Ok" indicates success. Other codes like
        "NoMatch", "TooBig", "InvalidOptions", "NoSegment" indicate errors.

    message (str, optional): An error message if `code` is not "Ok".

    tracepoints (list[Tracepoint | None]): An array containing snapping
        information for each input coordinate, in the same order as the input.

        Each element is either a Tracepoint object if the input coordinate
        was matched to a road segment as part of a SubMatching, or None if the
        input coordinate could not be matched or was part of a segment
        removed during trace splitting/tidying.

        Tracepoint Attributes:
            location (list[float, float]): `[longitude, latitude]` of the point on
                the road network to which the input coordinate was matched (the
                PhantomNode location).
            distance (float): The straight-line distance (in meters) from the
                input coordinate to the matched `location` on the road segment.
                Corresponds to `PhantomNodeWithDistance.distance`.
            name (str, optional): The name of the street the coordinate was
                matched to. Derived during route generation.
            hint (str, optional): Hint for routing purposes, useful for
                subsequent requests (e.g., `/route`). Encodes information about
                the matched segment.
            matchings_index (int): The index (`0...M-1`) of the Matching
                object in the `matchings` array that this tracepoint belongs to.
            waypoint_index (int): The index (`0...L-1`) within the `legs` array
                of the corresponding Matching object where this tracepoint lies.
                Specifically, it indicates the *start* of the leg that originates
                from this tracepoint's matched location. For the very last
                tracepoint of a matching, this index will equal the number of
                legs in that matching.
            alternatives_count (int, optional): Number of alternative matchings
                considered for this tracepoint. Derived from
                `SubMatching.alternatives_count`. Usually 0 in recent versions
                unless specific alternative logic is enabled/present.

    matchings (list[Matching]): An array of Matching objects.

        If the trace was not split, this array will contain a single element. If
        the trace was split (e.g., due to `gaps=split` or large
        inconsistencies), it will contain multiple Matching objects, each
        representing a contiguous matched portion of the trace.

        Matching Attributes:
            confidence (float): A score between 0 and 1 indicating the confidence
                in the correctness of this matching. Calculated using the
                `MatchingConfidence` logic based on trace vs. matched distance.
            distance (float): The total length (in meters) of the matched route
                path for this Matching object (sum of `legs[i].distance`).
                Calculated during the `ShortestPathSearch` phase.
            duration (float): The total estimated travel time (in seconds) for
                this Matching object (sum of `legs[i].duration`). Calculated
                during the `ShortestPathSearch` phase based on segment speeds
                and weights.
            weight (float): The total weight of the matched path according to the
                profile's weighting function (sum of `legs[i].weight`).
            weight_name (str): The name of the weight profile used (e.g.,
                "routability", "duration").
            legs (list[RouteLeg]): An array representing the segments of the
                matched path. Each leg connects two consecutive matched tracepoints
                (or user-defined waypoints if `waypoints` parameter was used and
                legs collapsed).

                RouteLeg Attributes:
                    distance (float): Aggregated distance (meters) for this leg.
                    duration (float): Aggregated duration (seconds) for this leg.
                    weight (float): Aggregated weight for this leg.
                    summary (str): Aggregated summary for this leg (often
                        deprecated or empty).
                    steps (list[RouteStep]): Detailed turn-by-turn instructions
                        for traversing this leg. This is standard OSRM route
                        representation.

                        RouteStep Attributes:
                            distance (float): Step distance.
                            duration (float): Step duration.
                            weight (float): Step weight.
                            name (str): Step name.
                            ref (str, optional): Step ref.
                            pronunciation (str, optional): Step pronunciation.
                            destinations (str, optional): Step destinations.
                            exits (str, optional): Step exits.
                            mode (str, optional): Step mode.
                            maneuver (object): Details about the turn or action
                                required at the beginning of the step.
                                - type (str): Maneuver type.
                                - modifier (str): Maneuver modifier.
                                - location (list[float, float]): Maneuver location [lon, lat].
                                - bearings (list[int]): Maneuver bearings.
                            intersections (list[int], optional): Information about
                                intersections along the step.
                            rotary_name (str, optional): Rotary name.
                            rotary_pronunciation (str, optional): Rotary pronunciation.
                            geometry (str): Encoded polyline (`polyline` or
                                `polyline6`) or GeoJSON `LineString` representing
                                the geometry of this step. Format depends on the
                                `geometries` request parameter.

                    annotation (Annotation, optional): Contains detailed data for
                        each segment *within* the leg, enabled by the
                        `annotations=true` request parameter. This is crucial
                        for detailed analysis.

                        Annotation Attributes:
                            distance (list[float]): Distance (meters) for each
                                internal segment making up the leg.
                            duration (list[float]): Duration (seconds) for each
                                internal segment.
                            datasources (list[int]): Index into
                                `metadata.datasource_names` indicating the source
                                of speed/duration data for each segment.
                            nodes (list[int]): A flattened list where pairs
                                `nodes[2*i], nodes[2*i+1]` form OSM Node IDs for
                                each internal segment.
                            weight (list[float]): Weight for each internal segment.
                            speed (list[float]): Speed (km/h) for each internal
                                segment.
                            metadata (object, optional): Additional metadata.
                                - datasource_names (list[str]): List of data
                                    source names (e.g., "speed", "duration")
                                    referenced by `datasources` indices.

            geometry (str, optional): Encoded polyline (`polyline` or
                `polyline6`) or GeoJSON `LineString` representing the *entire*
                geometry of this Matching object. Format depends on the
                `geometries` request parameter, and presence depends on the
                `overview` parameter (`full`, `simplified`, `false`).
"""

import json
import logging
import os
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import networkx as nx
import pandas as pd  # Used for describe() in stats, can be removed if not essential.
import pyproj
import shapely
from dotenv import load_dotenv

load_dotenv()

AVAIL_RNR_OSRM_URL = os.getenv(key="AVAIL_RNR_OSRM_URL")
# print(f"\n\n==========> AVAIL_RNR_OSRM_URL={AVAIL_RNR_OSRM_URL} =========\n\n")

# --- Custom Exceptions ---


class OsrmLogicError(Exception):
    """Base exception for errors occurring within this OSRM logic module."""

    pass


class OsrmApiError(OsrmLogicError):
    """
    Exception raised for errors during interaction with the OSRM HTTP API.
    This typically covers network issues, HTTP errors (non-200 status),
    or timeouts.
    """

    def __init__(
        self, message: str, url: Optional[str] = None, status: Optional[str] = None
    ):
        """
        Initializes the OsrmApiError.

        Args:
            message: The error message.
            url: The OSRM API URL that caused the error.
            status: The HTTP status code, if applicable.
        """
        super().__init__(message)
        self.url = url
        self.status = status


class OsrmResponseError(OsrmLogicError):
    """
    Exception raised for errors related to the *content* of the OSRM API response.
    This includes cases where OSRM returns a 200 OK HTTP status but the JSON
    payload indicates an internal error (e.g., code != "Ok") or has an
    unexpected structure preventing processing.
    """

    def __init__(self, message: str, details: Optional[Any] = None):
        """
        Initializes the OsrmResponseError.

        Args:
            message: The error message.
            details: Additional details about the error, potentially including
                     the problematic part of the OSRM response.
        """
        super().__init__(message)
        self.details = details


class OsrmValidationError(OsrmLogicError):
    """
    Exception raised when the OSRM response structure or content fails
    validation checks implemented in this module.
    This indicates potential inconsistencies or unexpected patterns in the
    response relative to assumptions based on the OSRM API behavior.
    """

    def __init__(self, message: str, errors: Optional[List[str]] = None):
        """
        Initializes the OsrmValidationError.

        Args:
            message: The main validation error message.
            errors: A list of specific validation failures found.
        """
        super().__init__(message)
        self.errors = errors if errors else []


class OsrmJoinError(OsrmLogicError):
    """
    Exception raised during the process of joining the OSRM matching results
    with the corresponding OSM graph data (e.g., NetworkX graph).
    This typically occurs if expected nodes or edges are missing in the graph.
    """

    def __init__(
        self,
        message: str,
        node_id: Optional[int] = None,
        edge: Optional[Tuple[int, int]] = None,
    ):
        """
        Initializes the OsrmJoinError.

        Args:
            message: The error message.
            node_id: The OSM node ID that caused the error, if applicable.
            edge: The OSM edge (u, v) that caused the error, if applicable.
        """
        super().__init__(message)
        self.node_id = node_id
        self.edge = edge


class BrokenInvariantError(OsrmLogicError):
    """
    Specific error indicating that a fundamental assumption or invariant
    about the data or process has been violated.
    Kept from original implementation.
    """

    pass


# --- Internal Dataclasses ---
# These dataclasses define the structured representation of the processed
# OSRM matching results used internally by this module.


@dataclass
class OsrmApiResult:
    """
    Holds the direct result of a successful call to the OSRM `/match` API.

    Attributes:
        response_dict: The parsed JSON response from the OSRM API as a Python dict.
        request_url: The full URL used for the OSRM API request.
        reversed_input: Flag indicating if the input coordinate sequence was
                        reversed before sending to OSRM.
    """

    response_dict: Dict[str, Any]
    request_url: str
    reversed_input: bool


@dataclass
class InternalMatchMetadata:
    """
    Internal container holding all necessary information for processing
    a single feature's map matching result. This structure aggregates input
    data and the OSRM API response before detailed joining and QA.

    Attributes:
        feature_id: Identifier for the input geographic feature being matched.
        feature_geometry: Original Shapely geometry of the input feature.
        feature_geometry_length_m: Pre-calculated geodesic length (meters) of the
                                   input feature geometry.
        feature_properties: Dictionary of original properties associated with the
                            input feature.
        reversed_input: Flag indicating if the feature's coordinate sequence was
                        reversed before matching.
        osm_subnet_name: The OSRM profile used (e.g., 'driving').
        input_coordinates_sequence: The sequence of (longitude, latitude) coordinates
                                    extracted from the feature geometry and sent
                                    to the OSRM API (potentially reversed).
        osrm_response: The parsed JSON dictionary received from the OSRM `/match` API.
        request_url: The full URL used for the OSRM API request.
    """

    feature_id: str | int
    feature_geometry: shapely.MultiLineString | shapely.LineString
    feature_geometry_length_m: float
    feature_properties: dict
    reversed_input: bool
    osm_subnet_name: str
    input_coordinates_sequence: Tuple[Tuple[float, float], ...]  # (lon, lat)
    osrm_response: Dict[str, Any]
    request_url: str


@dataclass
class JoinedLegSegment:
    """
    Represents detailed information about a single segment (corresponding to an
    underlying OSM edge traversed from node u to node v) within a matched leg.
    A 'leg' in OSRM connects two consecutive tracepoints in the result. This
    segment represents a portion of that leg that falls onto a single OSM edge.

    Attributes:
        u: The OSM node ID where this segment starts on the underlying graph edge.
        v: The OSM node ID where this segment ends on the underlying graph edge.
        leg_segment_idx: The zero-based index of this segment within the OSRM
                         leg's annotation data (specifically, within
                         `annotation.nodes`, `annotation.distance`, etc.).
        osrm_distance: The distance for this specific segment as reported by OSRM
                       in `leg.annotation.distance[leg_segment_idx]`, if available.
        osrm_duration: The duration for this specific segment as reported by OSRM
                       in `leg.annotation.duration[leg_segment_idx]`, if available.
        edge_length_m: The calculated geodesic length (meters) of the *entire*
                       underlying OSM edge from node u to node v.
        leg_segment_distance_m: The calculated geodesic distance (meters) covered
                                *by this leg* specifically *within* the u->v edge.
                                This can be less than `edge_length_m` if the leg
                                starts or ends partway along the edge.
        leg_segment_start_distance_along_leg_m: The cumulative calculated geodesic
                                               distance from the start of the *leg*
                                               to the start of *this segment*.
        leg_segment_end_distance_along_leg_m: The cumulative calculated geodesic
                                             distance from the start of the *leg*
                                             to the end of *this segment*.
        edge_start_offset_m: The calculated geodesic distance from OSM node `u`
                             to the point where this leg segment *starts* along
                             the edge u->v. Is 0 if the segment starts exactly at `u`.
        edge_end_offset_m: The calculated geodesic distance from the point where
                           this leg segment *ends* along the edge u->v to the
                           OSM node `v`. Is 0 if the segment ends exactly at `v`.
        is_initial_leg_segment: True if this is the first segment within its leg.
        is_final_leg_segment: True if this is the last segment within its leg.
    """

    u: int
    v: int
    leg_segment_idx: int
    osrm_distance: Optional[float]
    osrm_duration: Optional[float]
    edge_length_m: float
    leg_segment_distance_m: float
    leg_segment_start_distance_along_leg_m: float
    leg_segment_end_distance_along_leg_m: float
    edge_start_offset_m: float
    edge_end_offset_m: float
    is_initial_leg_segment: bool
    is_final_leg_segment: bool


@dataclass
class JoinedLeg:
    """
    Represents a processed 'leg' from an OSRM matching result, enriched with
    calculated distances and references to the original input feature and
    OSM graph nodes. An OSRM leg connects the matched locations of two
    consecutive tracepoints.

    Attributes:
        waypoint_index: The index of this leg within its parent `Matching` object's
                        `legs` array (same as `tracepoint.waypoint_index` for the
                        tracepoint *starting* this leg).
        num_leg_segments: The number of underlying OSM edge segments that constitute
                          this leg (derived from `len(leg.annotation.nodes) - 1`).
        leg_start_dist_along_matching_m: Cumulative calculated geodesic distance
                                         from the start of the *entire matching*
                                         (the sequence of legs) to the start of
                                         *this leg*.
        leg_end_dist_along_matching_m: Cumulative calculated geodesic distance
                                       from the start of the *entire matching*
                                       to the end of *this leg*.
        start_location: The original `location` [lon, lat] reported by OSRM for the
                        `tracepoint` that *starts* this leg.
        snapped_start_location: The calculated location [lon, lat] on the OSM graph
                                geometry corresponding to the start of this leg.
                                This is interpolated onto the first edge segment if
                                the leg doesn't start exactly at a node.
        start_tracepoint_waypoint_index: The `waypoint_index` from the OSRM tracepoint
                                         that starts this leg (should be same as
                                         `JoinedLeg.waypoint_index`).
        start_tracepoint_dist_from_corresponding_feature_coord_m: The OSRM-reported
            `distance` (meters) from the original input coordinate to its matched
            `start_location` on the graph.
        start_tracepoint_corresponding_feature_coord: The original input coordinate
            (lon, lat) from the input feature that corresponds to the tracepoint
            starting this leg.
        leg_start_distance_along_matched_feature_m: The calculated cumulative distance
            along the *original input feature's geometry* to the coordinate
            (`start_tracepoint_corresponding_feature_coord`) that initiated this leg.
        start_tracepoint_annotation_nodes: The list of OSM node IDs from
            `leg.annotation.nodes` for this leg, as reported by OSRM. Represents
            the path taken on the graph for this leg.
        end_location: The original `location` [lon, lat] reported by OSRM for the
                      `tracepoint` that *ends* this leg (i.e., the tracepoint
                      corresponding to `waypoint_index + 1`). Populated during the
                      processing of the *next* tracepoint.
        snapped_end_location: The calculated location [lon, lat] on the OSM graph
                              geometry corresponding to the end of this leg.
                              Interpolated onto the last edge segment if the leg
                              doesn't end exactly at a node.
        end_tracepoint_waypoint_index: The `waypoint_index` from the OSRM tracepoint
                                       that ends this leg (should be
                                       `JoinedLeg.waypoint_index + 1`). Populated
                                       by the next tracepoint.
        end_tracepoint_dist_from_corresponding_feature_coord_m: The OSRM-reported
            `distance` (meters) for the tracepoint ending this leg. Populated by
            the next tracepoint.
        end_tracepoint_corresponding_feature_coord: The original input coordinate
            (lon, lat) corresponding to the tracepoint ending this leg. Populated
            by the next tracepoint.
        leg_end_distance_along_matched_feature_m: The calculated cumulative distance
            along the *original input feature's geometry* to the coordinate
            ending this leg. Populated by the next tracepoint.
        end_tracepoint_annotation_nodes: The `leg.annotation.nodes` list for the
                                         *next* leg (starts where this leg ends).
                                         Can be useful for context but primarily
                                         associated with the next `JoinedLeg`.
                                         Populated by the next tracepoint.
        dist_between_tracepoints_m: Calculated geodesic distance (meters) between
                                    the `start_location` and `end_location` of this leg.
                                    Populated when the end location is processed.
        leg_distance_m: The total calculated geodesic distance (meters) of this leg,
                        summed across all its `JoinedLegSegment`s.
        leg_segments: A list of `JoinedLegSegment` objects detailing the path of
                      this leg along the underlying OSM graph edges.
        status: Optional status indicator for this leg, e.g., "Error" if processing failed.
        error_message: Optional error message if the status indicates an error.
    """

    waypoint_index: int

    num_leg_segments: int

    leg_start_dist_along_matching_m: float
    leg_end_dist_along_matching_m: float

    start_location: Tuple[float, float]  # OSRM tracepoint location [lon, lat]

    snapped_start_location: Optional[
        Tuple[float, float]
    ]  # Point on graph geom [lon, lat]

    start_tracepoint_waypoint_index: int
    start_tracepoint_dist_from_corresponding_feature_coord_m: float
    start_tracepoint_corresponding_feature_coord: Tuple[
        float, float
    ]  # Original feature coord [lon, lat]

    leg_start_distance_along_matched_feature_m: (
        float  # Dist along original feature geom
    )

    start_tracepoint_annotation_nodes: Optional[List[int]]  # OSM node IDs for this leg

    end_location: Optional[Tuple[float, float]]  # OSRM tracepoint location [lon, lat]

    snapped_end_location: Optional[
        Tuple[float, float]
    ]  # Point on graph geom [lon, lat]

    end_tracepoint_waypoint_index: Optional[int]
    end_tracepoint_dist_from_corresponding_feature_coord_m: Optional[float]
    end_tracepoint_corresponding_feature_coord: Optional[
        Tuple[float, float]
    ]  # Original feature coord [lon, lat]
    leg_end_distance_along_matched_feature_m: Optional[
        float
    ]  # Dist along original feature geom

    end_tracepoint_annotation_nodes: Optional[List[int]]  # OSM node IDs for *next* leg

    dist_between_tracepoints_m: Optional[
        float
    ]  # Geodesic distance between OSRM start/end locations

    leg_distance_m: float  # Total calculated geodesic distance of this leg's path

    leg_segments: List[JoinedLegSegment] = field(default_factory=list)

    status: Optional[str] = None  # e.g., "Error"

    error_message: Optional[str] = None


@dataclass
class JoinedMatching:
    """
    Represents a processed OSRM 'matching' object, corresponding to a single
    element in the top-level `matchings` array of the OSRM response.
    A single input trace might be split into multiple matchings if gaps or
    low confidence segments are detected by OSRM (`gaps=split`).

    Attributes:
        matching_index: The original index of this matching within the OSRM
                        response's `matchings` array.
        confidence: The confidence score (0.0 to 1.0) reported by OSRM for this
                    matching, indicating the likelihood that the match is correct.
                    Derived from `matching.confidence`.
        legs: A list of `JoinedLeg` objects representing the processed legs
              that constitute this matching.
    """

    matching_index: int
    confidence: float
    legs: List[JoinedLeg] = field(default_factory=list)


@dataclass
class GeometricQaResult:
    """
    Container for results from geometric Quality Assurance checks performed
    after processing the OSRM response.

    Attributes:
        leg_segments_props: A list of dictionaries, where each dictionary contains
                            QA metrics calculated for a segment between two matched
                            tracepoints (e.g., comparing feature segment length
                            to matched segment length, Frechet distance).
        error: An error message if QA calculation failed, otherwise None.
    """

    leg_segments_props: List[Dict[str, Any]] = field(default_factory=list)
    error: Optional[str] = None


@dataclass
class TracepointStatsResult:
    """
    Container for summary statistics about the distances between input coordinates
    and their matched points on the OSRM graph.

    Attributes:
        stats: A dictionary containing summary statistics (e.g., count, mean, std,
               min, max, percentiles) calculated from the `distance` field of all
               non-null OSRM tracepoints. Uses pandas `describe()` internally.
        error: An error message if statistics calculation failed, otherwise None.
    """

    stats: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None


@dataclass
class _TracepointData:
    """Internal temporary structure to hold validated data for one tracepoint."""

    original_index: int
    matching_idx: int
    waypoint_idx: int
    location: Tuple[float, float]
    distance: float
    is_final_for_matching: bool
    annotation: Optional[Dict]
    nodes: Optional[List[int]]
    next_tracepoint_location: Optional[Tuple[float, float]]


@dataclass
class _JoinLoopState:
    """Holds state variables for the main join loop."""

    last_leg_end_dist_along_matching: float = 0.0
    prev_matching_idx: int = -1


# --- Logger ---
logger = logging.getLogger(__name__)

# --- Helper Class for OSRM API Calls ---


class OsrmApiClient:
    """Internal helper class for handling OSRM HTTP API calls."""

    # def __init__(self, host: str):
    def __init__(self):
        # if not host or not host.startswith("http"):
        #     raise ValueError("Invalid OSRM host URL provided.")
        self.host = AVAIL_RNR_OSRM_URL.rstrip("/")

    def match(
        self,
        coordinates: List[Tuple[float, float]],
        osm_subnet_name: str,
        **options: Any,
    ) -> Tuple[Dict[str, Any], str]:
        """
        Calls the OSRM `/match` service via HTTP GET request.

        Constructs the URL, sends the request, and parses the JSON response.
        Includes default parameters necessary for detailed processing by this module.

        Args:
            coordinates: List of (longitude, latitude) tuples forming the trace.
            osm_subnet_name: The OSRM profile/subnet to use (e.g., 'driving', 'walk').
            **options: Additional query parameters for the OSRM API
                       (e.g., radiuses='5;10', timestamps='123;456', gaps='split').
                       These override or supplement the defaults.

        Returns:
            A tuple containing:
                - The parsed JSON response as a Python dictionary.
                - The full request URL string used.

        Raises:
            ValueError: If input coordinates or subnet name are invalid.
            OsrmApiError: If the API request fails due to network issues,
                          non-200 HTTP status codes, timeouts, or JSON decoding errors.
        """
        if not coordinates:
            raise ValueError("Coordinate list cannot be empty.")
        if not osm_subnet_name:
            raise ValueError("OSM subnet name must be provided.")

        # Format coordinates: {lon},{lat};{lon},{lat}...
        coords_str = ";".join([f"{lon:.7f},{lat:.7f}" for lon, lat in coordinates])

        # Base URL
        base_url = f"{self.host}/match/v1/{osm_subnet_name}/{coords_str}"

        # Default OSRM parameters needed for subsequent processing in this module
        params = {
            "geometries": "geojson",  # Use GeoJSON for easier parsing with Shapely
            "annotations": "true",  # Crucial: Provides nodes, distance, duration per segment
            "overview": "full",  # Get detailed geometry for the overall matchings
            "steps": "false",  # Turn-by-turn steps usually not needed for joining logic
        }

        # Update with user-provided options, potentially overriding defaults
        params.update(options)

        # Build query string, handling list-like parameters correctly
        query_string = urllib.parse.urlencode(params, doseq=True)
        request_url = f"{base_url}?{query_string}"

        logger.debug(
            f"Requesting OSRM URL (truncated coords): {base_url[:100]}...?{query_string}"
        )

        try:
            # Use context manager for the request, set timeout
            with urllib.request.urlopen(request_url, timeout=60) as response:
                status_code = response.getcode()
                if status_code == 200:
                    contents = response.read()
                    osrm_matching_response = json.loads(contents)
                    logger.debug(
                        f"OSRM response code field: {osrm_matching_response.get('code')}"
                    )
                    # Note: Further check on osrm_matching_response['code'] happens later
                    return osrm_matching_response, request_url
                else:
                    # Raise specific error for non-200 HTTP status
                    raise OsrmApiError(
                        message=f"OSRM request failed with HTTP status: {status_code} {response.reason}",
                        url=request_url,
                        status=str(status_code),
                    )
        except urllib.error.URLError as e:
            # Handle network-level errors (DNS, connection refused, etc.)
            logger.debug(f"OSRM URL Error for {request_url}: {e.reason}")
            raise OsrmApiError(message=f"URL Error: {e.reason}", url=request_url) from e
        except json.JSONDecodeError as e:
            # Handle cases where OSRM returns 200 OK but invalid JSON body
            logger.debug(f"Failed to decode JSON response from OSRM: {e}")
            raise OsrmApiError(
                message=f"Failed to decode JSON response: {e}", url=request_url
            ) from e
        except Exception as e:
            # Catch other potential errors (e.g., timeouts specified in urlopen)
            logger.debug(f"Error during OSRM request to {request_url}: {e}")
            raise OsrmApiError(
                message=f"Unexpected request exception: {e}", url=request_url
            ) from e


# --- Core Logic Functions ---


def flatten_feature_coordinates_logic(
    geometry: shapely.MultiLineString | shapely.LineString,
) -> List[Tuple[float, float]]:
    """
    Extracts and flattens coordinates from a Shapely LineString or MultiLineString
    into a single list of (longitude, latitude) tuples.

    Args:
        geometry: The input Shapely geometry.

    Returns:
        A list of coordinate tuples (longitude, latitude).

    Raises:
        TypeError: If input geometry is not a LineString or MultiLineString.
    """
    if isinstance(geometry, shapely.LineString):
        return list(geometry.coords)
    elif isinstance(geometry, shapely.MultiLineString):
        all_coords = []
        for line in geometry.geoms:
            all_coords.extend(list(line.coords))
        return all_coords
    else:
        raise TypeError("Input must be a shapely MultiLineString or LineString")


def call_osrm_api_logic(
    # host: str = AVAIL_RNR_OSRM_HOST,
    coordinates: List[Tuple[float, float]],
    reverse_input: bool,
    osm_subnet_name: str = "driving",
    options: Optional[Dict[str, Any]] = None,
) -> OsrmApiResult:
    """
    Prepares coordinates, calls the OSRM match API using OsrmApiClient,
    and performs initial validation on the response code field.

    Args:
        host: The base URL of the OSRM service (e.g., "http://localhost:5000").
        coordinates: List of (longitude, latitude) tuples for the trace.
        reverse_input: If True, reverses the coordinate list before sending.
        osm_subnet_name: The OSRM profile/subnet (e.g., 'driving').
        options: Dictionary of additional query parameters for the OSRM API,
                 passed to `OsrmApiClient.match`.

    Returns:
        An OsrmApiResult containing the response dictionary, request URL, and
        the reversal flag.

    Raises:
        OsrmApiError: If the API call itself fails (network, HTTP, JSON decode).
        OsrmResponseError: If the OSRM API returns successfully (HTTP 200) but
                           the JSON response's `code` field is not "Ok".
        ValueError: If inputs like host or coordinates are invalid.
    """
    # client = OsrmApiClient(host)
    client = OsrmApiClient()
    effective_options = options if options else {}

    # Reverse coordinates if requested
    coords_to_send = coordinates[::-1] if reverse_input else coordinates

    # Make the API call
    response_dict, request_url = client.match(
        coordinates=coords_to_send,
        osm_subnet_name=osm_subnet_name,
        **effective_options,
    )

    # Check OSRM's internal status code within the JSON response
    # See: https://github.com/Project-OSRM/osrm-backend/blob/master/docs/http.md#responses
    if response_dict.get("code") != "Ok":
        error_code = response_dict.get("code", "Unknown Error Code")
        error_msg = response_dict.get("message", "No message from OSRM.")
        # Raise error indicating OSRM reported a problem with the request
        raise OsrmResponseError(
            message=f"OSRM returned status '{error_code}': {error_msg}",
            details=response_dict,
        )

    return OsrmApiResult(
        response_dict=response_dict,
        request_url=request_url,
        reversed_input=reverse_input,
    )


def build_match_metadata_logic(
    feature_id: str | int,
    feature_properties: dict,
    feature_geometry: shapely.MultiLineString | shapely.LineString,
    osm_subnet_name: str,
    flattened_coords: List[Tuple[float, float]],
    osrm_api_result: OsrmApiResult,
    geod: pyproj.Geod,
) -> InternalMatchMetadata:
    """
    Constructs the InternalMatchMetadata object, consolidating input feature data
    and the OSRM API result into a single structure for further processing.
    Calculates the geodesic length of the input feature.

    Args:
        feature_id: Identifier of the original input feature.
        feature_properties: Original properties associated with the feature.
        feature_geometry: Original Shapely geometry of the feature.
        osm_subnet_name: The OSRM profile used for matching.
        flattened_coords: The coordinate sequence derived from the feature geometry.
        osrm_api_result: The result object from `call_osrm_api_logic`.
        geod: Initialized pyproj.Geod object for geodesic calculations.

    Returns:
        An initialized InternalMatchMetadata object.
    """
    try:
        # Calculate geodesic length of the original feature
        feature_length_m = geod.geometry_length(feature_geometry)
    except Exception as e:
        logger.warning(
            f"Could not calculate length for input feature {feature_id}: {e}"
        )
        feature_length_m = -1.0  # Indicate error or unknown length

    return InternalMatchMetadata(
        feature_id=feature_id,
        feature_geometry=feature_geometry,
        feature_geometry_length_m=feature_length_m,
        feature_properties=feature_properties,
        reversed_input=osrm_api_result.reversed_input,
        osm_subnet_name=osm_subnet_name,
        input_coordinates_sequence=tuple(
            flattened_coords
        ),  # Use tuple for immutability
        osrm_response=osrm_api_result.response_dict,
        request_url=osrm_api_result.request_url,
    )


def validate_osrm_structure_logic(match_meta: InternalMatchMetadata):
    """
    Performs structural validation checks on the OSRM `/match` response.

    This function verifies assumptions about the relationship between the
    `tracepoints` array and the `matchings` array, specifically focusing on
    the `matchings_index` and `waypoint_index` fields within non-null tracepoints.
    These indices link a specific input coordinate's match to a particular
    matching object and a specific leg within that matching.

    The validation checks ensure that these indices progress monotonically and
    logically, reflecting the sequential nature of the HMM processing in the
    OSRM backend.

    Checks performed:
    - Presence of `tracepoints` and `matchings` keys.
    - `matchings_index` starts at 0 for the first non-null tracepoint.
    - `waypoint_index` starts at 0 for the first non-null tracepoint.
    - `matchings_index` only increments by 1 when it changes.
    - `waypoint_index` resets to 0 when `matchings_index` increments.
    - `waypoint_index` increments by 1 for consecutive tracepoints within the
      same matching.
    - The final `waypoint_index` for a matching corresponds to the number of
      legs in that matching.
    - Index values stay within the bounds of the `matchings` and `legs` arrays.

    Args:
        match_meta: The InternalMatchMetadata containing the OSRM response.

    Raises:
        OsrmValidationError: If any validation check fails, containing details
                             about the specific failures.
        OsrmResponseError: If the basic structure (missing keys) is invalid.
    """
    errors = []
    matching_response = match_meta.osrm_response

    if not matching_response:
        raise OsrmResponseError("OSRM response dictionary is empty or None.")

    if "tracepoints" not in matching_response or "matchings" not in matching_response:
        errors.append("Missing 'tracepoints' or 'matchings' key in OSRM response.")
        # This is a fundamental structure error, raise immediately
        raise OsrmResponseError(
            "Invalid OSRM response structure: Missing 'tracepoints' or 'matchings'.",
            details=errors,
        )

    tracepoints = matching_response["tracepoints"]
    matchings = matching_response["matchings"]

    # Filter out null tracepoints, keeping original index if needed, but here we only need sequence
    nonnone_tracepoints = [
        (idx, tpt) for idx, tpt in enumerate(tracepoints) if tpt is not None
    ]

    if not nonnone_tracepoints:
        logger.warning(
            f"Feature {match_meta.feature_id}: No non-None tracepoints found. "
            "Skipping detailed structure validation."
        )
        return  # Nothing more to validate if no points were matched

    try:
        first_tpt_idx, first_tpt = nonnone_tracepoints[0]
        prev_tpt_original_idx = first_tpt_idx
        prev_m_idx = first_tpt["matchings_index"]
        prev_w_idx = first_tpt["waypoint_index"]

        # --- Check First Non-Null Tracepoint ---
        if (
            not isinstance(prev_m_idx, int)
            or prev_m_idx < 0
            or prev_m_idx >= len(matchings)
        ):
            errors.append(
                f"Tracepoint {first_tpt_idx}: Invalid matchings_index {prev_m_idx} "
                f"(must be 0 <= index < {len(matchings)} matchings)."
            )
        else:
            # OSRM HMM should always start matching from index 0
            if prev_m_idx != 0:
                errors.append(
                    f"Tracepoint {first_tpt_idx}: First non-null tracepoint matchings_index "
                    f"is {prev_m_idx}, expected 0."
                )

            # Check waypoint index relative to the legs of the *first* matching
            first_matching_legs = matchings[prev_m_idx].get("legs", [])
            len_first_legs = len(first_matching_legs)
            # Waypoint index should be 0 for the first point of the first leg
            if (
                not isinstance(prev_w_idx, int)
                or prev_w_idx < 0
                or prev_w_idx > len_first_legs
            ):
                errors.append(
                    f"Tracepoint {first_tpt_idx}: Invalid waypoint_index {prev_w_idx} "
                    f"for matching {prev_m_idx} (len={len_first_legs}). Must be 0 <= index <= len(legs)."
                )
            elif prev_w_idx != 0:
                errors.append(
                    f"Tracepoint {first_tpt_idx}: First non-null tracepoint waypoint_index "
                    f"is {prev_w_idx}, expected 0."
                )

        # --- Check Subsequent Non-Null Tracepoints ---
        for current_tpt_original_idx, tpt in nonnone_tracepoints[1:]:
            m_idx = tpt["matchings_index"]
            w_idx = tpt["waypoint_index"]

            # Basic index validation
            if not isinstance(m_idx, int) or m_idx < 0 or m_idx >= len(matchings):
                errors.append(
                    f"Tracepoint {current_tpt_original_idx}: Invalid matchings_index {m_idx} "
                    f"(must be 0 <= index < {len(matchings)} matchings)."
                )
                # Cannot reliably check waypoint index if matching index is invalid
                prev_m_idx = m_idx  # Update for next iteration's comparison
                prev_w_idx = -999  # Mark waypoint index as unreliable
                prev_tpt_original_idx = current_tpt_original_idx
                continue

            current_matching_legs = matchings[m_idx].get("legs", [])
            len_current_legs = len(current_matching_legs)

            # Waypoint index must be between 0 and len(legs) inclusive
            if not isinstance(w_idx, int) or w_idx < 0 or w_idx > len_current_legs:
                errors.append(
                    f"Tracepoint {current_tpt_original_idx}: Invalid waypoint_index {w_idx} "
                    f"for matching {m_idx} (len={len_current_legs}). Must be 0 <= index <= len(legs)."
                )
                # If waypoint index is invalid, further comparison might be misleading
                prev_m_idx = m_idx
                prev_w_idx = w_idx  # Update but know it's invalid
                prev_tpt_original_idx = current_tpt_original_idx
                continue

            # --- Compare with Previous Tracepoint ---
            if m_idx == prev_m_idx:  # Still within the same matching object
                # Waypoint index should increment by exactly 1
                expected_w_idx = prev_w_idx + 1
                if w_idx != expected_w_idx:
                    errors.append(
                        f"Tracepoint {current_tpt_original_idx}: Waypoint index jumped from "
                        f"{prev_w_idx} (at tracepoint {prev_tpt_original_idx}) to {w_idx} "
                        f"within matching {m_idx}. Expected index {expected_w_idx}."
                    )
            else:  # Moved to a new matching object (trace split occurred)
                # Matching index should increment by exactly 1
                expected_m_idx = prev_m_idx + 1
                if m_idx != expected_m_idx:
                    errors.append(
                        f"Tracepoint {current_tpt_original_idx}: Matchings index jumped from "
                        f"{prev_m_idx} (at tracepoint {prev_tpt_original_idx}) to {m_idx}. "
                        f"Expected index {expected_m_idx}."
                    )

                # Waypoint index must reset to 0 for the start of a new matching
                if w_idx != 0:
                    errors.append(
                        f"Tracepoint {current_tpt_original_idx}: Waypoint index is {w_idx}, "
                        f"expected 0 at the start of new matching {m_idx} "
                        f"(previous was {prev_m_idx} at tracepoint {prev_tpt_original_idx})."
                    )

                # Check if the *previous* tracepoint correctly terminated the *previous* matching.
                # This check needs the state *before* the indices changed.
                if prev_m_idx >= 0 and prev_m_idx < len(
                    matchings
                ):  # Check prev_m_idx was valid
                    prev_matching_legs = matchings[prev_m_idx].get("legs", [])
                    len_prev_legs = len(prev_matching_legs)
                    # The last waypoint index processed for the previous matching should equal its leg count
                    if prev_w_idx != len_prev_legs:
                        errors.append(
                            f"Tracepoint {prev_tpt_original_idx}: Ended matching {prev_m_idx} "
                            f"with waypoint_index {prev_w_idx}, but matching {prev_m_idx} "
                            f"has {len_prev_legs} legs. Expected index {len_prev_legs}."
                        )

            # Update state for the next iteration
            prev_m_idx = m_idx
            prev_w_idx = w_idx
            prev_tpt_original_idx = current_tpt_original_idx

        # --- Final Check ---
        # Check the very last non-null tracepoint's waypoint index against its matching's leg count.
        if prev_m_idx >= 0 and prev_m_idx < len(
            matchings
        ):  # Check last m_idx was valid
            last_matching_legs = matchings[prev_m_idx].get("legs", [])
            len_last_legs = len(last_matching_legs)
            if prev_w_idx != len_last_legs:
                errors.append(
                    f"Tracepoint {prev_tpt_original_idx}: Final non-null tracepoint ended "
                    f"matching {prev_m_idx} with waypoint_index {prev_w_idx}, but "
                    f"matching {prev_m_idx} has {len_last_legs} legs. Expected index {len_last_legs}."
                )

    except (KeyError, IndexError, TypeError) as e:
        # Catch errors accessing potentially missing keys or out-of-bounds indices
        errors.append(
            f"Error accessing OSRM response structure during validation: {type(e).__name__} - {e}"
        )

    if errors:
        raise OsrmValidationError(
            f"OSRM response structure validation failed for feature {match_meta.feature_id}. {len(errors)} error(s).",
            errors=errors,
        )
    else:
        logger.debug(
            f"OSRM response structure validated successfully for feature {match_meta.feature_id}."
        )


def extract_osm_node_chains_logic(match_meta: InternalMatchMetadata) -> List[List[int]]:
    """
    Extracts sequences of unique, consecutive OSM node IDs from the OSRM response.

    It processes each `matching` and concatenates the `nodes` list from the
    `annotation` of each `leg`. Overlapping nodes between consecutive legs
    are handled correctly to produce continuous paths.

    This relies on `annotations=true` being used in the OSRM request. The
    `nodes` array in the annotation represents the sequence of OSM node IDs
    traversed for that leg, as determined by the OSRM routing engine after the
    initial map matching.

    Args:
        match_meta: The InternalMatchMetadata containing the OSRM response.

    Returns:
        A list of lists, where each inner list represents a continuous chain
        of OSM node IDs corresponding to a single `matching` object from the
        OSRM response.
    """
    osrm_match_response = match_meta.osrm_response
    matchings = osrm_match_response.get("matchings", [])
    node_chains = []

    for matching_idx, matching in enumerate(matchings):
        current_chain: List[int] = []
        legs = matching.get("legs", [])

        for leg_idx, leg in enumerate(legs):
            try:
                # Check for annotation existence
                annotation = leg.get("annotation")
                if not annotation:
                    logger.warning(
                        f"Feature {match_meta.feature_id}, Matching {matching_idx}, "
                        f"Leg {leg_idx}: Missing 'annotation'. Cannot extract nodes. "
                        "Ensure 'annotations=true' was used in the request."
                    )
                    continue  # Skip leg if no annotation

                # Check for nodes within annotation
                nodes = annotation.get("nodes")
                if not nodes:
                    logger.warning(
                        f"Feature {match_meta.feature_id}, Matching {matching_idx}, "
                        f"Leg {leg_idx}: Missing 'nodes' in annotation. Cannot extract nodes."
                    )
                    continue  # Skip leg if no nodes list

                if not isinstance(nodes, list):
                    logger.warning(
                        f"Feature {match_meta.feature_id}, Matching {matching_idx}, "
                        f"Leg {leg_idx}: Annotation 'nodes' is not a list ({type(nodes)}). Skipping."
                    )
                    continue

                # OSRM annotations should already provide unique consecutive nodes.
                # The original check for duplicates is removed as likely unnecessary.
                # If needed, it could be added back.

                # Efficiently merge the new nodes list with the current chain, handling overlap.
                # Example: chain = [1, 2, 3], nodes = [3, 4, 5] -> overlap=1, result = [1, 2, 3, 4, 5]
                # Example: chain = [1, 2, 3], nodes = [4, 5, 6] -> overlap=0, result = [1, 2, 3, 4, 5, 6]
                if not current_chain:
                    current_chain.extend(nodes)
                elif nodes:  # Only extend if there are new nodes
                    overlap = 0
                    max_possible_overlap = min(len(current_chain), len(nodes))
                    # Find the largest suffix of current_chain that matches a prefix of nodes
                    for k in range(max_possible_overlap, 0, -1):
                        if current_chain[-k:] == nodes[:k]:
                            overlap = k
                            break
                    # Append the non-overlapping part of the new nodes
                    current_chain.extend(nodes[overlap:])

            except (AttributeError, TypeError, KeyError, IndexError) as e:
                # Catch potential issues accessing nested dictionary keys/list indices
                logger.debug(
                    f"Error processing nodes for Matching {matching_idx}, Leg {leg_idx}: "
                    f"{type(e).__name__} - {e}. Skipping leg."
                )
                continue  # Skip this leg on error

        if current_chain:  # Only add non-empty chains
            node_chains.append(current_chain)

    return node_chains


def validate_node_paths_logic(
    match_meta: InternalMatchMetadata,
    osm_graph: nx.MultiDiGraph,
    node_chains: List[List[int]],
):
    """
    Checks if the extracted OSM node sequences form connected paths in the
    provided NetworkX graph (`osm_graph`).

    It iterates through each node chain and verifies that for every consecutive
    pair of nodes (u, v) in the chain, an edge exists from u to v in the graph.

    Args:
        match_meta: The InternalMatchMetadata (used for logging context).
        osm_graph: The NetworkX graph representing the OSM road network used by OSRM.
                   It must contain nodes with the IDs extracted and edges
                   representing traversable segments.
        node_chains: The list of node chains extracted by
                     `extract_osm_node_chains_logic`.

    Raises:
        OsrmValidationError: If any pair of consecutive nodes (u, v) in any chain
                             does not have a corresponding directed edge u->v in
                             the `osm_graph`. The error includes details of the
                             unconnected pairs.
    """
    unconnected_pairs = []
    for chain_idx, chain in enumerate(node_chains):
        # Iterate through pairs of consecutive nodes (u, v)
        for i, (u, v) in enumerate(zip(chain[:-1], chain[1:])):
            # Check if a directed edge exists from u to v in the graph
            if not osm_graph.has_edge(u=u, v=v):
                unconnected_pairs.append(
                    {"chain_index": chain_idx, "pair_index": i, "u": u, "v": v}
                )

    if unconnected_pairs:
        error_message = (
            f"Found {len(unconnected_pairs)} unconnected node pairs in OSRM result "
            f"for feature {match_meta.feature_id}. This indicates a mismatch between "
            f"the OSRM result's path and the provided OSM graph."
        )
        logger.warning(f"{error_message} Details: {unconnected_pairs}")
        # Raise validation error with details of unconnected pairs
        raise OsrmValidationError(
            error_message, errors=[f"Unconnected pair: {p}" for p in unconnected_pairs]
        )
    else:
        logger.debug(
            f"Node path validation successful for feature {match_meta.feature_id}. "
            f"All node chains correspond to connected paths in the graph."
        )


# --- Decomposition of Join Logic ---


def _calculate_feature_coord_distances(
    coords: Tuple[Tuple[float, float], ...], geod: pyproj.Geod
) -> List[float]:
    """
    Helper to calculate cumulative geodesic distance along a sequence of coordinates.

    Args:
        coords: Sequence of (longitude, latitude) coordinates.
        geod: Initialized pyproj.Geod object for geodesic calculations.

    Returns:
        A list where element `i` is the cumulative geodesic distance (meters)
        from `coords[0]` to `coords[i]`. Returns NaN for segments where
        distance calculation fails.
    """
    if not coords:
        return []

    distances = [0.0]
    if len(coords) > 1:
        for idx in range(1, len(coords)):
            coord1 = coords[idx - 1]
            coord2 = coords[idx]
            try:
                # Calculate forward azimuth, backward azimuth, and distance
                _, _, dist_m = geod.inv(coord1[0], coord1[1], coord2[0], coord2[1])
                # Check for NaN result from geod.inv if points are identical/antipodal
                if pd.isna(dist_m):
                    logger.warning(
                        f"Geodesic distance calculation returned NaN between feature coords {idx - 1} and {idx}."
                    )
                    distances.append(float("nan"))
                elif pd.isna(distances[-1]):
                    distances.append(float("nan"))  # Propagate NaN
                else:
                    distances.append(distances[-1] + dist_m)
            except Exception as e:
                logger.warning(
                    f"Could not calculate distance between feature coords {idx - 1} ({coord1}) "
                    f"and {idx} ({coord2}): {type(e).__name__} - {e}"
                )
                distances.append(float("nan"))  # Mark distance as invalid

    return distances


def _process_leg_segment(
    u: int,
    v: int,
    leg_seg_idx: int,
    annotation: dict,
    osm_graph: nx.MultiDiGraph,
    geod: pyproj.Geod,
    is_initial: bool,
    is_final: bool,
    start_tracepoint_location: Tuple[float, float],
    end_tracepoint_location: Tuple[float, float],
    previous_leg_segment_end_dist: float,
) -> Tuple[
    JoinedLegSegment,
    float,
    Optional[Tuple[float, float]],
    Optional[Tuple[float, float]],
]:
    """
    Processes a single OSM edge segment (u -> v) within an OSRM leg.

    Calculates geodesic distances, determines snapped start/end points on the
    edge geometry based on the leg's start/end tracepoint locations, and
    calculates offsets.

    Args:
        u: Start OSM node ID of the edge segment.
        v: End OSM node ID of the edge segment.
        leg_seg_idx: Index of this segment within the leg's annotation arrays.
        annotation: The `annotation` dictionary for the current OSRM leg.
        osm_graph: The NetworkX graph containing OSM node coordinates.
        geod: Initialized pyproj.Geod object for calculations.
        is_initial: True if this is the first segment in the leg.
        is_final: True if this is the last segment in the leg.
        start_tracepoint_location: The OSRM `location` of the tracepoint *starting* this leg.
        end_tracepoint_location: The OSRM `location` of the tracepoint *ending* this leg.
        previous_leg_segment_end_dist: Cumulative distance along the leg up to the
                                       end of the previous segment (0.0 for initial).

    Returns:
        A tuple containing:
        - JoinedLegSegment: Dataclass with calculated details for this segment.
        - float: The cumulative distance along the leg up to the end of this segment.
        - Optional[Tuple[float, float]]: Snapped start location [lon, lat] if is_initial.
        - Optional[Tuple[float, float]]: Snapped end location [lon, lat] if is_final.

    Raises:
        OsrmJoinError: If nodes `u` or `v` are not found in the `osm_graph`.
    """
    # --- Get Node Coordinates and Edge Geometry ---
    try:
        node_u_data = osm_graph.nodes[u]
        node_v_data = osm_graph.nodes[v]
        # Assume nodes have 'x' (lon) and 'y' (lat) attributes
        node_u_pt = shapely.Point(node_u_data["x"], node_u_data["y"])
        node_v_pt = shapely.Point(node_v_data["x"], node_v_data["y"])
    except KeyError as e:
        missing_node = u if e.args[0] == u else v
        raise OsrmJoinError(
            f"Node {missing_node} (part of segment {u}->{v}) not found in OSM graph G.",
            node_id=missing_node,
            edge=(u, v),
        ) from e

    # Define the straight line geometry of the OSM edge segment
    edge_linestring = shapely.LineString([node_u_pt, node_v_pt])
    # Calculate the full length of the underlying OSM edge
    try:
        edge_length_m = geod.geometry_length(edge_linestring)
        if pd.isna(edge_length_m):
            edge_length_m = 0.0  # Handle zero-length edges
    except Exception as e:
        logger.warning(
            f"Could not calculate edge length for ({u}, {v}): {e}. Setting to NaN."
        )
        edge_length_m = float("nan")

    # --- Determine Snapped Start/End Points for this Leg Segment ---
    # These points define the portion of the u->v edge covered by this leg.
    leg_starts_at_u = True
    leg_ends_at_v = True
    snapped_start_location_on_edge = node_u_pt
    snapped_end_location_on_edge = node_v_pt
    snapped_start_for_leg: Optional[Tuple[float, float]] = None
    snapped_end_for_leg: Optional[Tuple[float, float]] = None

    # If this is the first segment of the leg, the leg starts somewhere *along* this edge,
    # potentially not exactly at node u. We project the leg's starting tracepoint
    # onto this edge to find the exact start location.
    if is_initial:
        try:
            start_trace_pt = shapely.Point(start_tracepoint_location)
            # Project the OSRM start location onto the edge geometry
            start_proj_dist_along_edge = shapely.line_locate_point(
                edge_linestring, start_trace_pt
            )
            # Interpolate to get the actual point on the line
            snapped_start_location_on_edge = edge_linestring.interpolate(
                start_proj_dist_along_edge
            )
            snapped_start_for_leg = tuple(snapped_start_location_on_edge.coords[0])
            leg_starts_at_u = False  # Leg starts partway along the edge
        except Exception as e:
            logger.warning(
                f"Could not project/interpolate start location for edge ({u}, {v}): {e}"
            )
            # Fallback: Assume starts at u, distances might become NaN
            snapped_start_location_on_edge = node_u_pt

    # If this is the final segment of the leg, the leg ends somewhere *along* this edge,
    # potentially not exactly at node v. We project the leg's ending tracepoint.
    if is_final:
        try:
            end_trace_pt = shapely.Point(end_tracepoint_location)
            # Project the OSRM end location onto the edge geometry
            end_proj_dist_along_edge = shapely.line_locate_point(
                edge_linestring, end_trace_pt
            )
            # Interpolate to get the actual point on the line
            snapped_end_location_on_edge = edge_linestring.interpolate(
                end_proj_dist_along_edge
            )
            snapped_end_for_leg = tuple(snapped_end_location_on_edge.coords[0])
            leg_ends_at_v = False  # Leg ends partway along the edge
        except Exception as e:
            logger.warning(
                f"Could not project/interpolate end location for edge ({u}, {v}): {e}"
            )
            # Fallback: Assume ends at v, distances might become NaN
            snapped_end_location_on_edge = node_v_pt

    # --- Calculate Distances for this Leg Segment ---
    leg_segment_distance_m = float("nan")
    edge_start_offset_m = float("nan")
    edge_end_offset_m = float("nan")

    try:
        # Geodesic distance covered *by the leg* within this u->v edge
        _, _, leg_segment_distance_m = geod.inv(
            snapped_start_location_on_edge.x,
            snapped_start_location_on_edge.y,
            snapped_end_location_on_edge.x,
            snapped_end_location_on_edge.y,
        )
        if pd.isna(leg_segment_distance_m):
            leg_segment_distance_m = 0.0

        # Offset from node u to where the leg segment starts on the edge
        if leg_starts_at_u:
            edge_start_offset_m = 0.0
        else:
            _, _, edge_start_offset_m = geod.inv(
                node_u_pt.x,
                node_u_pt.y,
                snapped_start_location_on_edge.x,
                snapped_start_location_on_edge.y,
            )
            if pd.isna(edge_start_offset_m):
                edge_start_offset_m = 0.0

        # Offset from where the leg segment ends on the edge to node v
        if leg_ends_at_v:
            edge_end_offset_m = 0.0
        else:
            # Calculate distance from segment end to node v
            _, _, dist_to_v = geod.inv(
                snapped_end_location_on_edge.x,
                snapped_end_location_on_edge.y,
                node_v_pt.x,
                node_v_pt.y,
            )
            if pd.isna(dist_to_v):
                dist_to_v = 0.0
            # Ensure offset isn't negative due to projection inaccuracies near node
            edge_end_offset_m = max(0.0, dist_to_v)

            # Sanity check: start offset + segment dist + end offset should approx equal edge length
            # Allow some tolerance for geodesic vs projection differences
            total_calculated = (
                edge_start_offset_m + leg_segment_distance_m + edge_end_offset_m
            )
            if (
                not pd.isna(edge_length_m)
                and not pd.isna(total_calculated)
                and abs(total_calculated - edge_length_m)
                > max(5.0, edge_length_m * 0.01)
            ):  # Tolerate 5m or 1% error
                logger.warning(
                    f"Edge ({u},{v}): Significant distance mismatch. "
                    f"EdgeLen={edge_length_m:.2f}, "
                    f"StartOffset={edge_start_offset_m:.2f}, "
                    f"SegmentDist={leg_segment_distance_m:.2f}, "
                    f"EndOffset={edge_end_offset_m:.2f}. "
                    f"Total={total_calculated:.2f}"
                )
                # Potential issue: If end offset calculation is problematic, recalculate?
                # For now, just log warning. Recalculating end offset based on others:
                # edge_end_offset_m = max(0.0, edge_length_m - edge_start_offset_m - leg_segment_distance_m)

    except Exception as e:
        logger.warning(
            f"Could not calculate geodesic distances/offsets for edge ({u}, {v}): {e}"
        )
        # Ensure relevant distances remain NaN on error
        leg_segment_distance_m = float("nan")
        edge_start_offset_m = float("nan")
        edge_end_offset_m = float("nan")

    # --- Calculate Cumulative Distance Along Leg ---
    leg_segment_start_dist_along_leg = previous_leg_segment_end_dist
    if pd.isna(previous_leg_segment_end_dist) or pd.isna(leg_segment_distance_m):
        leg_segment_end_dist_along_leg = float("nan")  # Propagate NaN
    else:
        leg_segment_end_dist_along_leg = (
            previous_leg_segment_end_dist + leg_segment_distance_m
        )

    # --- Extract OSRM Annotation Data Safely ---
    osrm_dist = None
    osrm_dur = None
    try:
        if annotation:
            if "distance" in annotation and leg_seg_idx < len(annotation["distance"]):
                osrm_dist = annotation["distance"][leg_seg_idx]
            if "duration" in annotation and leg_seg_idx < len(annotation["duration"]):
                osrm_dur = annotation["duration"][leg_seg_idx]
    except (IndexError, TypeError) as e:
        logger.warning(
            f"Error accessing OSRM annotation data for edge ({u}, {v}), index {leg_seg_idx}: {e}"
        )

    # --- Create Result Dataclass ---
    segment_data = JoinedLegSegment(
        u=u,
        v=v,
        leg_segment_idx=leg_seg_idx,
        osrm_distance=osrm_dist,
        osrm_duration=osrm_dur,
        edge_length_m=edge_length_m,
        leg_segment_distance_m=leg_segment_distance_m,
        leg_segment_start_distance_along_leg_m=leg_segment_start_dist_along_leg,
        leg_segment_end_distance_along_leg_m=leg_segment_end_dist_along_leg,
        edge_start_offset_m=edge_start_offset_m,
        edge_end_offset_m=edge_end_offset_m,
        is_initial_leg_segment=is_initial,
        is_final_leg_segment=is_final,
    )

    return (
        segment_data,
        leg_segment_end_dist_along_leg,  # Pass cumulative distance for next segment
        snapped_start_for_leg,  # Pass snapped coordinate if initial
        snapped_end_for_leg,  # Pass snapped coordinate if final
    )


def _join_helper_extract_tracepoint_data(
    tracepoint: Optional[Dict],
    tracepoints: List[Optional[Dict]],
    matchings: List[Dict],
    feature_coord_idx: int,
) -> Optional[_TracepointData]:
    """
    Safely extracts and validates essential data from a single tracepoint dict.
    Also finds the location of the next non-null tracepoint.

    Args:
        tracepoint: The tracepoint dictionary from the OSRM response (or None).
        tracepoints: The full list of tracepoints from the OSRM response.
        matchings: The list of matchings from the OSRM response.
        feature_coord_idx: The original index of this tracepoint in the input coords.

    Returns:
        A _TracepointData object if the tracepoint is not None and valid,
        otherwise None or raises OsrmResponseError.

    Raises:
        OsrmResponseError: If indices are out of bounds or types are wrong.
    """
    if tracepoint is None:
        return None  # Skip null tracepoints

    try:
        matching_idx: int = tracepoint["matchings_index"]
        waypoint_idx: int = tracepoint["waypoint_index"]
        location: Tuple[float, float] = tuple(tracepoint["location"])
        tracepoint_dist: float = tracepoint["distance"]

        # Validate indices
        if not (0 <= matching_idx < len(matchings)):
            raise OsrmResponseError(
                f"Invalid matching_index {matching_idx} at tracepoint {feature_coord_idx}."
            )
        current_matching = matchings[matching_idx]
        matching_legs = current_matching.get("legs", [])
        len_matching_legs = len(matching_legs)
        if not (0 <= waypoint_idx <= len_matching_legs):
            raise OsrmResponseError(
                f"Invalid waypoint_index {waypoint_idx} for matching {matching_idx} "
                f"(legs={len_matching_legs}) at tracepoint {feature_coord_idx}."
            )

        is_final_for_matching = waypoint_idx == len_matching_legs

        # Get annotation and nodes if this tracepoint starts a leg
        annotation = None
        nodes = None
        if not is_final_for_matching:
            annotation = matching_legs[waypoint_idx].get("annotation")
            if annotation:
                nodes = annotation.get("nodes")

        # Find the location of the next non-null tracepoint
        next_tp_loc = None
        for next_tpt_candidate in tracepoints[feature_coord_idx + 1 :]:
            if next_tpt_candidate is not None:
                next_tp_loc = tuple(next_tpt_candidate["location"])
                break

        return _TracepointData(
            original_index=feature_coord_idx,
            matching_idx=matching_idx,
            waypoint_idx=waypoint_idx,
            location=location,
            distance=tracepoint_dist,
            is_final_for_matching=is_final_for_matching,
            annotation=annotation,
            nodes=nodes,
            next_tracepoint_location=next_tp_loc,
        )

    except (KeyError, IndexError, TypeError) as e:
        # Wrap underlying errors in a more specific exception
        raise OsrmResponseError(
            f"Invalid OSRM response structure near tracepoint {feature_coord_idx}.",
            details=str(e),
        ) from e


def _join_helper_update_previous_leg(
    joined_matchings_list: List[JoinedMatching],
    prev_match_idx: int,
    prev_wp_idx: int,
    current_tp_data: _TracepointData,
    match_meta: InternalMatchMetadata,
    feature_coord_distances_along: List[float],
    geod: pyproj.Geod,
):
    """
    Updates the end_* fields of the previously processed JoinedLeg object.

    Args:
        joined_matchings_list: The list storing the results being built.
        prev_match_idx: The matching index of the leg to update.
        prev_wp_idx: The waypoint index (leg index) of the leg to update.
        current_tp_data: The processed data for the *current* tracepoint, which
                         defines the end of the previous leg.
        match_meta: Overall metadata for context.
        feature_coord_distances_along: Pre-calculated cumulative distances.
        geod: Geod object for calculations.
    """
    try:
        prev_joined_leg = joined_matchings_list[prev_match_idx].legs[prev_wp_idx]

        prev_joined_leg.end_location = current_tp_data.location
        prev_joined_leg.end_tracepoint_dist_from_corresponding_feature_coord_m = (
            current_tp_data.distance
        )
        prev_joined_leg.end_tracepoint_corresponding_feature_coord = (
            match_meta.input_coordinates_sequence[current_tp_data.original_index]
        )
        prev_joined_leg.leg_end_distance_along_matched_feature_m = (
            feature_coord_distances_along[current_tp_data.original_index]
        )
        # Note: current_tp_data.nodes belongs to the *next* leg conceptually
        prev_joined_leg.end_tracepoint_annotation_nodes = current_tp_data.nodes

        # Calculate distance between snapped points once end point is known
        if (
            prev_joined_leg.snapped_start_location
            and prev_joined_leg.snapped_end_location
        ):
            try:
                _, _, dist_snapped = geod.inv(
                    prev_joined_leg.snapped_start_location[0],
                    prev_joined_leg.snapped_start_location[1],
                    prev_joined_leg.snapped_end_location[0],
                    prev_joined_leg.snapped_end_location[1],
                )
                prev_joined_leg.dist_between_tracepoints_m = (
                    dist_snapped if not pd.isna(dist_snapped) else 0.0
                )
            except Exception as e_inv:
                logger.warning(
                    f"Could not calc snapped dist for Leg [{prev_match_idx}][{prev_wp_idx}]: {e_inv}"
                )
                prev_joined_leg.dist_between_tracepoints_m = float("nan")
        else:
            prev_joined_leg.dist_between_tracepoints_m = float(
                "nan"
            )  # Cannot calculate

    except (IndexError, KeyError):
        logger.error(
            f"Internal Error: Could not find previous leg [{prev_match_idx}][{prev_wp_idx}] "
            f"to update at tracepoint {current_tp_data.original_index}."
        )
        # Consider raising a more critical error if this state is unexpected


def _join_helper_initialize_leg(
    tp_data: _TracepointData,
    match_meta: InternalMatchMetadata,
    feature_coord_distances_along: List[float],
    cumulative_dist_start: float,
) -> Tuple[JoinedLeg, Optional[str]]:
    """
    Initializes a new JoinedLeg object based on the tracepoint starting it.
    Performs precondition checks.

    Args:
        tp_data: Validated data for the tracepoint starting the leg.
        match_meta: Overall metadata.
        feature_coord_distances_along: Pre-calculated cumulative distances.
        cumulative_dist_start: Cumulative distance along the matching up to this leg's start.

    Returns:
        A tuple containing:
        - Initialized JoinedLeg object.
        - Optional[str]: An error message if preconditions fail, otherwise None.
    """
    error_message = None
    # Precondition checks
    if tp_data.next_tracepoint_location is None:
        error_message = "Cannot process leg: No subsequent non-None tracepoint found."
    elif not tp_data.annotation or not tp_data.nodes or len(tp_data.nodes) < 2:
        error_message = (
            f"Cannot process leg: Missing or invalid annotation/nodes "
            f"(Nodes count: {len(tp_data.nodes) if tp_data.nodes else 'None'}). "
            f"Ensure 'annotations=true' used."
        )

    if error_message:
        logger.error(
            f"Feature {match_meta.feature_id}, Matching {tp_data.matching_idx}, "
            f"Leg {tp_data.waypoint_idx}: {error_message}"
        )

    joined_leg = JoinedLeg(
        waypoint_index=tp_data.waypoint_idx,
        num_leg_segments=(len(tp_data.nodes) - 1) if tp_data.nodes else 0,
        leg_start_dist_along_matching_m=cumulative_dist_start,
        leg_end_dist_along_matching_m=float("nan"),  # Calculated after segments
        start_location=tp_data.location,
        snapped_start_location=None,  # Set by first segment
        start_tracepoint_waypoint_index=tp_data.waypoint_idx,
        start_tracepoint_dist_from_corresponding_feature_coord_m=tp_data.distance,
        start_tracepoint_corresponding_feature_coord=match_meta.input_coordinates_sequence[
            tp_data.original_index
        ],
        leg_start_distance_along_matched_feature_m=feature_coord_distances_along[
            tp_data.original_index
        ],
        start_tracepoint_annotation_nodes=tp_data.nodes,
        # End fields populated by the *next* tracepoint's processing
        end_location=None,
        snapped_end_location=None,  # Set by last segment
        end_tracepoint_waypoint_index=tp_data.waypoint_idx + 1,
        end_tracepoint_dist_from_corresponding_feature_coord_m=None,
        end_tracepoint_corresponding_feature_coord=None,
        leg_end_distance_along_matched_feature_m=None,
        end_tracepoint_annotation_nodes=None,
        dist_between_tracepoints_m=None,  # Calculated after end info known
        leg_distance_m=0.0,  # Accumulates during segment processing
        status="Error" if error_message else None,
        error_message=error_message,
        leg_segments=[],
    )
    return joined_leg, error_message


def _join_helper_process_segments_for_leg(
    joined_leg: JoinedLeg,  # Modified in place
    tp_data: _TracepointData,
    osm_graph: nx.MultiDiGraph,
    geod: pyproj.Geod,
) -> float:
    """
    Processes all OSM segments constituting a single OSRM leg.
    Calls _process_leg_segment for each u->v pair from annotation.nodes.
    Updates the joined_leg object with segment data, total distance, and
    snapped locations. Sets error status on failure.

    Args:
        joined_leg: The JoinedLeg object to populate (modified in place).
        tp_data: Validated data for the tracepoint starting the leg.
        osm_graph: The NetworkX graph.
        geod: Geod object for calculations.

    Returns:
        The calculated total distance of the leg (float('nan') on error).
    """
    accumulated_dist_this_leg = 0.0
    if joined_leg.status == "Error":  # Already failed preconditions
        return float("nan")

    try:
        # We already checked nodes exist and len >= 2 in _initialize_leg
        nodes = tp_data.nodes
        start_location = tp_data.location
        # We checked next_tracepoint_location exists in _initialize_leg
        end_location = tp_data.next_tracepoint_location

        for leg_seg_idx, (u, v) in enumerate(zip(nodes[:-1], nodes[1:])):
            segment_data, seg_end_dist, snp_start, snp_end = _process_leg_segment(
                u=u,
                v=v,
                leg_seg_idx=leg_seg_idx,
                annotation=tp_data.annotation,
                osm_graph=osm_graph,
                geod=geod,
                is_initial=(leg_seg_idx == 0),
                is_final=(leg_seg_idx == len(nodes) - 2),
                start_tracepoint_location=start_location,
                end_tracepoint_location=end_location,
                previous_leg_segment_end_dist=accumulated_dist_this_leg,
            )
            joined_leg.leg_segments.append(segment_data)
            accumulated_dist_this_leg = (
                seg_end_dist  # Update cumulative dist for next segment
            )

            # Capture snapped locations from first/last segment
            if segment_data.is_initial_leg_segment and snp_start:
                joined_leg.snapped_start_location = snp_start
            if segment_data.is_final_leg_segment and snp_end:
                joined_leg.snapped_end_location = snp_end

            # Check for NaN propagation from segment processing
            if pd.isna(accumulated_dist_this_leg):
                logger.warning(
                    f"NaN distance encountered processing segment {leg_seg_idx} "
                    f"({u}->{v}) for leg {joined_leg.waypoint_index}. "
                    f"Leg distance will be NaN."
                )
                # No need to break, NaNs will propagate correctly

        # Finalize leg distance
        joined_leg.leg_distance_m = accumulated_dist_this_leg
        return accumulated_dist_this_leg  # Return the calculated distance

    except OsrmJoinError as je:
        logger.error(f"Leg {joined_leg.waypoint_index}: Error joining segment: {je}")
        joined_leg.status = "Error"
        joined_leg.error_message = str(je)
        joined_leg.leg_distance_m = float("nan")
        return float("nan")
    except Exception as e_seg:
        logger.exception(
            f"Leg {joined_leg.waypoint_index}: Unexpected error processing segments: {e_seg}"
        )
        joined_leg.status = "Error"
        joined_leg.error_message = f"Unexpected segment processing error: {e_seg}"
        joined_leg.leg_distance_m = float("nan")
        return float("nan")


def join_with_osm_graph_logic(
    match_meta: InternalMatchMetadata,
    osm_graph: nx.MultiDiGraph,
    geod: pyproj.Geod,
) -> List[JoinedMatching]:
    """
    Joins the OSRM matching result with the OSM graph (NetworkX MultiDiGraph).
    Refactored version using helper functions for improved clarity.

    Iterates through the OSRM tracepoints, reconstructing each matched leg
    segment by segment. Calculates detailed geodesic distances, snapped points,
    and offsets relative to the OSM graph.

    Args:
        match_meta: The InternalMatchMetadata containing the validated OSRM
                    response, input feature details, etc.
        osm_graph: The NetworkX MultiDiGraph representing the OSM road network.
                   Nodes must have 'x' (lon) and 'y' (lat) attributes.
        geod: Initialized pyproj.Geod object for accurate geodesic calculations.

    Returns:
        A list of `JoinedMatching` objects, containing the fully processed and
        enriched matching results. Returns an empty list if the OSRM response
        was empty or invalid.

    Raises:
        OsrmJoinError: If processing fails due to missing nodes/edges in the graph
                       (raised from helper functions).
        OsrmResponseError: If the OSRM response structure is unexpectedly invalid
                           during processing (raised from helper functions).
    """
    matching_response = match_meta.osrm_response
    tracepoints = matching_response.get("tracepoints", [])
    matchings = matching_response.get("matchings", [])

    if not tracepoints or not matchings:
        logger.debug(
            f"Feature {match_meta.feature_id}: OSRM response lacks tracepoints "
            f"or matchings. Cannot perform join. Returning empty result."
        )
        return []

    # --- Initialize Result Structure ---
    joined_matchings_list: List[JoinedMatching] = []
    for idx, m in enumerate(matchings):
        confidence = m.get("confidence", 0.0)
        normalized_confidence = max(0.0, min(1.0, confidence))
        if confidence != normalized_confidence:
            logger.debug(
                f"Clamping confidence {confidence} to {normalized_confidence} for Matching {idx}"
            )
        joined_matchings_list.append(
            JoinedMatching(
                matching_index=idx, confidence=normalized_confidence, legs=[]
            )
        )

    # --- Pre-calculate cumulative distances along the *original* input feature ---
    feature_coord_distances_along = _calculate_feature_coord_distances(
        match_meta.input_coordinates_sequence, geod
    )

    # --- Initialize Loop State ---
    loop_state = _JoinLoopState()

    # --- Main Loop: Iterate through Tracepoints ---
    for feature_coord_idx, tracepoint in enumerate(tracepoints):
        # --- Extract & Validate Data for Current Tracepoint ---
        tp_data = _join_helper_extract_tracepoint_data(
            tracepoint, tracepoints, matchings, feature_coord_idx
        )

        # Handle null tracepoints: break distance continuity and continue
        if tp_data is None:
            loop_state.last_leg_end_dist_along_matching = float("nan")
            continue

        # --- Update Previous Leg's End Information ---
        # Needs info from the *current* tracepoint (tp_data)
        if tp_data.waypoint_idx > 0:
            # The leg to update is defined by the waypoint_idx *before* the current one
            _join_helper_update_previous_leg(
                joined_matchings_list=joined_matchings_list,
                prev_match_idx=tp_data.matching_idx,  # Assumes same matching index as prev leg
                prev_wp_idx=tp_data.waypoint_idx - 1,
                current_tp_data=tp_data,
                match_meta=match_meta,
                feature_coord_distances_along=feature_coord_distances_along,
                geod=geod,
            )

        # --- Process Current Leg (if this tracepoint starts one) ---
        if not tp_data.is_final_for_matching:
            # Reset cumulative distance if we are starting a new matching object
            if tp_data.matching_idx != loop_state.prev_matching_idx:
                loop_state.last_leg_end_dist_along_matching = 0.0
            loop_state.prev_matching_idx = tp_data.matching_idx  # Update state

            # Initialize the JoinedLeg object and check preconditions
            joined_leg, init_error = _join_helper_initialize_leg(
                tp_data=tp_data,
                match_meta=match_meta,
                feature_coord_distances_along=feature_coord_distances_along,
                cumulative_dist_start=loop_state.last_leg_end_dist_along_matching,
            )

            # Process all segments within this leg if initialization succeeded
            if init_error is None:
                leg_distance = _join_helper_process_segments_for_leg(
                    joined_leg=joined_leg,  # Modified in place
                    tp_data=tp_data,
                    osm_graph=osm_graph,
                    geod=geod,
                )

                # Update cumulative distance state for the *next* leg
                if pd.isna(loop_state.last_leg_end_dist_along_matching) or pd.isna(
                    leg_distance
                ):
                    joined_leg.leg_end_dist_along_matching_m = float("nan")
                    loop_state.last_leg_end_dist_along_matching = float("nan")
                else:
                    joined_leg.leg_end_dist_along_matching_m = (
                        loop_state.last_leg_end_dist_along_matching + leg_distance
                    )
                    loop_state.last_leg_end_dist_along_matching = (
                        joined_leg.leg_end_dist_along_matching_m
                    )
            else:
                # Initialization failed, leg is already marked with error
                # Ensure cumulative distance reflects the break
                loop_state.last_leg_end_dist_along_matching = float("nan")

            # --- Add Processed Leg to Results ---
            try:
                joined_matchings_list[tp_data.matching_idx].legs.append(joined_leg)
            except IndexError:
                logger.error(
                    f"Internal Error: Attempted to add leg to invalid matching index {tp_data.matching_idx}."
                )
                # Consider raising a critical error here

    # --- Final Return ---
    return joined_matchings_list


def perform_geometric_qa_logic(
    match_meta: InternalMatchMetadata, geod: pyproj.Geod
) -> GeometricQaResult:
    """
    Performs basic geometry-based Quality Assurance checks by comparing segments
    of the original input feature geometry to the matched path between corresponding
    tracepoints.

    Calculates Frechet distance and length comparisons for segments between
    consecutive non-null matched tracepoints belonging to the same matching.

    Args:
        match_meta: The InternalMatchMetadata containing OSRM response and feature info.
        geod: Initialized pyproj.Geod object for geodesic calculations.

    Returns:
        A GeometricQaResult dataclass containing a list of calculated properties
        for each segment and an optional error message.
    """
    qa_props = []
    error_msg = None
    try:
        # Ensure necessary data is present
        if (
            "tracepoints" not in match_meta.osrm_response
            or "matchings" not in match_meta.osrm_response
            or not match_meta.input_coordinates_sequence
        ):
            return GeometricQaResult(
                error="Missing required data (tracepoints, matchings, or input coords) for QA."
            )

        tracepoints = match_meta.osrm_response["tracepoints"]
        matchings = match_meta.osrm_response["matchings"]
        matched_feature_coord_sequence = match_meta.input_coordinates_sequence

        # Get indices of non-null tracepoints
        nonnone_tracepoint_indices = [
            i for i, tpt in enumerate(tracepoints) if tpt is not None
        ]

        # Iterate through consecutive pairs of non-null tracepoints
        for i in range(len(nonnone_tracepoint_indices) - 1):
            u_original_idx = nonnone_tracepoint_indices[i]
            v_original_idx = nonnone_tracepoint_indices[i + 1]

            tpt_u = tracepoints[u_original_idx]
            tpt_v = tracepoints[v_original_idx]

            # Check if both tracepoints belong to the same matching object
            u_matching_idx = tpt_u["matchings_index"]
            v_matching_idx = tpt_v["matchings_index"]
            if u_matching_idx != v_matching_idx:
                continue  # Skip comparison across different matchings (splits)

            # Get confidence of the relevant matching
            confidence = 0.0
            if 0 <= u_matching_idx < len(matchings):
                raw_confidence = matchings[u_matching_idx].get("confidence", 0.0)
                confidence = max(0.0, min(1.0, raw_confidence))  # Use clamped value
            else:
                logger.debug(
                    f"QA: Invalid matching index {u_matching_idx} encountered."
                )
                continue  # Cannot get confidence

            # Extract the segment of the *original* input feature coordinates
            # corresponding to this pair of tracepoints.
            feature_segment_coords = list(
                matched_feature_coord_sequence[u_original_idx : v_original_idx + 1]
            )
            if len(feature_segment_coords) < 2:
                logger.debug(
                    f"QA: Skipping segment {u_original_idx}-{v_original_idx}, not enough feature coords ({len(feature_segment_coords)})."
                )
                continue  # Need at least two points to form a line

            # Calculate geometry and length of the original feature segment
            feature_segment_geom = shapely.LineString(feature_segment_coords)
            feature_segment_len_m = float("nan")
            try:
                feature_segment_len_m = geod.geometry_length(feature_segment_geom)
                if pd.isna(feature_segment_len_m):
                    feature_segment_len_m = 0.0
            except Exception as e_len:
                logger.debug(
                    f"QA: Could not calculate feature segment length for {u_original_idx}-{v_original_idx}: {e_len}"
                )
                # Keep NaN

            # Create geometry representing the *matched* path segment (straight line between snapped points)
            loc_u = shapely.Point(tpt_u["location"])
            loc_v = shapely.Point(tpt_v["location"])
            matched_segment_geom = shapely.LineString([loc_u, loc_v])
            # Note: Could alternatively use the actual routed geometry if needed, but that requires more complex extraction.

            # Calculate Frechet distance (measure of shape similarity)
            frechet_dist = float("nan")
            try:
                # Ensure both geometries are valid LineStrings before calculation
                if feature_segment_geom.length > 0 and matched_segment_geom.length > 0:
                    frechet_dist = shapely.frechet_distance(
                        matched_segment_geom, feature_segment_geom
                    )
            except Exception as e_frechet:
                logger.debug(
                    f"QA: Could not calculate Frechet distance for tracepoints {u_original_idx}-{v_original_idx}: {e_frechet}"
                )
                # Keep NaN

            # Store calculated properties
            qa_props.append(
                {
                    "matching_confidence": confidence,
                    "matching_index": u_matching_idx,
                    "tracepoint_u_input_index": u_original_idx,  # Index in original coordinate list
                    "tracepoint_v_input_index": v_original_idx,  # Index in original coordinate list
                    "feature_segment_length_m": feature_segment_len_m,
                    "frechet_distance_m": frechet_dist,
                    # Add other QA metrics if desired (e.g., matched segment length from leg data)
                }
            )

    except (KeyError, IndexError, TypeError) as e:
        error_msg = (
            f"Error during geometric QA preparation for feature "
            f"{match_meta.feature_id}: {type(e).__name__} - {e}"
        )
        logger.error(error_msg)
    except Exception as e_qa:  # Catch other unexpected errors
        error_msg = (
            f"Unexpected error during geometric QA for feature "
            f"{match_meta.feature_id}: {type(e_qa).__name__} - {e_qa}"
        )
        logger.exception(error_msg)  # Log traceback for unexpected errors

    return GeometricQaResult(leg_segments_props=qa_props, error=error_msg)


def get_tracepoints_stats_logic(
    match_meta: InternalMatchMetadata,
) -> TracepointStatsResult:
    """
    Calculates summary statistics for the `distance` field of all non-null
    tracepoints in the OSRM response.

    The `distance` field represents the straight-line distance between an input
    coordinate and its matched point on the road graph. These statistics provide
    an overall measure of how far the input trace deviated from the matched path.

    Args:
        match_meta: The InternalMatchMetadata containing the OSRM response.

    Returns:
        A TracepointStatsResult dataclass containing a dictionary of summary
        statistics (count, mean, std, min, max, percentiles) and an optional
        error message.
    """
    stats_dict = {}
    error_msg = None
    try:
        # Extract 'distance' from all non-null tracepoints where distance is a valid number
        distances = [
            tpt["distance"]
            for tpt in match_meta.osrm_response.get("tracepoints", [])
            if tpt is not None and isinstance(tpt.get("distance"), (int, float))
        ]

        if distances:
            # Use pandas describe() for convenient calculation of summary statistics
            # Note: This introduces a pandas dependency for this specific function.
            # If pandas is undesirable, calculate stats manually (mean, std, min, max, etc.).
            stats_series = pd.Series(distances)
            stats_dict = stats_series.describe().to_dict()

            # Convert numpy numeric types (e.g., numpy.float64) to standard Python types
            # for broader compatibility (e.g., JSON serialization).
            stats_dict = {
                k: v.item() if hasattr(v, "item") else v for k, v in stats_dict.items()
            }
        else:
            # Handle case where there are no valid distances
            stats_dict = {"count": 0}

    except KeyError as e:
        # Specifically catch if 'distance' key is missing unexpectedly
        error_msg = (
            f"Error calculating tracepoint stats for feature {match_meta.feature_id}: "
            f"Missing expected key '{e.args[0]}' in tracepoint."
        )
        logger.error(error_msg)
        stats_dict = {}  # Ensure empty dict on error
    except Exception as e:
        # Catch other potential errors during statistics calculation
        error_msg = (
            f"Error calculating tracepoint stats for feature {match_meta.feature_id}: "
            f"{type(e).__name__} - {e}"
        )
        logger.debug(error_msg)  # Log as warning, might not be critical
        stats_dict = {}  # Ensure empty dict on error

    return TracepointStatsResult(stats=stats_dict, error=error_msg)
