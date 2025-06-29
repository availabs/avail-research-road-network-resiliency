from enum import Enum
from os import PathLike
from typing import List, Tuple, TypeAlias, TypedDict

import geopandas
import networkx as nx
import pandera.pandas as pa
from pandera.typing import DataFrame, Index, Series
from pandera.typing.geopandas import GeoSeries

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


# @pa.extensions.register_check_method()
# def is_point(geoseries: GeoSeries):
#     """Ensure all geometries in the GeoSeries are Points."""
#     # The check is more robust if it handles empty geoseries gracefully
#     if geoseries.empty:
#         return True
#     return (geoseries.geom_type == "Point").all()


# @pa.extensions.register_check_method()
# def is_linestring(geoseries: GeoSeries):
#     """Ensure all geometries in the GeoSeries are Points."""
#     # The check is more robust if it handles empty geoseries gracefully
#     if geoseries.empty:
#         return True
#     return (geoseries.geom_type == "linestring").all()


class NodesSchema(pa.DataFrameModel):
    """
    Defines the schema for the nodes GeoDataFrame created from an OSMnx graph.
    The index of the DataFrame is the OSMnx node ID.
    """

    # Index is the OSMnx node ID (integer)
    y: Series[float]
    x: Series[float]
    street_count: Series[int]
    osmid_original: Series[int] = pa.Field(nullable=True)  # Original OSM node ID

    geometry: GeoSeries  # = pa.Field(is_point=())

    class Config:  # type: ignore
        # strict = "filter"  # Ignore columns not defined in schema
        strict = False  # Ignore columns not defined in schema
        coerce = False


class EdgesSchema(pa.DataFrameModel):
    """
    Defines the schema for the enriched edges GeoDataFrame.

    This schema includes attributes from the original OSM data, as well as
    numerous attributes added during the enrichment process, such as road
    classification, naming, and travel time estimations.
    """

    # Index columns
    u: Index[int] = pa.Field()
    v: Index[int] = pa.Field()
    key: Index[int] = pa.Field()

    # --- Base OSMnx attributes from Pyrosm ---
    # osmid: Series[object]  # Can be a list of OSM way IDs
    # oneway: Series[bool]
    # lanes: Series[object] = pa.Field(nullable=True)  # Can be a list of strings
    # name: Series[object] = pa.Field(nullable=True)  # Can be a list of strings
    # highway: Series[object]  # Can be a list of strings
    # maxspeed: Series[object] = pa.Field(nullable=True)  # Can be a list of strings
    # reversed: Series[bool]
    # length: Series[float]  # Length in meters

    geometry: GeoSeries  # = pa.Field(is_linestring=())

    # --- Enrichment attributes from highway_type_analysis_for_way ---
    roadclass: Series[int]
    edge_min_roadclass: Series[int]
    edge_max_roadclass: Series[int]
    # roadtype: Series[str]
    edge_highest_highway_type: Series[str]
    edge_lowest_highway_type: Series[str]

    # --- Other enrichment attributes ---
    # ref: Series[str] = pa.Field(nullable=True)
    # bridge: Series[str] = pa.Field(nullable=True)
    # is_paved: Series[bool]
    # length_mi: Series[float]
    # _intersects_region_: Series[bool]  # FIXME: Alias the solution?
    road_name: Series[str] = pa.Field(nullable=True)
    from_name: Series[str] = pa.Field(nullable=True)
    to_name: Series[str] = pa.Field(nullable=True)
    speed_kph: Series[float]
    travel_time: Series[float]
    osm_way_along_info: Series[object]  # List of dicts from clean_geometries

    class Config:  # type: ignore
        # strict = "filter"  # Ignore columns not defined in schema
        strict = False  # Ignore columns not defined in schema
        coerce = False


class FullEdgesSchema(EdgesSchema):
    merged_edges: Series[object] = pa.Field(nullable=True)


class BaseEnrichedOsmNetworkData(TypedDict, total=True):
    g: nx.MultiDiGraph
    nodes_gdf: DataFrame[NodesSchema]
    edges_gdf: DataFrame[EdgesSchema]


class EnrichedOsmNetworkData(BaseEnrichedOsmNetworkData):
    pass


class FullEnrichedOsmNetworkData(BaseEnrichedOsmNetworkData):
    ENRICH_VERSION: str
    G: nx.MultiDiGraph
    edges_gdf: DataFrame[FullEdgesSchema]  # type: ignore


class EnrichedOsmNetworkDataWithRegions(EnrichedOsmNetworkData):
    region_gdf: geopandas.GeoDataFrame
    buffered_region_gdf: geopandas.GeoDataFrame


class FullEnrichedOsmNetworkDataWithRegions(FullEnrichedOsmNetworkData):
    region_gdf: geopandas.GeoDataFrame
    buffered_region_gdf: geopandas.GeoDataFrame


class OsmNetworkMetadata(TypedDict, total=True):
    osm_pbf: PathLike
    geoid: str
    buffer_dist_mi: int
    region_name: str
    osm_version: str


class SimplifiedEnrichedOsmNetworkDataWithFullMetadata(
    EnrichedOsmNetworkDataWithRegions, OsmNetworkMetadata
):
    pass


class EnrichedOsmNetworkDataWithFullMetadata(
    FullEnrichedOsmNetworkDataWithRegions, OsmNetworkMetadata
):
    pass


class BaseRoadSpanSchema(pa.DataFrameModel):
    u: Index[int] = pa.Field()
    v: Index[int] = pa.Field()
    key: Index[int] = pa.Field()
    span_idx: Index[int] = pa.Field()

    geometry: GeoSeries  # = pa.Field(is_linestring=())

    start_coord_idx: Series[int]
    end_coord_idx: Series[int]
    start_ratio_along: Series[float]
    end_ratio_along: Series[float]
    osmids: Series[object]
    osm_nodes: Series[object]

    class Config:  # type: ignore
        # strict = "filter"  # Ignore columns not defined in schema
        strict = False  # Ignore columns not defined in schema
        coerce = False


class BridgeSpanSchema(BaseRoadSpanSchema):
    pass


class NonBridgeSpanSchema(BaseRoadSpanSchema):
    pass


class CombinedRoadSpansSchema(pa.DataFrameModel):
    u: Index[int] = pa.Field()
    v: Index[int] = pa.Field()
    key: Index[int] = pa.Field()
    span_idx: Index[int] = pa.Field()

    geometry: GeoSeries  # = pa.Field(is_linestring=())
    _road_span_type_: Series[str] = pa.Field(isin=["BRIDGE", "NONBRIDGE"])

    start_coord_idx: Series[int]
    end_coord_idx: Series[int]
    start_ratio_along: Series[float]
    end_ratio_along: Series[float]
    osmids: Series[object]  # list of ints
    osm_nodes: Series[object]  # list of ints or list of lists of ints

    class Config:  # type: ignore
        # strict = "filter"  # Ignore columns not defined in schema
        strict = False  # Ignore columns not defined in schema
        coerce = False
