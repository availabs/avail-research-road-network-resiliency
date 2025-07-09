# FIXME: Right now, the process of converting the undirected OSM edges from the directed
#        may not be deterministic. This would affect JOINing across output that includes
#        this conversion as a processing step.

import argparse
import json
import re
import stat
from pathlib import Path
from typing import Optional, Pattern, Tuple, Union, cast

import duckdb
import duckdb.typing as duckdb_types
import geopandas as gpd
import pandas as pd
from fuzzywuzzy import fuzz
from pandera.typing import DataFrame
from prefect import task

from common.osm.enrich import CombinedRoadSpansSchema, convert_graph_to_gdfs
from common.osm.schemas import (
    EnrichedOsmNetworkDataWithFullMetadata,
    FullEdgesSchema,
    RoadClass,
)
from common.us_census.tiger import get_region_boundary_gdf
from tasks.osm import (
    create_combined_road_spans_task,
    enrich_osm_task,
)

# Define script's directory for robust path creation
SCRIPT_DIR = Path(__file__).resolve().parent

# --- Static Configuration ---
BUFFER_SIZE_METERS = 30
TARGET_CRS = "EPSG:32618"

# Scoring constants for matching
# For preferred road/flowline intersection matches
MATCH_TYPE_PREFERENCE_MULTIPLIER = 3 / 4
# For less-preferred shortest-line-to-span matches
MATCH_TYPE_PENALTY_MULTIPLIER = 4 / 3

# When NAACC crossing type (bridge/non-bridge) matches road span type
# NOTE: The "Bridge" label is rather unreliable because large culverts
#       are often labeled bridges in both the OSM and the NAACC datasets.
CROSSING_TYPE_MATCH_MULTIPLIER = 9 / 10
# When types mismatch
CROSSING_TYPE_MISMATCH_MULTIPLIER = 10 / 9


@task(name="Get all non-parking-aisle Ways enriched osm")
def get_network_type_all_enriched_osm_task(
    osm_pbf: Path,
) -> EnrichedOsmNetworkDataWithFullMetadata:
    enriched_osm = enrich_osm_task(
        osm_pbf=osm_pbf,  #
        network_type="all",
    )

    return enriched_osm


@task(name="Get all undirected non-parking-aisle Ways enriched osm")
def get_undirected_enriched_osm_task(
    enriched_osm: EnrichedOsmNetworkDataWithFullMetadata,
) -> EnrichedOsmNetworkDataWithFullMetadata:
    G = enriched_osm["G"]
    g = enriched_osm["g"]

    # undirected_G = G.to_undirected()
    undirected_G = G

    undirected_g = g.to_undirected()

    undirected_nodes_gdf, undirected_edges_gdf = convert_graph_to_gdfs(undirected_g)  # type: ignore

    undirected_enriched_osm = enriched_osm | {
        "G": undirected_G,
        "g": undirected_g,
        "nodes_gdf": undirected_nodes_gdf,
        "edges_gdf": undirected_edges_gdf,
    }

    return cast(EnrichedOsmNetworkDataWithFullMetadata, undirected_enriched_osm)


def gpd_geom_to_wkb(
    gdf: Union[
        gpd.GeoDataFrame,
        DataFrame[FullEdgesSchema],
        DataFrame[CombinedRoadSpansSchema],
    ],
) -> gpd.GeoDataFrame:
    wkb_gdf = gdf.drop(columns=gdf.geometry.name)

    wkb_gdf["wkb_geometry"] = gdf.geometry.to_wkb()

    wkb_gdf = wkb_gdf.sort_index().reset_index()

    wkb_gdf["_id_"] = wkb_gdf.index

    return cast(gpd.GeoDataFrame, wkb_gdf)


def query_table_existence(
    db_conn: duckdb.DuckDBPyConnection,  #
    table_name: str,
) -> bool:
    res = db_conn.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_name = ?", [table_name]
    ).fetchone()

    return bool(res)


def log_number_of_features_loaded_into_table(
    db_conn: duckdb.DuckDBPyConnection,  #
    table_name: str,
) -> None:
    loaded_count = db_conn.execute(f"SELECT COUNT(*) FROM {table_name};").fetchone()[0]  # type: ignore

    print(f"Loaded {loaded_count} features into {table_name}.")


def load_region_under_study_table(
    db_conn: duckdb.DuckDBPyConnection,  #
    geoid: str,
) -> None:
    table_name = "region_under_study"

    table_exists = query_table_existence(db_conn, table_name)

    if table_exists:
        print(f"Table '{table_name}' already exists. Skipping creation.")
        return

    region_under_study_gdf = get_region_boundary_gdf(geoid=geoid)

    if region_under_study_gdf.crs != "EPSG:4326":
        region_under_study_gdf = region_under_study_gdf.to_crs("EPSG:4326")

    num_features = len(region_under_study_gdf)

    db_conn.execute(f"SET pandas_analyze_sample = {num_features}")

    # --- Register DataFrames and Filter View with DuckDB ---
    db_conn.register("region_under_study_gdf", gpd_geom_to_wkb(region_under_study_gdf))

    db_conn.execute(f"""
        CREATE TABLE {table_name} AS
          SELECT
              ST_Union_Agg(
                ST_GeomFromWKB(wkb_geometry)
              ) AS geom
            FROM
              region_under_study_gdf
        ;
    """)

    db_conn.unregister(view_name="region_under_study")


def load_osm_tables(
    db_conn: duckdb.DuckDBPyConnection,  #
    osm_pbf_path: Path,
) -> None:
    edges_table_name = "osm_undirected_edges"
    spans_table_name = "road_spans"

    edges_table_exists = query_table_existence(db_conn, edges_table_name)
    spans_table_exists = query_table_existence(db_conn, spans_table_name)

    if edges_table_exists and spans_table_exists:
        print(
            f"Tables '{edges_table_name}' and '{spans_table_name} already exist. Skipping creation."
        )
        return

    enriched_osm = get_network_type_all_enriched_osm_task(
        osm_pbf=osm_pbf_path,
    )

    undirected_enriched_osm = get_undirected_enriched_osm_task(
        enriched_osm=enriched_osm
    )

    undirected_all_edges_gdf = undirected_enriched_osm["edges_gdf"]

    if not edges_table_exists:
        num_features = len(undirected_all_edges_gdf)

        db_conn.execute(f"SET pandas_analyze_sample = {num_features}")

        # --- Register DataFrames and Filter View with DuckDB ---
        db_conn.register(
            view_name="undirected_all_edges_gdf",  #
            python_object=gpd_geom_to_wkb(undirected_all_edges_gdf),
        )

        db_conn.execute("""
          CREATE TABLE osm_undirected_edges AS
            SELECT DISTINCT
                a.* EXCLUDE (_id_, wkb_geometry),
                ST_GeomFromWKB(a.wkb_geometry) AS geom
              FROM
                  undirected_all_edges_gdf AS a
                INNER JOIN
                  region_under_study AS b
                    ON ST_Intersects(
                      ST_GeomFromWKB(a.wkb_geometry),
                      b.geom
                    )
              ORDER BY u, v, key
          ;

          ALTER TABLE osm_undirected_edges ADD PRIMARY KEY (u, v, key) ;
        """)

        db_conn.unregister(view_name="undirected_all_edges_gdf")

        log_number_of_features_loaded_into_table(
            db_conn=db_conn,  #
            table_name=edges_table_name,
        )
    else:
        print(f"Table '{edges_table_name}' already exists. Skipping creation.")
        return

    if not spans_table_exists:
        undirected_road_spans_gdf = create_combined_road_spans_task(
            edges_gdf=undirected_all_edges_gdf
        )

        db_conn.register("road_spans_gdf", gpd_geom_to_wkb(undirected_road_spans_gdf))

        db_conn.execute("""
        CREATE TABLE road_spans AS
          SELECT
              a.* EXCLUDE(_id_, wkb_geometry),

              c.roadclass,
              c.road_name     AS road_name,
              c.from_name     AS road_from_name,
              c.to_name       AS road_to_name,
              c.length_mi     AS road_length_mi,

              c.highway       AS road_highway_tags,
              c.service       AS road_service_tags,

              (a.u != c.u)    AS _edge_reversed_,

              ST_GeomFromWKB(a.wkb_geometry) AS geom
            FROM
                road_spans_gdf AS a
              INNER JOIN
                region_under_study AS b
                  ON ST_Intersects(
                    ST_GeomFromWKB(
                      a.wkb_geometry
                    ),
                    b.geom
                  )
              LEFT OUTER JOIN osm_undirected_edges AS c
                ON (
                  ( (a.u, a.v, a.key) = (c.u, c.v, c.key) )
                  OR
                  ( (a.v, a.u, a.key) = (c.u, c.v, c.key) )
                )
          ;

          ALTER TABLE road_spans ADD PRIMARY KEY (u, v, key, span_idx) ;
        """)

        db_conn.unregister(view_name="road_spans_gdf")

        log_number_of_features_loaded_into_table(
            db_conn=db_conn,  #
            table_name=spans_table_name,
        )
    else:
        print(f"Table '{spans_table_name}' already exists. Skipping creation.")
        return


def load_road_flowline_intersections_table(
    db_conn: duckdb.DuckDBPyConnection,  #
    road_flowline_intersections_path: Path,
) -> None:
    table_name = "road_flowline_intersections"

    table_exists = query_table_existence(db_conn, table_name)

    if table_exists:
        print(f"Table '{table_name}' already exists. Skipping creation.")
        return

    road_flowline_intersections_gdf = gpd.read_file(
        road_flowline_intersections_path,  #
        layer="roadspans_x_flowlines_with_risk",
    )

    num_features = len(road_flowline_intersections_gdf)

    db_conn.execute(f"SET pandas_analyze_sample = {num_features}")

    # --- Register DataFrames and Filter View with DuckDB ---
    db_conn.register(
        "road_intersections_initial_gdf",
        gpd_geom_to_wkb(road_flowline_intersections_gdf),
    )
    db_conn.execute("""
      CREATE TABLE road_flowline_intersections AS
        SELECT
            a.* EXCLUDE (_id_, wkb_geometry),
            ST_GeomFromWKB(a.wkb_geometry) AS geom
          FROM
             road_intersections_initial_gdf AS a
          LEFT OUTER JOIN
            road_intersections_initial_gdf AS b
              ON (
                ( (a.u, a.v, a.key) = (b.v, b.u, b.key) )
                AND
                ( a._id_ > b._id_ )
              )
            INNER JOIN
              region_under_study AS c
                ON ST_Intersects(ST_GeomFromWKB(a.wkb_geometry), c.geom)
          WHERE (b._id_ IS NULL)
      ;
    """)

    db_conn.unregister(view_name="road_intersections_initial_gdf")

    log_number_of_features_loaded_into_table(
        db_conn=db_conn,  #
        table_name=table_name,
    )


def load_naacc_crossings_tables(
    db_conn: duckdb.DuckDBPyConnection,  #
    naacc_crossings_path: Path,
) -> None:
    table_name = "naacc_crossings"

    table_exists = query_table_existence(db_conn, table_name)

    if table_exists:
        print(f"Table '{table_name}' already exists. Skipping creation.")
        return

    naacc_df = pd.read_csv(naacc_crossings_path, encoding="cp1252")
    naacc_gdf = gpd.GeoDataFrame(
        naacc_df,
        geometry=gpd.points_from_xy(
            naacc_df.GPS_X_Coordinate, naacc_df.GPS_Y_Coordinate
        ),
        crs="EPSG:4326",
    )
    num_features = len(naacc_gdf)

    db_conn.execute(f"SET pandas_analyze_sample = {num_features}")

    # --- Register DataFrames and Filter View with DuckDB ---
    db_conn.register("naacc_initial_gdf", gpd_geom_to_wkb(naacc_gdf))

    db_conn.execute("""
        CREATE TABLE naacc_crossings AS
          SELECT DISTINCT ON (Crossing_Code)
              a.* EXCLUDE (_id_, wkb_geometry),
              ( UPPER(Road_Type) = 'RAILROAD' ) AS _is_railway_crossing_,
              ST_GeomFromWKB(a.wkb_geometry) AS geom
            FROM
                naacc_initial_gdf AS a
              INNER JOIN
                region_under_study AS b
                  ON ST_Intersects(ST_GeomFromWKB(a.wkb_geometry), b.geom)
            ORDER BY
                a.Crossing_Code,
                Date_Last_Updated DESC NULLS LAST,
                Survey_ID DESC NULLS LAST,
                _id_
        ;
    """)

    db_conn.unregister(view_name="naacc_initial_gdf")

    log_number_of_features_loaded_into_table(
        db_conn=db_conn,  #
        table_name=table_name,
    )


def match_crossings_to_intersections(db_conn: duckdb.DuckDBPyConnection) -> None:
    """
    Performs Stage 1: Joins NAACC crossings to road/flowline intersections.
    """
    print("\n--- Stage 1: Matching NAACC Crossings to Road/Flowline Intersections ---")

    db_conn.execute(f"""
        DROP TABLE IF EXISTS stage1_matched ;

        CREATE TABLE stage1_matched AS
          SELECT
              osm_x_flowlines.u                       AS osm_road_u,
              osm_x_flowlines.v                       AS osm_road_v,
              osm_x_flowlines.key                     AS osm_road_key,
              osm_x_flowlines._road_span_type_        AS osm_road_span_type,
              osm_x_flowlines.span_idx                AS osm_road_span_idx,

              naacc.Crossing_Code                     AS naacc_crossing_code,

              osm_x_flowlines.permanent_identifier    AS nhd_flowline_permanent_identifier,
              osm_x_flowlines.intxn_idx               AS osm_road_span_nhd_flowline_intxn_idx,

              ST_Distance(
                ST_Transform(
                  naacc.geom,
                  'EPSG:4326',
                  '{TARGET_CRS}',
                  always_xy := True
                ),
                ST_Transform(
                  osm_x_flowlines.geom,
                  'EPSG:4326',
                  '{TARGET_CRS}',
                  always_xy := True
                )
              ) as match_distance_m,

              ROW_NUMBER() OVER(
                  PARTITION BY
                      naacc_crossing_code
                  ORDER BY
                      match_distance_m
              ) as match_rank,

              'road_flowline_intersection_match' as match_type,

              ST_MakeLine(
                naacc.geom ,
                osm_x_flowlines.geom
              ) AS geom

            FROM
                naacc_crossings AS naacc
              INNER JOIN
                road_flowline_intersections AS osm_x_flowlines
                  ON (
                    ST_DWithin(
                      ST_Transform(
                        naacc.geom,
                        'EPSG:4326',
                        '{TARGET_CRS}',
                        always_xy := True
                      ),
                      ST_Transform(
                        osm_x_flowlines.geom,
                        'EPSG:4326',
                        '{TARGET_CRS}',
                        always_xy := True
                      ),
                      {BUFFER_SIZE_METERS}
                    )
                  )
            WHERE (NOT naacc._is_railway_crossing_)
        ;
    """)

    log_number_of_features_loaded_into_table(
        db_conn=db_conn,  #
        table_name="stage1_matched",
    )


def match_crossings_to_road_spans(db_conn: duckdb.DuckDBPyConnection) -> None:
    """
    Performs Stage 2: Finds all potential road span matches within the buffer
    for ALL NAACC crossings. The decision is deferred to the final combination step.
    """
    print("\n--- Stage 2: Finding All Potential Road Span Matches ---")

    db_conn.execute(f"""
        DROP TABLE IF EXISTS stage2_matched ;

        CREATE TABLE stage2_matched AS
            SELECT
                road_spans.u                    AS osm_road_u,
                road_spans.v                    AS osm_road_v,
                road_spans.key                  AS osm_road_key,
                road_spans._road_span_type_     AS osm_road_span_type,
                road_spans.span_idx             AS osm_road_span_idx,

                naacc.Crossing_Code             AS naacc_crossing_code,

                ST_Distance(
                    ST_Transform(
                      naacc.geom,
                      'EPSG:4326',
                      '{TARGET_CRS}',
                      always_xy := True
                    ),
                    ST_Transform(
                      road_spans.geom,
                      'EPSG:4326',
                      '{TARGET_CRS}',
                      always_xy := True
                    )
                ) AS match_distance_m,

                ROW_NUMBER() OVER(
                    PARTITION BY
                        naacc_crossing_code
                    ORDER BY
                        match_distance_m
                ) as match_rank,

                'shortest_line_to_road_span' AS match_type,

                ST_ShortestLine(
                  naacc.geom,
                  road_spans.geom
                ) AS geom,

              FROM
                  naacc_crossings AS naacc
                INNER JOIN road_spans AS road_spans
                  ON ST_DWithin(
                    ST_Transform(
                      naacc.geom,
                      'EPSG:4326',
                      '{TARGET_CRS}',
                      always_xy := True
                    ),
                    ST_Transform(
                      road_spans.geom,
                      'EPSG:4326',
                      '{TARGET_CRS}',
                      always_xy := True
                    ),
                    {BUFFER_SIZE_METERS}
                  )
              WHERE (NOT naacc._is_railway_crossing_)
        ;
    """)

    log_number_of_features_loaded_into_table(
        db_conn=db_conn,  #
        table_name="stage2_matched",
    )


def _b(pattern_str: str) -> Pattern:
    """Compiles a string into a regex pattern with word boundaries."""
    return re.compile(r"\b" + pattern_str + r"\b")


# Pre-compile regular expressions for performance improvement.
# This list is created once when the module is loaded, not on every function call.
REPLACEMENTS_PIPELINE = [
    # First, handle single-word terms.
    (_b("north"), "n"),
    (_b("south"), "s"),
    (_b("east"), "e"),
    (_b("west"), "w"),
    (_b("road"), "rd"),
    (_b("street"), "st"),
    (_b("avenue"), "ave"),
    (_b("boulevard"), "blvd"),
    (_b("drive"), "dr"),
    (_b("lane"), "ln"),
    (_b("court"), "ct"),
    (_b("place"), "pl"),
    (_b("circle"), "cir"),
    (_b("trail"), "trl"),
    (_b("parkway"), "pkwy"),
    (_b("highway"), "hwy"),
    (_b("route"), "rte"),
    (_b("county"), "co"),
    (_b("state"), "st"),
    # Now, handle multi-word terms using the standardized single words.
    (_b("co rd"), "cr"),
    (_b("co rte"), "cr"),
    (_b("st rte"), "sr"),
    (_b("st hwy"), "sr"),
    (_b("us hwy"), "us"),
]


def calculate_road_name_similarity(
    osm_road_name: Optional[str], naacc_road_name: Optional[str]
) -> Optional[Tuple[Optional[float], Optional[str], Optional[str]]]:
    """
    Calculates a similarity score and returns the normalized names for comparison.

    The function performs several steps to provide a robust similarity score:
    1.  **Splitting Composite Names**: The OSM road name is split by "/" to handle
        composite names like "US 9W/River Road". Each part is compared against the
        NAACC road name, and the highest score is returned.
    2.  **Normalization**: Converts both names to lowercase, removes punctuation, and
        standardizes common road type abbreviations.
    3.  **Numeric Identifier Matching**: It extracts numeric parts from both names,
        including optional single-letter suffixes (e.g., "9W"). A match on these
        identifiers results in a high score of 95.
    4.  **Fuzzy String Matching**: If a numeric match isn't found, it uses
        `fuzz.ratio` for standard string similarity.

    Args:
        osm_road_name (Optional[str]): The road name from the OpenStreetMap dataset.
        naacc_road_name (Optional[str]): The road name from the NAACC dataset.

    Returns:
        A tuple containing:
        - The similarity score (float).
        - The normalized OSM name part that produced the best score (str).
        - The normalized NAACC name (str).
        Returns (0.5, None, None) if both names are missing,
        and (0.0, None, None) if one is missing.
    """
    osm_is_missing = not osm_road_name
    naacc_is_missing = not naacc_road_name

    if osm_is_missing and naacc_is_missing:
        return 0.5, None, None
    if osm_is_missing or naacc_is_missing:
        return 0.0, None, None

    # Normalization helper function
    def normalize(name: str) -> str:
        """
        Standardizes a road name for comparison.
        """
        name = name.lower()
        # Replace any sequence of one or more non-alphanumeric characters with a single space.
        name = re.sub(r"[^\w\s]+", " ", name)

        # Apply replacements using the pre-compiled regex list
        for pattern, new in REPLACEMENTS_PIPELINE:
            name = pattern.sub(new, name)

        return name

    # Split OSM road name by '/' to handle composite names.
    osm_name_parts = osm_road_name.split("/")

    # Normalize the NAACC name once before the loop.
    norm_naacc_name = normalize(naacc_road_name).strip()
    # This regex finds a number followed by a single optional letter (but not if it's part of a longer word like '1st'), OR just a number.
    naacc_numbers = set(re.findall(r"\d+[a-z](?!\w)|\d+", norm_naacc_name))

    max_score = 0.0
    best_osm_part = ""
    # Iterate through each part of the (potentially composite) OSM name.
    for part in osm_name_parts:
        # Normalize the current OSM part.
        norm_osm_part = normalize(part.strip())
        if not norm_osm_part:
            continue

        # This regex finds a number followed by a single optional letter (but not if it's part of a longer word like '1st'), OR just a number.
        osm_numbers = set(re.findall(r"\d+[a-z](?!\w)|\d+", norm_osm_part))

        current_score = 0.0
        # --- Intelligent Comparison (prioritizing numeric route identifiers) ---
        if osm_numbers and naacc_numbers and osm_numbers == naacc_numbers:
            current_score = 95.0
        else:
            # --- Fuzzy Matching for textual parts ---
            current_score = float(fuzz.ratio(norm_osm_part, norm_naacc_name))

        # Update the max score and the best matching OSM part.
        if current_score > max_score:
            max_score = current_score
            best_osm_part = norm_osm_part
            # Optimization: if we get a perfect score, no need to check other parts.
            if max_score == 100.0:
                break

    return max_score, best_osm_part, norm_naacc_name


def calculate_match_score(
    match_type: str,
    crossing_type: Optional[str],
    osm_road_span_type: Optional[str],
    match_distance_m: float,
    name_similarity_score: Optional[float],
    osm_is_roadway: Optional[bool],
    osm_is_service_road: Optional[bool],
    naacc_is_trail: Optional[bool],
    naacc_is_unnamed_road: Optional[bool],
    naacc_is_driveway: Optional[bool],
) -> float:
    """
    Calculates a preference score for a match. Lower is better.
    This function is registered as a UDF in DuckDB.
    The score starts with distance and is modified by multipliers to favor
    more confident matches.
    """

    match_score = match_distance_m

    if match_type == "road_flowline_intersection_match":
        match_score *= MATCH_TYPE_PREFERENCE_MULTIPLIER
    else:
        match_score *= MATCH_TYPE_PENALTY_MULTIPLIER

    # Handle cases where crossing_type might be None from the database.
    crossing_class = "NONBRIDGE"

    if crossing_type and "BRIDGE" in crossing_type.upper():
        crossing_class = "BRIDGE"

    if crossing_class == osm_road_span_type:
        match_score *= CROSSING_TYPE_MATCH_MULTIPLIER
    else:
        match_score *= CROSSING_TYPE_MISMATCH_MULTIPLIER

    name_similarity_score = name_similarity_score or 0

    name_similarity_multiplier = 1 - ((name_similarity_score / 100) * 0.2)

    match_score *= name_similarity_multiplier

    if osm_is_roadway:
        if osm_is_service_road:
            if naacc_is_trail:
                match_score *= 10
            elif naacc_is_driveway:
                match_score *= 1.5
        elif naacc_is_trail or naacc_is_driveway:
            match_score *= 100
        elif naacc_is_unnamed_road:
            match_score *= 10

    return match_score


def create_scored_candidate_matches_table(
    db_conn: duckdb.DuckDBPyConnection,
) -> None:
    """
    Combines results from Stage 1 and 2, chooses the best match for each
    crossing using a UDF for scoring, and fetches final GDFs from DuckDB.
    """
    print("\n--- Combining, Prioritizing, and Fetching All Results ---")

    # Register the UDF with DuckDB
    db_conn.create_function(
        "calculate_road_name_similarity",  # Name of the function in SQL
        calculate_road_name_similarity,  # The Python function
        [duckdb_types.VARCHAR, duckdb_types.VARCHAR],  # Input types
        # Define the output as a STRUCT with named fields
        duckdb.struct_type(
            {
                "similarity_score": duckdb_types.DOUBLE,
                "osm_name_normalized": duckdb_types.VARCHAR,
                "naacc_name_normalized": duckdb_types.VARCHAR,
            }
        ),
    )

    # Create a Python UDF to handle the complex scoring logic
    db_conn.create_function(
        "calculate_match_score",
        calculate_match_score,  # type: ignore
        [
            duckdb_types.VARCHAR,
            duckdb_types.VARCHAR,
            duckdb_types.VARCHAR,
            duckdb_types.DOUBLE,
            duckdb_types.DOUBLE,
            duckdb_types.BOOLEAN,
            duckdb_types.BOOLEAN,
            duckdb_types.BOOLEAN,
            duckdb_types.BOOLEAN,
            duckdb_types.BOOLEAN,
        ],
        duckdb_types.DOUBLE,
    )

    db_conn.execute(f"""
      DROP SEQUENCE IF EXISTS scored_candidate_matches_id_seq ;
      CREATE SEQUENCE scored_candidate_matches_id_seq START 1 ;

      DROP TABLE IF EXISTS scored_candidate_matches ;
      CREATE TABLE scored_candidate_matches  AS
        WITH cte_match_meta AS (
          SELECT
              osm_road_u,
              osm_road_v,
              osm_road_key,

              osm.osm_road_name,
              osm.osm_from_name,
              osm.osm_to_name,
              osm.osm_road_class,
              osm.osm_road_type,

              osm.osm_is_roadway,
              osm.osm_is_service_road,

              candidate_matches.naacc_crossing_code,
              naacc.Crossing_Type                           AS naacc_crossing_type,
              naacc.Road                                    AS naacc_road_name,
              naacc.Road_Type                               AS naacc_road_type,
              CONTAINS(UPPER(naacc.Road_Type), 'TRAIL')     AS naacc_is_trail,

              (
                ( COALESCE(naacc.Road, '') = '' )
                OR
                CONTAINS(UPPER(naacc.Road), 'UNNAMED')
              ) AS naacc_is_unnamed_road,

              (
                ( UPPER(naacc.Road) = 'DRIVEWAY' )
                OR
                ( UPPER(naacc.Road_Type) = 'DRIVEWAY' )
              ) AS naacc_is_driveway,

              calculate_road_name_similarity(
                osm.osm_road_name,
                CASE
                  WHEN (
                    CONTAINS(UPPER(naacc.Road), 'DRIVEWAY')
                    OR
                    CONTAINS(UPPER(naacc.Road), 'UNNAMED')
                  ) THEN NULL
                  ELSE naacc.Road
                END
              ) AS road_name_similarity_result
            FROM (
              SELECT
                  osm_road_u,
                  osm_road_v,
                  osm_road_key,
                  naacc_crossing_code
                FROM
                  stage1_matched
              UNION
              SELECT
                  osm_road_u,
                  osm_road_v,
                  osm_road_key,
                  naacc_crossing_code
                FROM
                  stage2_matched
            ) AS candidate_matches
              INNER JOIN (
                  SELECT DISTINCT ON (osm_road_u, osm_road_v, osm_road_key)
                      u             AS osm_road_u,
                      v             AS osm_road_v,
                      key           AS osm_road_key,

                      road_name     AS osm_road_name,
                      from_name     AS osm_from_name,
                      to_name       AS osm_to_name,

                      roadclass     AS osm_road_class,
                      roadtype      AS osm_road_type,

                      (roadclass < {RoadClass.other.value})     AS osm_is_roadway,
                      (roadclass = {RoadClass.service.value})   AS osm_is_service_road
                    FROM
                      osm_undirected_edges
                ) AS osm
                  USING (osm_road_u, osm_road_v, osm_road_key)
              INNER JOIN naacc_crossings AS naacc
                  ON (candidate_matches.naacc_crossing_code = naacc.Crossing_Code)
        )
        SELECT
            nextval('scored_candidate_matches_id_seq') AS _id_,

            t.*,

            ROW_NUMBER() OVER(
                PARTITION BY
                    t.naacc_crossing_code
                ORDER BY t.match_score
            ) AS rank_across_match_types_for_crossing

          FROM (
            SELECT
                *,

                ROW_NUMBER() OVER(
                    PARTITION BY
                        naacc_crossing_code
                    ORDER BY match_score
                ) AS rank_within_match_type_for_crossing,

                ROW_NUMBER() OVER(
                    PARTITION BY
                        osm_road_u,
                        osm_road_v,
                        osm_road_key,
                        osm_road_span_type,
                        osm_road_span_idx,

                        nhd_flowline_permanent_identifier,
                        osm_road_span_nhd_flowline_intxn_idx
                    ORDER BY match_score
                ) AS rank_within_match_type_for_road_flowline_intxn
              FROM (
                SELECT
                    -- START: Primary Key
                    osm_road_u,
                    osm_road_v,
                    osm_road_key,
                    s1.osm_road_span_type,
                    s1.osm_road_span_idx,

                    s1.nhd_flowline_permanent_identifier,
                    s1.osm_road_span_nhd_flowline_intxn_idx,

                    naacc_crossing_code,
                    -- END: Primary Key

                    match_meta.naacc_crossing_type,

                    s1.match_distance_m,
                    s1.match_type,

                    match_meta.osm_road_name,
                    match_meta.osm_from_name,
                    match_meta.osm_to_name,

                    match_meta.osm_road_class,
                    match_meta.osm_road_type,

                    match_meta.osm_is_roadway,
                    match_meta.osm_is_service_road,

                    match_meta.naacc_road_name,
                    match_meta.naacc_is_trail, 
                    match_meta.naacc_is_unnamed_road,
                    match_meta.naacc_is_driveway, 

                    COALESCE(
                      (
                        match_meta.osm_is_roadway
                        AND
                        match_meta.naacc_is_trail
                      )
                      , FALSE
                    ) AS match_has_inconsistent_road_types,

                    (match_meta.road_name_similarity_result).osm_name_normalized
                        AS osm_road_name_normalized,

                    (match_meta.road_name_similarity_result).naacc_name_normalized
                        AS naacc_road_name_normalized,

                    (match_meta.road_name_similarity_result).similarity_score
                        AS match_road_name_similarity_score,

                    calculate_match_score(
                        s1.match_type,
                        match_meta.naacc_crossing_type,
                        s1.osm_road_span_type,
                        s1.match_distance_m,
                        (match_meta.road_name_similarity_result).similarity_score,
                        match_meta.osm_is_roadway,
                        match_meta.osm_is_service_road,
                        match_meta.naacc_is_trail,
                        match_meta.naacc_is_unnamed_road,
                        match_meta.naacc_is_driveway
                    ) as match_score,

                    s1.geom
                FROM
                    stage1_matched AS s1
                  INNER JOIN cte_match_meta AS match_meta
                    USING (
                      osm_road_u,
                      osm_road_v,
                      osm_road_key,
                      naacc_crossing_code
                    )
              )

            UNION ALL

            SELECT
                *,

                ROW_NUMBER() OVER(
                    PARTITION BY
                        naacc_crossing_code
                    ORDER BY match_score
                ) AS rank_within_match_type_for_crossing,

                NULL AS rank_within_match_type_for_road_flowline_intxn -- Does not apply.
              FROM (
                SELECT
                    -- START: Primary Key
                    osm_road_u,
                    osm_road_v,
                    osm_road_key,
                    s2.osm_road_span_type,
                    s2.osm_road_span_idx,

                    NULL AS nhd_flowline_permanent_identifier,
                    NULL AS osm_road_span_nhd_flowline_intxn_idx,

                    naacc_crossing_code,
                    -- END: Primary Key

                    match_meta.naacc_crossing_type,

                    s2.match_distance_m,
                    s2.match_type,

                    match_meta.osm_road_name,
                    match_meta.osm_from_name,
                    match_meta.osm_to_name,

                    match_meta.osm_road_class,
                    match_meta.osm_road_type,

                    match_meta.osm_is_roadway,
                    match_meta.osm_is_service_road,

                    match_meta.naacc_road_name,
                    match_meta.naacc_is_trail, 
                    match_meta.naacc_is_unnamed_road,
                    match_meta.naacc_is_driveway, 

                    COALESCE(
                      (
                        match_meta.osm_is_roadway
                        AND
                        match_meta.naacc_is_trail
                      )
                      , FALSE
                    ) AS match_has_inconsistent_road_types,

                    (match_meta.road_name_similarity_result).osm_name_normalized
                        AS osm_road_name_normalized,

                    (match_meta.road_name_similarity_result).naacc_name_normalized
                        AS naacc_road_name_normalized,

                    (match_meta.road_name_similarity_result).similarity_score
                        AS match_road_name_similarity_score,

                    calculate_match_score(
                        s2.match_type,
                        match_meta.naacc_crossing_type,
                        s2.osm_road_span_type,
                        s2.match_distance_m,
                        (match_meta.road_name_similarity_result).similarity_score,
                        match_meta.osm_is_roadway,
                        match_meta.osm_is_service_road,
                        match_meta.naacc_is_trail,
                        match_meta.naacc_is_unnamed_road,
                        match_meta.naacc_is_driveway
                    ) as match_score,

                    s2.geom
                  FROM
                      stage2_matched AS s2
                    INNER JOIN cte_match_meta AS match_meta
                      USING (
                        osm_road_u,
                        osm_road_v,
                        osm_road_key,
                        naacc_crossing_code
                      )
            )
          ) AS t

          ORDER BY
            rank_within_match_type_for_road_flowline_intxn DESC NULLS LAST,
            match_score
      ;
    """)

    log_number_of_features_loaded_into_table(
        db_conn=db_conn,  #
        table_name="scored_candidate_matches",
    )


def create_final_matches_table(
    db_conn: duckdb.DuckDBPyConnection,
) -> None:
    db_conn.execute("""
      DROP TABLE IF EXISTS final_matches ;

      CREATE TABLE final_matches AS
        SELECT DISTINCT
            naacc_crossing_code,
            any_value(COLUMNS(* EXCLUDE(naacc_crossing_code))),
            'All ideal matches, no arguments between nearby Road/Flowline intersections' AS match_reason
          FROM scored_candidate_matches
          WHERE (
            ( NOT match_has_inconsistent_road_types )
            AND
            ( rank_within_match_type_for_road_flowline_intxn = 1 )
            AND
            ( match_distance_m < 20 )
          )
          GROUP BY (naacc_crossing_code)
          HAVING COUNT(DISTINCT(_id_) = 1) -- No other scored_candidate match has naacc_crossing_code ranked #1.
          ORDER BY naacc_crossing_code
      ;

      ALTER TABLE final_matches ADD PRIMARY KEY (naacc_crossing_code) ;
    """)

    db_conn.execute("""
      INSERT INTO final_matches
        SELECT
            a.naacc_crossing_code,
            any_value(COLUMNS(a.* EXCLUDE(a.naacc_crossing_code))),
            'High confidence decision for Road/Flowline intersection' AS match_reason
          FROM scored_candidate_matches AS a
            LEFT OUTER JOIN scored_candidate_matches AS b
              -- Search for any other road/flowline candidate matches with legit claim to NAACC crossing.
              ON (
                ( a.naacc_crossing_code = b.naacc_crossing_code )
                AND
                ( NOT b.match_has_inconsistent_road_types )
                AND
                ( b.rank_within_match_type_for_road_flowline_intxn = 1 )
                AND
                ( b.match_distance_m < 20 )
                AND
                ( b.match_road_name_similarity_score > 80.0 )
              )
            LEFT OUTER JOIN final_matches AS c
              ON (a.naacc_crossing_code = c.naacc_crossing_code)
            LEFT OUTER JOIN final_matches AS d
              ON (
                (
                  a.osm_road_u,
                  a.osm_road_v,
                  a.osm_road_key,
                  a.osm_road_span_type,
                  a.osm_road_span_idx,
                  a.nhd_flowline_permanent_identifier,
                  a.osm_road_span_nhd_flowline_intxn_idx
                )
                =
                (
                  d.osm_road_u,
                  d.osm_road_v,
                  d.osm_road_key,
                  d.osm_road_span_type,
                  d.osm_road_span_idx,
                  d.nhd_flowline_permanent_identifier,
                  d.osm_road_span_nhd_flowline_intxn_idx
                )
              )
          WHERE (
            ( NOT a.match_has_inconsistent_road_types )
            AND
            ( a.rank_within_match_type_for_road_flowline_intxn = 1 )
            AND
            ( a.match_distance_m < 15 )
            AND
            ( a.match_road_name_similarity_score > 90.0 )
            AND 
            ( b.naacc_crossing_code IS NULL )
            AND
            ( d.osm_road_u IS NULL )
          )
        GROUP BY (a.naacc_crossing_code)
        HAVING ( COUNT(1) = 1 ) -- No other scored_candidate match has naacc_crossing_code ranked #1.
        ORDER BY a.naacc_crossing_code
      ;
    """)

    db_conn.execute("""
      INSERT INTO final_matches
        SELECT DISTINCT ON (naacc_crossing_code)
            naacc_crossing_code,
            a.* EXCLUDE(naacc_crossing_code),
            'Min match rank across all types for crossing' AS match_reason
          FROM scored_candidate_matches AS a
            LEFT OUTER JOIN final_matches AS b
              USING (naacc_crossing_code)
            LEFT OUTER JOIN final_matches AS c
              ON (
                (
                  a.osm_road_u,
                  a.osm_road_v,
                  a.osm_road_key,
                  a.osm_road_span_idx,
                  a.nhd_flowline_permanent_identifier,
                  a.osm_road_span_nhd_flowline_intxn_idx
                )
                =
                (
                  c.osm_road_u,
                  c.osm_road_v,
                  c.osm_road_key,
                  c.osm_road_span_idx,
                  c.nhd_flowline_permanent_identifier,
                  c.osm_road_span_nhd_flowline_intxn_idx
                )
              )
          WHERE (
            ( NOT a.match_has_inconsistent_road_types )
            AND
            ( b.naacc_crossing_code IS NULL )
          )
          ORDER BY
              a.naacc_crossing_code,
              (c._id_ IS NULL) DESC,
              a.rank_across_match_types_for_crossing 
      ;
    """)

    log_number_of_features_loaded_into_table(
        db_conn=db_conn,  #
        table_name="final_matches",
    )


# def create_final_crossing_points_table(
#     db_conn: duckdb.DuckDBPyConnection,
# ) -> None:
#     """
#     Creates a table of the final matched crossing points on the road network.
#     The geometry is the endpoint of the snap line from the final_matches table.
#     """
#     print("\n\n\n--- Creating Final Crossing Points Table ---")

#     db_conn.execute("""
#         DROP TABLE IF EXISTS osm_ways_x_nhd_flowlines_x_naacc_crossings ;
#         CREATE TABLE osm_ways_x_nhd_flowlines_x_naacc_crossings AS
#           SELECT
#               * EXCLUDE(geom),
#               ST_EndPoint(geom) AS geom
#             FROM
#               final_matches;
#     """)

#     log_number_of_features_loaded_into_table(
#         db_conn=db_conn,  #
#         table_name="osm_ways_x_nhd_flowlines_x_naacc_crossings",
#     )


def create_final_crossing_points_table(
    db_conn: duckdb.DuckDBPyConnection,
) -> None:
    """
    Creates a table of the final matched crossing points on the road network.
    The geometry is the endpoint of the snap line from the final_matches table.
    """
    print("\n\n\n--- Creating Final Crossing Points Table ---")

    db_conn.execute("""
        DROP TABLE IF EXISTS osm_ways_x_nhd_flowlines_x_naacc_crossings ;
        CREATE TABLE osm_ways_x_nhd_flowlines_x_naacc_crossings AS
          SELECT DISTINCT ON (naacc_crossing_code)
              a.osm_road_name,
              a.osm_from_name,
              a.osm_to_name,

              a.naacc_road_name,
              b.Crossing_Comment      AS naacc_crossing_comment,
              b.Location_Description  AS naacc_location_description,

              b.Stream_Name AS naacc_stream_name,
              c.gnis_name   AS nhd_waterway_name,

              a.match_distance_m,

              -- START: New Confidence Score Calculation
              ROUND(
                LEAST(
                  85.0,
                  GREATEST(
                    0.0,
                    CASE
                      -- 1. Top Tier: Unanimous, high-confidence intersection matches
                      WHEN starts_with(a.match_reason, 'All ideal matches')
                        THEN 95.0
                      
                      -- 2. Second Tier: High-confidence matches that survived the anti-join
                      WHEN starts_with(a.match_reason, 'High confidence decision')
                        THEN 85.0

                      -- 3. Final Tier: Score is calculated based on several factors
                      ELSE (
                        -- Start with a base score
                        (20.0 - a.match_distance_m)
                        + (a.match_road_name_similarity_score / 1.5)
                        + CASE WHEN a.match_type = 'road_flowline_intersection_match' THEN 5 ELSE 0 END
                        -- Penalize for inconsistent road types (e.g., roadway matching a trail)
                        - CASE WHEN a.match_has_inconsistent_road_types THEN 15 ELSE 0 END
                      )
                    END 
                  )
                )
              ) AS match_confidence_score,
              -- END: New Confidence Score Calculation

              a.naacc_crossing_code,
              a.naacc_crossing_type,
              b.Inlet_Structure_type      AS naacc_inlet_structure_type,
              b.Outlet_Structure_type     AS naacc_outlet_structure_type,
              b.Structure_Comment         AS naacc_structure_comment,

              a.nhd_flowline_permanent_identifier,
              c.ftype       AS nhd_waterway_type,

              CASE c.ftype
                WHEN 334	THEN 'Connector'
                WHEN 336	THEN 'CanalDitch'
                WHEN 420	THEN 'Underground Conduit'
                WHEN 428	THEN 'Pipeline'
                WHEN 460	THEN 'StreamRiver'
                WHEN 468	THEN 'Drainageway'
                WHEN 558	THEN 'ArtificialPath'
                WHEN 566	THEN 'Coastline'
                ELSE NULL
              END AS nhd_waterway_type_description,

              CASE c.fcode
                WHEN 46006  THEN 'StreamRiver - Perennial'
                WHEN 46003  THEN 'StreamRiver - Intermittent'
                WHEN 46007  THEN 'StreamRiver - Ephemeral'
                ELSE NULL
              END AS nhd_stream_type_description,

              c.qema AS 'nhd_mean_annual_gage_adjusted_flow_cu_ft_per_sec',

              a.match_score,

              a.osm_road_name_normalized,
              a.naacc_road_name_normalized,

              a.match_road_name_similarity_score,

              a.match_type,

              a.osm_road_class,
              a.osm_road_type,
              a.osm_is_roadway,
              a.osm_is_service_road,
              a.naacc_is_trail,
              a.naacc_is_unnamed_road,
              a.naacc_is_driveway,
              a.match_has_inconsistent_road_types,
              a.match_reason,

              a.osm_road_u,
              a.osm_road_v,
              a.osm_road_key,
              a.osm_road_span_type,
              a.osm_road_span_idx,
              a.osm_road_span_nhd_flowline_intxn_idx,

              ST_EndPoint(a.geom) AS geom
            FROM
                final_matches AS a
              INNER JOIN naacc_crossings AS b
                ON (a.naacc_crossing_code = b.Crossing_Code)
              LEFT OUTER JOIN road_flowline_intersections AS c
                ON (
                  -- NOTE: If loop (u == v), then there will be 2 matches, making DISTINCT ON necessary.
                  (
                    (
                      a.osm_road_u,
                      a.osm_road_v,
                      a.osm_road_key,
                      a.osm_road_span_idx,
                      a.nhd_flowline_permanent_identifier,
                      a.osm_road_span_nhd_flowline_intxn_idx
                    )
                    IN
                    (
                      (c.u, c.v, c.key, c.span_idx, c.permanent_identifier, c.intxn_idx),
                      (c.v, c.u, c.key, c.span_idx, c.permanent_identifier, c.intxn_idx)
                    )
                  )
                )
        ;
    """)

    log_number_of_features_loaded_into_table(
        db_conn=db_conn,  #
        table_name="osm_ways_x_nhd_flowlines_x_naacc_crossings",
    )


def export_database_to_geoparquet(
    db_conn: duckdb.DuckDBPyConnection,  #
    export_dir: Path,
) -> None:
    """
    Exports all tables from the DuckDB database to a GeoParquet file structure.
    """
    print("\n--- Exporting Full Database to GeoParquet Structure ---")
    export_dir.mkdir(exist_ok=True, parents=True)

    db_conn.execute(f"EXPORT DATABASE '{export_dir}' (FORMAT PARQUET);")

    print(f"Database successfully exported to directory: {export_dir}")


def export_database_to_gpkg(
    db_conn: duckdb.DuckDBPyConnection,  #
    export_path: Path,
) -> None:
    """
    Exports all tables from the DuckDB database to a single GeoPackage file.
    Complex types (LIST, MAP) are automatically converted to JSON strings.
    """
    print("\n--- Exporting Full Database to GeoPackage ---")
    export_path.unlink(missing_ok=True)  # Start with a fresh file

    tables = db_conn.execute("SHOW TABLES;").df()

    for _, table_row in tables.iterrows():
        table_name = table_row["name"]
        print(f"  - Processing and exporting table: {table_name}")

        try:
            schema_df = db_conn.execute(f"DESCRIBE {table_name};").df()
            columns = schema_df["column_name"].tolist()

            # Identify geometry column and prepare for fetching
            if "geom" in columns:
                query = f"""
                  SELECT
                      * EXCLUDE(geom),
                      ST_AsWKB(geom) AS wkb_geometry
                    FROM {table_name}
                  ;
                """

                df = db_conn.execute(query).df()
            else:
                # Non-spatial table
                df = db_conn.table(table_name).df()

            if df.empty:
                print(f"    - Skipping empty table: {table_name}")
                continue

            # Stringify complex columns
            for col in df.columns:
                if df[col].dtype == "object":
                    first_valid_index = df[col].first_valid_index()
                    if first_valid_index is not None and isinstance(
                        df[col].loc[first_valid_index], (list, dict)
                    ):
                        print(f"    - Stringifying complex column: {col}")
                        df[col] = df[col].apply(
                            lambda x: json.dumps(x) if x is not None else None
                        )

            # Create GeoDataFrame or regular DataFrame and write to file
            if "wkb_geometry" in df.columns:
                geometries = df["wkb_geometry"].apply(
                    lambda x: bytes(x) if isinstance(x, bytearray) else x
                )
                gdf = gpd.GeoDataFrame(
                    df.drop(columns=["wkb_geometry"]),
                    geometry=gpd.GeoSeries.from_wkb(geometries),
                    crs="EPSG:4326",
                )
                gdf.to_file(export_path, layer=table_name, driver="GPKG")
                print(f"    - Successfully exported spatial layer: {table_name}")
            else:
                # For non-spatial tables
                gpd.GeoDataFrame(df).to_file(
                    export_path, layer=table_name, driver="GPKG"
                )
                print(f"    - Successfully exported non-spatial layer: {table_name}")

        except Exception as e:
            print(f"    - Could not export table {table_name}: {e}")

    export_path.chmod(stat.S_IREAD | stat.S_IRGRP | stat.S_IROTH)

    print(f"Database successfully exported to GeoPackage: {export_path}")


def main(
    geoid: str,  #
    clean: Optional[bool] = False,
):
    """
    Main execution function to run the data fusion workflow.
    This function handles all file I/O and orchestrates the core logic.
    """
    db_path = SCRIPT_DIR / f"osm_x_usgs_x_naacc_{geoid}.duckdb"
    geoparquet_export_path = SCRIPT_DIR / f"osm_x_usgs_x_naacc_{geoid}_geoparquet"
    gpkg_export_path = SCRIPT_DIR / f"osm_x_usgs_x_naacc_{geoid}_database.gpkg"

    if db_path.exists():
        if clean:
            db_path.unlink()
        else:
            db_path.chmod(stat.S_IREAD | stat.S_IWRITE | stat.S_IRGRP | stat.S_IROTH)

    # --- Path Configuration ---
    base_data_dir = SCRIPT_DIR / "../../../data"

    # / f"processed/osm/nonservice-roadways-buffer-10mi-county-{geoid}_nonservice-roadways-buffer-50mi-state-36_us-250101.osm.pbf"
    osm_pbf_path = (
        base_data_dir
        / "processed/osm"
        / "all-ways-buffer-10mi-county-36001_buffer-50mi-state-36_all-nonparking-highways-us-250101.osm.pbf"
    )
    naacc_crossings_path = SCRIPT_DIR / "../data/raw/naacc-crossings-detailed.csv"
    road_flowline_intersections_path = (
        SCRIPT_DIR
        # / "../../e011_pjt_freight_transportation/nys_gpkgs/e010_roadspans_x_flowlines_with_risk.buffer-50mi-state-36.gpkg"
        / "../notebooks/undirected_all_nonparking_roadways_hydrological_hazard_flow.gpkg"
    )

    # --- Data Loading (I/O) ---
    print("--- Loading Initial Data from Files ---")
    if not osm_pbf_path.exists():
        raise FileNotFoundError(f"Required OSM PBF file not found at: {osm_pbf_path}")

    # --- Database and Workflow Execution ---
    db_conn = None
    try:
        db_conn = duckdb.connect(database=str(db_path), read_only=False)
        db_conn.execute("INSTALL spatial; LOAD spatial;")

        load_region_under_study_table(
            db_conn=db_conn,  #
            geoid=geoid,
        )

        load_osm_tables(
            db_conn=db_conn,  #
            osm_pbf_path=osm_pbf_path,
        )

        load_road_flowline_intersections_table(
            db_conn=db_conn,
            road_flowline_intersections_path=road_flowline_intersections_path,
        )

        load_naacc_crossings_tables(
            db_conn=db_conn,  #
            naacc_crossings_path=naacc_crossings_path,
        )

        match_crossings_to_intersections(db_conn)
        match_crossings_to_road_spans(db_conn)

        create_scored_candidate_matches_table(db_conn)
        create_final_matches_table(db_conn)
        create_final_crossing_points_table(db_conn)

        # Export the full database for observability
        export_database_to_geoparquet(db_conn, geoparquet_export_path)
        export_database_to_gpkg(db_conn, gpkg_export_path)

    finally:
        if db_conn:
            db_conn.close()
        # Set database to read-only after the connection is closed
        if db_path.exists():
            db_path.chmod(stat.S_IREAD | stat.S_IRGRP | stat.S_IROTH)
        print(f"\nWorkflow finished. Database '{db_path}' is now read-only.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fuse NAACC Stream Crossings with OSM/NHD intersection data."
    )
    parser.add_argument(
        "--geoid",
        required=True,
        help="The county GEOID to process (e.g., 36001 for Albany, NY).",
    )
    args = parser.parse_args()

    main(geoid=args.geoid)
