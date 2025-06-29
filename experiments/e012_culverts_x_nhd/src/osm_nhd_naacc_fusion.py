import argparse
import json
import stat
from pathlib import Path
from typing import Optional, cast

import duckdb
import duckdb.typing as duckdb_types
import geopandas as gpd
import pandas as pd
from pandera.typing import DataFrame
from prefect import task

from common.osm.enrich import CombinedRoadSpansSchema, convert_graph_to_gdfs
from common.osm.schemas import EnrichedOsmNetworkDataWithFullMetadata
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
CROSSING_TYPE_MATCH_MULTIPLIER = 0.8
# When types mismatch
CROSSING_TYPE_MISMATCH_MULTIPLIER = 1.25


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


def gpd_geom_to_wkb(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    wkb_gdf = gdf.drop(columns=gdf.geometry.name)

    wkb_gdf["wkb_geometry"] = gdf.geometry.to_wkb()

    wkb_gdf = wkb_gdf.sort_index().reset_index()

    wkb_gdf["_id_"] = wkb_gdf.index

    return wkb_gdf


def register_and_filter_data(
    db_conn: duckdb.DuckDBPyConnection,
    naacc_gdf: gpd.GeoDataFrame,
    road_intersections_gdf: gpd.GeoDataFrame,
    road_spans_gdf: DataFrame[CombinedRoadSpansSchema],
    filter_gdf: gpd.GeoDataFrame,
) -> None:
    """
    Registers GeoDataFrames with DuckDB and filters them using a spatial boundary.
    """
    max_len = max(
        len(naacc_gdf),
        len(road_intersections_gdf),
        len(road_spans_gdf),
        len(filter_gdf),
    )

    db_conn.execute(f"SET pandas_analyze_sample = {max_len}")

    # --- Register DataFrames and Filter View with DuckDB ---
    db_conn.register("naacc_initial_gdf", gpd_geom_to_wkb(naacc_gdf))

    db_conn.register(
        "road_intersections_initial_gdf", gpd_geom_to_wkb(road_intersections_gdf)
    )
    db_conn.register("road_spans_initial_gdf", gpd_geom_to_wkb(road_spans_gdf))
    db_conn.register("filter_gdf", gpd_geom_to_wkb(filter_gdf))

    db_conn.execute("""
        CREATE TABLE region_under_study AS
          SELECT
              ST_Union_Agg(
                ST_GeomFromWKB(wkb_geometry)
              ) AS geom
            FROM
              filter_gdf
        ;
    """)

    db_conn.unregister(view_name="filter_gdf")

    # --- Filter data within DuckDB ---
    print("\n--- Filtering Source Data to Boundary ---")
    db_conn.execute("""
        CREATE TABLE naacc_crossings AS
          SELECT DISTINCT ON (Crossing_Code)
              a.* EXCLUDE (_id_, wkb_geometry),
              -- (
              --   ( UPPER(Road_Type) NOT IN ('RAILROAD', 'TRAIL') )
              --   AND
              --   ( LOWER("Road") NOT IN ('NO ROAD OR TRAIL PRESENT', 'UNNAMED') )
              -- ) AS _is_roadway_crossing_,
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

    db_conn.execute("""
      CREATE TABLE road_intersections AS
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

    db_conn.execute("""
      CREATE TABLE road_spans AS
        SELECT
            a.* EXCLUDE(_id_, wkb_geometry),
            ST_GeomFromWKB(a.wkb_geometry) AS geom
          FROM
              road_spans_initial_gdf AS a
            -- LEFT OUTER JOIN
            --   road_spans_initial_gdf AS b
            --     ON (
            --       ( (a.u, a.v, a.key) = (b.v, b.u, b.key) )
            --       AND
            --       ( a._id_ > b._id_ )
            --     )
            INNER JOIN
              region_under_study AS c
                ON ST_Intersects(ST_GeomFromWKB(a.wkb_geometry), c.geom)
            -- WHERE (b._id_ IS NULL)
    """)

    db_conn.unregister(view_name="road_spans_initial_gdf")

    naacc_count = db_conn.execute("SELECT COUNT(*) FROM naacc_crossings;").fetchone()[0]  # type: ignore
    road_count = db_conn.execute("SELECT COUNT(*) FROM road_intersections;").fetchone()[  # type: ignore
        0
    ]
    print(f"Found {naacc_count} NAACC records within boundary.")
    print(f"Found {road_count} road/flowline intersection records within boundary.")


def match_crossings_to_intersections(db_conn: duckdb.DuckDBPyConnection) -> None:
    """
    Performs Stage 1: Joins NAACC crossings to road/flowline intersections.
    """
    print("\n--- Stage 1: Matching NAACC Crossings to Road/Flowline Intersections ---")
    db_conn.execute(f"""
        CREATE TABLE stage1_matched AS
          WITH ranked_matches AS (
              SELECT
                  roads_x_flowlines.u   AS road_u,
                  roads_x_flowlines.v   AS road_v,
                  roads_x_flowlines.key AS road_key,

                  roads_x_flowlines._road_span_type_ AS road_span_type,
                  roads_x_flowlines.span_idx AS road_span_idx,

                  roads_x_flowlines.permanent_identifier AS nhd_flowline_permanent_identifier,
                  roads_x_flowlines.intxn_idx AS road_span_nhd_flowline_intxn_idx,

                  naacc.Crossing_Code,
                  naacc.Crossing_Type,

                  ST_Distance(
                    ST_Transform(
                      naacc.geom,
                      'EPSG:4326',
                      '{TARGET_CRS}'
                    ),
                    ST_Transform(
                      roads_x_flowlines.geom,
                      'EPSG:4326',
                      '{TARGET_CRS}'
                    )
                  ) as distance_m,

                  naacc.geom as naacc_geom,
                  roads_x_flowlines.geom as road_geom,

                  ROW_NUMBER() OVER(
                      PARTITION BY
                          naacc.Crossing_Code
                      ORDER BY
                          distance_m
                  ) as match_rank

                FROM
                    naacc_crossings AS naacc
                  INNER JOIN
                    road_intersections AS roads_x_flowlines
                      ON (
                        ST_DWithin(
                          ST_Transform(
                            naacc.geom,
                            'EPSG:4326',
                            '{TARGET_CRS}'
                          ),
                          ST_Transform(
                            roads_x_flowlines.geom,
                            'EPSG:4326',
                            '{TARGET_CRS}'
                          ),
                          {BUFFER_SIZE_METERS}
                        )
                    )
              -- WHERE (naacc._is_roadway_crossing_)
        )
        SELECT
            road_u,
            road_v,
            road_key,
            road_span_type,
            road_span_idx,
            nhd_flowline_permanent_identifier,
            road_span_nhd_flowline_intxn_idx,

            Crossing_Code,
            Crossing_Type,

            distance_m,

            ST_MakeLine(
              naacc_geom,
              road_geom
            ) AS geom,

            'road_flowline_intersection_match' as match_type
          FROM
            ranked_matches
          WHERE
            match_rank <= 3
        ;
    """)
    stage1_count = db_conn.execute("SELECT COUNT(*) FROM stage1_matched;").fetchone()[0]  # type: ignore
    print(f"Found {stage1_count} potential high-confidence matches in Stage 1.")


def match_crossings_to_road_spans(db_conn: duckdb.DuckDBPyConnection) -> None:
    """
    Performs Stage 2: Finds all potential road span matches within the buffer
    for ALL NAACC crossings. The decision is deferred to the final combination step.
    """
    print("\n--- Stage 2: Finding All Potential Road Span Matches ---")
    db_conn.execute(f"""
        CREATE TABLE stage2_matched AS
        SELECT
            *,
            ROW_NUMBER() OVER(
                PARTITION BY
                    Crossing_Code
                ORDER BY
                    distance_m
            ) as match_rank
          FROM (
            SELECT
                s.u   AS road_u,
                s.v   AS road_v,
                s.key AS road_key,

                s._road_span_type_ AS road_span_type,
                s.span_idx AS road_span_idx,

                c.Crossing_Code,
                c.Crossing_Type,

                ST_Distance(
                    ST_Transform(
                      c.geom,
                      'EPSG:4326',
                      '{TARGET_CRS}'
                    ),
                    ST_Transform(
                      s.geom,
                      'EPSG:4326',
                      '{TARGET_CRS}'
                    )
                ) AS distance_m,

                ST_ShortestLine(
                  c.geom,
                  s.geom
                ) AS geom,

                'shortest_line_to_road_span' AS match_type,
              FROM
                naacc_crossings AS c
                  INNER JOIN road_spans AS s
                    ON ST_DWithin(
                        ST_Transform(
                          c.geom,
                          'EPSG:4326',
                          '{TARGET_CRS}'
                        ),
                        ST_Transform(
                          s.geom,
                          'EPSG:4326',
                          '{TARGET_CRS}'
                        ),
                        {BUFFER_SIZE_METERS}
                    )
              -- WHERE (c._is_roadway_crossing_)
          )
        ;
    """)
    stage2_count = db_conn.execute("SELECT COUNT(*) FROM stage2_matched;").fetchone()[0]  # type: ignore
    print(f"Found {stage2_count} potential road span matches in Stage 2.")


def calculate_match_score(
    match_type: str,
    crossing_type: Optional[str],
    road_span_type: Optional[str],
    distance_m: float,
) -> float:
    """
    Calculates a preference score for a match. Lower is better.
    This function is registered as a UDF in DuckDB.
    The score starts with distance and is modified by multipliers to favor
    more confident matches.
    """

    match_score = distance_m

    if match_type == "road_flowline_intersection_match":
        match_score *= MATCH_TYPE_PREFERENCE_MULTIPLIER
    else:
        match_score *= MATCH_TYPE_PENALTY_MULTIPLIER

    # Handle cases where crossing_type might be None from the database.
    crossing_class = "NONBRIDGE"

    if crossing_type and "BRIDGE" in crossing_type.upper():
        crossing_class = "BRIDGE"

    if crossing_class == road_span_type:
        match_score *= CROSSING_TYPE_MATCH_MULTIPLIER
    else:
        match_score *= CROSSING_TYPE_MISMATCH_MULTIPLIER

    return match_score


def create_scored_candidate_matches_table(
    db_conn: duckdb.DuckDBPyConnection,
) -> None:
    """
    Combines results from Stage 1 and 2, chooses the best match for each
    crossing using a UDF for scoring, and fetches final GDFs from DuckDB.
    """
    print("\n--- Combining, Prioritizing, and Fetching All Results ---")

    # Create a Python UDF to handle the complex scoring logic
    db_conn.create_function(
        "calculate_match_score",
        calculate_match_score,  # type: ignore
        [
            duckdb_types.VARCHAR,
            duckdb_types.VARCHAR,
            duckdb_types.VARCHAR,
            duckdb_types.DOUBLE,
        ],
        duckdb_types.DOUBLE,
    )

    db_conn.execute("""
      CREATE SEQUENCE scored_candidate_matches_id_seq START 1;

      CREATE TABLE scored_candidate_matches  AS
        SELECT
            nextval('scored_candidate_matches_id_seq') AS _id_,

            t.*,

            ROW_NUMBER() OVER(
                PARTITION BY
                    t.Crossing_Code
                ORDER BY t.match_score
            ) AS rank_across_match_types_for_crossing

          FROM (
            SELECT
                *,

                ROW_NUMBER() OVER(
                    PARTITION BY
                        Crossing_Code
                    ORDER BY match_score
                ) AS rank_within_match_type_for_crossing,

                ROW_NUMBER() OVER(
                    PARTITION BY
                        road_u,
                        road_v,
                        road_key,
                        road_span_type,
                        road_span_idx,

                        nhd_flowline_permanent_identifier,
                        road_span_nhd_flowline_intxn_idx
                    ORDER BY match_score
                ) AS rank_within_match_type_for_road_flowline_intxn

              FROM (
                SELECT
                    -- START: Primary Key
                    road_u,
                    road_v,
                    road_key,
                    road_span_type,
                    road_span_idx,

                    nhd_flowline_permanent_identifier,
                    road_span_nhd_flowline_intxn_idx,

                    Crossing_Code,
                    -- END: Primary Key

                    Crossing_Type,

                    distance_m,
                    match_type,

                    calculate_match_score(
                        match_type,
                        Crossing_Type,
                        road_span_type,
                        distance_m
                    ) as match_score,

                    geom
                FROM
                    stage1_matched
              )

            UNION ALL

            SELECT
                *,

                ROW_NUMBER() OVER(
                    PARTITION BY
                        Crossing_Code
                    ORDER BY match_score
                ) AS rank_within_match_type_for_crossing,

                NULL AS rank_within_match_type_for_road_flowline_intxn -- Does not apply.
              FROM (
                SELECT
                    -- START: Primary Key
                    road_u,
                    road_v,
                    road_key,
                    road_span_type,
                    road_span_idx,

                    NULL AS nhd_flowline_permanent_identifier,
                    NULL AS road_span_nhd_flowline_intxn_idx,

                    Crossing_Code,
                    -- END: Primary Key

                    Crossing_Type,

                    distance_m,
                    match_type,

                    calculate_match_score(
                        match_type,
                        Crossing_Type,
                        road_span_type,
                        distance_m
                    ) as match_score,

                    geom
                  FROM
                    stage2_matched
            )
          ) AS t

          ORDER BY
            rank_within_match_type_for_road_flowline_intxn DESC NULLS LAST,
            match_score
      ;
    """)


def create_final_matches_table(
    db_conn: duckdb.DuckDBPyConnection,
) -> None:
    # FIXME: Change this so that it's all Crossings for which only one R/F Intxn has it ranked #1.
    # No disputes amongst Road/Flowline intersections for nearby NAACC Crossings
    db_conn.execute("""
      CREATE TABLE final_matches AS
        SELECT
            a.*,
            'All ideal matches, no arguments between nearby Road/Flowline intersections' AS match_reason
          FROM scored_candidate_matches AS a
          WHERE (
            ( rank_within_match_type_for_crossing = 1 )
            AND
            ( rank_within_match_type_for_road_flowline_intxn = 1 )
            AND
            ( distance_m < 20 )
          )
      ;

      ALTER TABLE final_matches ADD PRIMARY KEY (Crossing_Code) ;
    """)

    db_conn.execute("""
        INSERT INTO final_matches
          SELECT DISTINCT ON (Crossing_Code)
              a.*,
            'Min match rank across all types for crossing' AS match_reason
            FROM scored_candidate_matches AS a
              LEFT OUTER JOIN final_matches AS b
                USING (Crossing_Code)
            WHERE ( b.Crossing_Code IS NULL )
            ORDER BY b.Crossing_Code, b.rank_across_match_types_for_crossing
      ;
    """)

    final_count = db_conn.execute("SELECT COUNT(*) FROM final_matches").fetchone()[0]  # type: ignore
    print(f"Created final set of {final_count} prioritized matches.")


def create_final_crossing_points_table(
    db_conn: duckdb.DuckDBPyConnection,
) -> None:
    """
    Creates a table of the final matched crossing points on the road network.
    The geometry is the endpoint of the snap line from the final_matches table.
    """
    print("\n--- Creating Final Crossing Points Table ---")

    db_conn.execute("""
        CREATE TABLE naacc_crossings_x_roadways AS
        SELECT
            * EXCLUDE(geom),
            ST_EndPoint(geom) AS geom
        FROM
            final_matches;
    """)
    final_points_count = db_conn.execute(
        "SELECT COUNT(*) FROM naacc_crossings_x_roadways"
    ).fetchone()[0]  # type: ignore
    print(
        f"Created final table 'naacc_crossings_x_roadways' with {final_points_count} points."
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
    db_conn: duckdb.DuckDBPyConnection, export_path: Path
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


def main(geoid: str):
    """
    Main execution function to run the data fusion workflow.
    This function handles all file I/O and orchestrates the core logic.
    """

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

    db_path = SCRIPT_DIR / f"osm_x_usgs_x_naacc_{geoid}.duckdb"
    geoparquet_export_path = SCRIPT_DIR / f"osm_x_usgs_x_naacc_{geoid}_geoparquet"
    gpkg_export_path = SCRIPT_DIR / f"osm_x_usgs_x_naacc_{geoid}_database.gpkg"

    # --- Data Loading (I/O) ---
    print("--- Loading Initial Data from Files ---")
    if not osm_pbf_path.exists():
        raise FileNotFoundError(f"Required OSM PBF file not found at: {osm_pbf_path}")

    naacc_df = pd.read_csv(naacc_crossings_path, encoding="cp1252")
    naacc_gdf = gpd.GeoDataFrame(
        naacc_df,
        geometry=gpd.points_from_xy(
            naacc_df.GPS_X_Coordinate, naacc_df.GPS_Y_Coordinate
        ),
        crs="EPSG:4326",
    )
    road_intersections_gdf = gpd.read_file(
        road_flowline_intersections_path,  #
        layer="roadspans_x_flowlines_with_risk",
    )

    enriched_osm = get_network_type_all_enriched_osm_task(
        osm_pbf=osm_pbf_path,
    )

    undirected_enriched_osm = get_undirected_enriched_osm_task(
        enriched_osm=enriched_osm
    )

    undirected_all_edges_gdf = undirected_enriched_osm["edges_gdf"]

    road_spans_gdf = create_combined_road_spans_task(edges_gdf=undirected_all_edges_gdf)

    filter_gdf = get_region_boundary_gdf(geoid=geoid)
    if filter_gdf.crs != "EPSG:4326":
        filter_gdf = filter_gdf.to_crs("EPSG:4326")

    db_path.unlink(missing_ok=True)

    # --- Database and Workflow Execution ---
    db_conn = None
    try:
        db_conn = duckdb.connect(database=str(db_path), read_only=False)
        db_conn.execute("INSTALL spatial; LOAD spatial;")

        register_and_filter_data(
            db_conn,
            naacc_gdf,  #
            road_intersections_gdf,
            road_spans_gdf,
            filter_gdf,
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


"""
Now I need a methodology document.



The document audience is transportation and hazard mitigation planners and researchers, so the technical details of SQL and DuckDB will be a distraction.



What is critical is very thorough explication of the input datasets, their utility for planning, and the value added by fusion them into a single dataset.



The high level, intuitive, explanation of the methodology is important.



Also, please include a data dictionary of the
"""
