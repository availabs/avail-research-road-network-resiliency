import ast
import os
import pickle

import geopandas as gpd
import pandas as pd
import psycopg2.sql
import psycopg2
import osmnx as ox
from sqlalchemy import text as sql_text

from ..core.utils import create_base_paths_df, create_detour_paths_df, restore_entities_from_output_dir

def load_detours_analysis_into_postgis(
    db_conn,
    postgres_schema,
    roadways_gdf,
    detours_info_df,
    clean_schema=False,
    base_paths_df=None,
    detour_paths_df=None,
    single_transaction=True
):
    def format_with_schema(sql):
        return psycopg2.sql.SQL(sql).format(postgres_schema=psycopg2.sql.Identifier(postgres_schema))

    try:
        if single_transaction:
            db_conn.execute(
                sql_text(
                    'BEGIN ;'
                )
            )

        db_conn.exec_driver_sql(
            format_with_schema('CREATE EXTENSION IF NOT EXISTS POSTGIS ;')
        )

        if clean_schema:
            db_conn.exec_driver_sql(
                format_with_schema('DROP SCHEMA IF EXISTS {postgres_schema} CASCADE ;')
            )

        db_conn.exec_driver_sql(
            format_with_schema('CREATE SCHEMA IF NOT EXISTS {postgres_schema} ;')
        )

        roadways_gdf.reset_index(inplace=False).to_postgis(
            name='roadways',
            con=db_conn,
            schema=postgres_schema,
            index=False,
            if_exists='replace',
        )

        db_conn.exec_driver_sql(
            format_with_schema(
                '''
                    ALTER TABLE {postgres_schema}.roadways
                        ADD PRIMARY KEY (u, v, key)
                    ;

                    CLUSTER {postgres_schema}.roadways
                        USING roadways_pkey
                    ;
                '''
            )
        )

        detours_info_df.reset_index(inplace=False).to_sql(
            name='detour_info',
            con=db_conn,
            schema=postgres_schema,
            index=False,
            if_exists='replace'
        )

        db_conn.exec_driver_sql(
            format_with_schema(
                '''
                    ALTER TABLE {postgres_schema}.detour_info
                        ADD PRIMARY KEY (u, v, key)
                    ;

                    CLUSTER {postgres_schema}.detour_info
                        USING detour_info_pkey
                    ;
                '''
            )
        )

        base_paths_df = base_paths_df or create_base_paths_df(detours_info_df)

        base_paths_df.reset_index(inplace=False).to_sql(
            name='base_paths',
            con=db_conn,
            schema=postgres_schema,
            index=False,
            if_exists='replace'
        )

        db_conn.exec_driver_sql(
            format_with_schema(
                '''
                    ALTER TABLE {postgres_schema}.base_paths
                        ADD PRIMARY KEY (target_edge_u, target_edge_v, target_edge_key, path_edge_index)
                    ;

                    ALTER TABLE {postgres_schema}.base_paths
                        ADD CONSTRAINT base_paths_roadways_fkey
                        FOREIGN KEY (target_edge_u, target_edge_v, target_edge_key)
                        REFERENCES {postgres_schema}.roadways (u, v, key)
                        MATCH FULL
                    ;

                    CLUSTER {postgres_schema}.base_paths
                        USING base_paths_pkey
                    ;
                '''
            )
        )

        detour_paths_df = detour_paths_df or create_detour_paths_df(detours_info_df)

        detour_paths_df.reset_index(inplace=False).to_sql(
            name='detour_paths',
            con=db_conn,
            schema=postgres_schema,
            index=False,
            if_exists='replace'
        )

        db_conn.exec_driver_sql(
            format_with_schema(
                '''
                    ALTER TABLE {postgres_schema}.detour_paths
                        ADD PRIMARY KEY (target_edge_u, target_edge_v, target_edge_key, path_edge_index)
                    ;

                    ALTER TABLE {postgres_schema}.detour_paths
                        ADD CONSTRAINT detour_paths_roadways_fkey
                        FOREIGN KEY (target_edge_u, target_edge_v, target_edge_key)
                        REFERENCES {postgres_schema}.roadways (u, v, key)
                        MATCH FULL
                    ;

                    CLUSTER {postgres_schema}.detour_paths
                        USING detour_paths_pkey
                    ;
                '''
            )
        )

        db_conn.exec_driver_sql(
            format_with_schema(
                '''
                    CREATE OR REPLACE VIEW {postgres_schema}.detour_info_geoms
                    AS
                        SELECT
                            -- Because QGIS throws an error if the layer does not have a Primary Key column.
                            (
                                '('       ||
                                u::TEXT   ||
                                ', '      ||
                                v::TEXT   ||
                                ', '      ||
                                key::TEXT ||
                                ')'
                            ) AS edge_id,

                            b.*,
                            a.roadclass,
                            a.speed_kph,
                            a.travel_time,
                            a.road_name,
                            a.from_name,
                            a.to_name,

                            -- To make it easier to copy/paste coords into Google Maps
                            (
                                '('                 ||
                                b.origin_lat::TEXT  ||
                                ', '                ||
                                b.origin_lon::TEXT  ||
                                ')'
                            ) AS origin_lat_lon,

                            (
                                '('                      ||
                                b.destination_lat::TEXT  ||
                                ', '                     ||
                                b.destination_lon::TEXT  ||
                                ')'
                            ) AS destination_lat_lon,

                            a.geometry
                        FROM {postgres_schema}.roadways AS a
                            INNER JOIN {postgres_schema}.detour_info AS b
                                USING (u, v, key)
                ;'''
            )
        )

        db_conn.exec_driver_sql(
            format_with_schema(
                '''
                    CREATE OR REPLACE VIEW {postgres_schema}.base_path_geoms
                    AS
                        SELECT
                            -- Because QGIS throws an error if the layer does not have a Primary Key column.
                            (
                                '('                     ||
                                b.target_edge_u::TEXT   ||
                                ', '                    ||
                                b.target_edge_v::TEXT   ||
                                ', '                    ||
                                b.target_edge_key::TEXT ||
                                ', '                    ||
                                b.path_edge_index::TEXT ||
                                ')'
                            ) AS edge_id,

                            a.u,
                            a.v,
                            a.key,

                            b.target_edge_u,
                            b.target_edge_v,
                            b.target_edge_key,
                            b.path_edge_index,

                            a.geometry
                        FROM {postgres_schema}.roadways AS a
                            INNER JOIN {postgres_schema}.base_paths AS b
                                ON (
                                ( a.u = b.path_edge_u )
                                AND
                                ( a.v = b.path_edge_v )
                                AND
                                ( a.key = b.path_edge_key )
                                )
                ;'''
            )
        )

        db_conn.exec_driver_sql(
            format_with_schema(
                '''
                    CREATE OR REPLACE VIEW {postgres_schema}.detour_path_geoms
                    AS
                        SELECT
                            -- Because QGIS throws an error if the layer does not have a Primary Key column.
                            (
                                '('                     ||
                                b.target_edge_u::TEXT   ||
                                ', '                    ||
                                b.target_edge_v::TEXT   ||
                                ', '                    ||
                                b.target_edge_key::TEXT ||
                                ', '                    ||
                                b.path_edge_index::TEXT ||
                                ')'
                            ) AS edge_id,

                            a.u,
                            a.v,
                            a.key,

                            b.target_edge_u,
                            b.target_edge_v,
                            b.target_edge_key,
                            b.path_edge_index,

                            a.geometry
                        FROM {postgres_schema}.roadways AS a
                            INNER JOIN {postgres_schema}.detour_paths AS b
                                ON (
                                ( a.u = b.path_edge_u )
                                AND
                                ( a.v = b.path_edge_v )
                                AND
                                ( a.key = b.path_edge_key )
                                )
                ;'''
            )
        )

        db_conn.exec_driver_sql(
            format_with_schema(
                '''
                    CREATE OR REPLACE VIEW {postgres_schema}.detour_path_origin_geoms
                    AS
                        SELECT
                            -- Because QGIS throws an error if the layer does not have a Primary Key column.
                            (
                                '('         ||
                                a.u::TEXT   ||
                                ', '        ||
                                a.v::TEXT   ||
                                ', '        ||
                                a.key::TEXT ||
                                ')'
                            ) AS edge_id,

                            a.u,
                            a.v,
                            a.key,

                            a.origin_node_id AS node_id,

                            -- To make it easier to copy/paste into Google Maps
                            (
                                '('                 ||
                                a.origin_lat::TEXT  ||
                                ', '                ||
                                a.origin_lon::TEXT  ||
                                ')'
                            ) AS lat_lon,

                            ST_SetSRID(
                                ST_MakePoint(a.origin_lon, a.origin_lat),
                                4326
                            ) AS geometry
                        FROM {postgres_schema}.detour_info AS a
                ;'''
            )
        )

        db_conn.exec_driver_sql(
            format_with_schema(
                '''
                    CREATE OR REPLACE VIEW {postgres_schema}.detour_path_destination_geoms
                    AS
                        SELECT
                            -- Because QGIS throws an error if the layer does not have a Primary Key column.
                            (
                                '('         ||
                                a.u::TEXT   ||
                                ', '        ||
                                a.v::TEXT   ||
                                ', '        ||
                                a.key::TEXT ||
                                ')'
                            ) AS edge_id,

                            a.u,
                            a.v,
                            a.key,

                            a.destination_node_id AS node_id,

                            -- To make it easier to copy/paste into Google Maps
                            (
                                '('                 ||
                                a.destination_lat::TEXT  ||
                                ', '                ||
                                a.destination_lon::TEXT  ||
                                ')'
                            ) AS lat_lon,

                            ST_SetSRID(
                                ST_MakePoint(a.destination_lon, a.destination_lat),
                                4326
                            ) AS geometry
                        FROM {postgres_schema}.detour_info AS a
                ;'''
            )
        )

        if single_transaction:
            db_conn.execute(
                sql_text(
                    'COMMIT ;'
                )
            )
    
    except Exception as e:
        if single_transaction:
            db_conn.execute(
                sql_text(
                    'ROLLBACK ;'
                )
            )

        raise e

def load_analysis_dir_into_postgres(
    db_conn,
    postgres_schema,
    output_dir,
    clean_schema=False,
    single_transaction=True
):
    entities = restore_entities_from_output_dir(output_dir)

    return load_detours_analysis_into_postgis(
        db_conn,
        postgres_schema,
        roadways_gdf=entities['edges_gdf'],
        detours_info_df=entities['detours_info_df'],
        clean_schema=clean_schema,
        single_transaction=single_transaction,
    )