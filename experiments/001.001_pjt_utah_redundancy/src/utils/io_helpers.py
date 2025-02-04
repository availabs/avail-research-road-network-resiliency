import os

import geopandas as gpd

from ..core.utils import create_base_paths_df, create_detour_paths_df

# TODO: Implement the reverse process: create (Geo)DataFrames from the GeoPackage.
#       NOTE: Will require converting TEXT columns to lists and tuples.
#       NOTE: Testing will be essential.
#
#           import ast
# 
#           my_string = "[(1, 2), 'hello']"
#           d = ast.literal_eval(my_string)
#
#           print(d)  # Output: [(1, 2), 'hello']

def create_detours_gpkg(
    filename,
    roadways_gdf,
    detours_info_df,
    base_paths_df=None,
    detour_paths_df=None,
):
    if not filename:
        raise BaseException('filename is required')

    try:
        os.remove(filename)
    except OSError:
        pass

    roadways_gdf.to_file(
        filename=filename,
        layer='roadways',
        driver='GPKG',
        engine='pyogrio'
    )

    # Create an attributes-only (aspatial) table. No geometry. Just the detours data to join with the edges_gdf layer.
    gpd.GeoDataFrame(detours_info_df).to_file(
        filename=filename,
        layer='detours_info',
        driver='GPKG',
        engine='pyogrio'
    )

    base_paths_df = base_paths_df if base_paths_df is not None else create_base_paths_df(detours_info_df)

    gpd.GeoDataFrame(base_paths_df).to_file(
        filename=filename,
        layer='base_paths',
        driver='GPKG',
        engine='pyogrio'
    )

    detour_paths_df = detour_paths_df if detour_paths_df is not None else create_detour_paths_df(detours_info_df)

    gpd.GeoDataFrame(detour_paths_df).to_file(
        filename=filename,
        layer='detour_paths',
        driver='GPKG',
        engine='pyogrio'
    )

def _get_paths_gpd_from_gpkg(filename, path_type):
    if path_type not in ['base', 'detour']:
        raise BaseException('path_type must be either "base" or "detour"')

    sql = f'''
        SELECT
            a.u,
            a.v,
            a.key,

            b.target_edge_u,
            b.target_edge_v,
            b.target_edge_key,
            b.path_edge_index,

            a.geom
        FROM roadways AS a
            INNER JOIN {path_type}_paths AS b
            ON (
                ( a.u = b.path_edge_u )
                AND
                ( a.v = b.path_edge_v )
                AND
                ( a.key = b.path_edge_key )
            )
    '''

    return gpd.read_file(
        filename=filename,
        sql=sql,
        sql_dialect='SQLITE',
        driver='GPKG',
        engine='pyogrio'
    )

def get_base_paths_gpd_from_gpkg(filename):
    return _get_paths_gpd_from_gpkg(filename=filename, path_type='base')

def get_detour_paths_gpd_from_gpkg(filename):
    return _get_paths_gpd_from_gpkg(filename=filename, path_type='detour')