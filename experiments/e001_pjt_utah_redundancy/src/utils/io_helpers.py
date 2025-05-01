import os
import stat
from os import PathLike
from pathlib import Path

import geopandas as gpd
import pandas as pd

from ..core.utils import create_base_paths_df, create_detour_paths_df


def create_detours_gpkg(
    filename: PathLike,
    roadways_gdf: gpd.GeoDataFrame,
    detours_info_df: pd.DataFrame,
    base_paths_df: pd.DataFrame | None = None,
    detour_paths_df: pd.DataFrame | None = None,
) -> PathLike:
    if not filename:
        raise ValueError("filename is required")

    # Ensure the directory path to filename exists.
    fpath = Path(filename)
    fpath.parent.mkdir(parents=True, exist_ok=True)

    try:
        os.remove(filename)
    except OSError:
        pass

    roadways_gdf.to_file(
        filename=filename,  #
        layer="roadways",
        driver="GPKG",
        engine="pyogrio",
    )

    # Create an attributes-only (aspatial) table. No geometry. Just the detours data to join with the edges_gdf layer.
    gpd.GeoDataFrame(detours_info_df).to_file(
        filename=filename,  #
        layer="detours_info",
        driver="GPKG",
        engine="pyogrio",
    )

    base_paths_df = (
        base_paths_df
        if base_paths_df is not None
        else create_base_paths_df(detours_info_df)
    )

    gpd.GeoDataFrame(base_paths_df).to_file(
        filename=filename,  #
        layer="base_paths",
        driver="GPKG",
        engine="pyogrio",
    )

    detour_paths_df = (
        detour_paths_df
        if detour_paths_df is not None
        else create_detour_paths_df(detours_info_df)
    )

    gpd.GeoDataFrame(detour_paths_df).to_file(
        filename=filename,  #
        layer="detour_paths",
        driver="GPKG",
        engine="pyogrio",
    )

    read_only_perms = stat.S_IREAD | stat.S_IRGRP | stat.S_IROTH
    os.chmod(fpath, read_only_perms)

    return filename


def _get_paths_gpd_from_gpkg(filename, path_type):
    if path_type not in ["base", "detour"]:
        raise BaseException('path_type must be either "base" or "detour"')

    sql = f"""
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
    """

    return gpd.read_file(
        filename=filename,
        sql=sql,
        sql_dialect="SQLITE",
        driver="GPKG",
        engine="pyogrio",
    )


def get_base_paths_gpd_from_gpkg(filename):
    return _get_paths_gpd_from_gpkg(filename=filename, path_type="base")


def get_detour_paths_gpd_from_gpkg(filename):
    return _get_paths_gpd_from_gpkg(filename=filename, path_type="detour")
