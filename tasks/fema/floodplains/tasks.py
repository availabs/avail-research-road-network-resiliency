from os import PathLike

import geopandas as gpd
from prefect import task

from common.fema.floodplains.core import get_clipped_floodplain_data


@task
def get_clipped_floodplain_data_task(
    floodplains_gpkg_path: PathLike,
    buffered_region_gdf: gpd.GeoDataFrame,
):
    return get_clipped_floodplain_data(
        floodplains_gpkg_path=floodplains_gpkg_path,
        buffered_region_gdf=buffered_region_gdf,
    )
