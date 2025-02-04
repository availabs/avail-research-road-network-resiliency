import pandas as pd
import geopandas as gpd

def _create_path_type_dataframe(detours_info_df, path_type):
    paths_data = []

    for target_edge_id, path_edge_ids in detours_info_df[path_type + '_path_edge_ids'].items():
        if path_edge_ids:
            for path_edge_index, path_edge_id in enumerate(path_edge_ids):
                paths_data.append({
                    'target_edge_u': target_edge_id[0],
                    'target_edge_v': target_edge_id[1],
                    'target_edge_key': target_edge_id[2],

                    'path_edge_index': path_edge_index,

                    'path_edge_u': path_edge_id[0],
                    'path_edge_v': path_edge_id[1],
                    'path_edge_key': path_edge_id[2],
                })
            
    # Create an attributes-only (aspatial) table. No geometry. Just the target and path edge IDs.
    return pd.DataFrame(paths_data)

def create_base_paths_df(detours_info_df):
    return _create_path_type_dataframe(detours_info_df, 'base')

def create_detour_paths_df(detours_info_df):
    return _create_path_type_dataframe(detours_info_df, 'detour')

def extract_detour_geodataframes_for_edge_id(
    roadways_gdf,
    detour_info_df,
    edge_id,
):
    edge_detour_info = detour_info_df.loc[edge_id]
    edge_gdf = roadways_gdf.loc[[edge_id]]

    origin_point_gdf = gpd.GeoDataFrame(
        geometry=gpd.points_from_xy(
            [edge_detour_info['origin_lon']],
            [edge_detour_info['origin_lat']],
        )
    )

    destination_point_gdf = gpd.GeoDataFrame(
        geometry=gpd.points_from_xy(
            [edge_detour_info['destination_lon']],
            [edge_detour_info['destination_lat']],
        )
    )
    
    base_path_edges_gdf = roadways_gdf.loc[edge_detour_info['base_path_edge_ids']]

    detour_path_edge_ids = edge_detour_info['detour_path_edge_ids']
    detour_path_edges_gdf = detour_path_edge_ids and roadways_gdf.loc[detour_path_edge_ids]

    return dict(
        edge_detour_info=edge_detour_info,
        edge_gdf=edge_gdf,
        origin_point_gdf=origin_point_gdf,
        destination_point_gdf=destination_point_gdf,
        base_path_edges_gdf=base_path_edges_gdf,
        detour_path_edges_gdf=detour_path_edges_gdf,
    )
