import argparse
import os
import pickle
import re
import shutil
import sys
import time

this_dir = os.path.dirname(
    os.path.abspath(__file__)
)

local_modules_path = os.path.join(this_dir, '..')

if local_modules_path not in sys.path:
    sys.path.append(local_modules_path)

from src.core.redundancy_metric import compute_redundancy_for_edges

from src.utils.osm_helpers import create_enriched_osmnx_graph
from src.utils.visualization import render_side_by_side_detour_map_for_edge
from src.utils.io_helpers import create_detours_gpkg


def get_region_geoid_from_osm_pbf(osm_pbf):
    # Take the first part of the PBF file name that includes a string of at least 5 consecutive digits.
    # roadways_prefix_regex = re.compile(r'^vehicle-nonservice-roadways')
    geoid_regex = re.compile(r'[0-9]{5,}')

    region_id = [
        p for p in os.path.basename(osm_pbf).split('_')
        if re.search(geoid_regex, p)
    ][0]

    region_geoid = re.search(geoid_regex, region_id).group(0)

    return region_geoid


def get_convention_paths(region_geoid):
    out_dir = os.path.join(this_dir, '../redundancy_analysis_output', region_geoid)

    osm_graph_output_file = os.path.join(out_dir, 'osmnx_graph.pkl')
    road_network_image_file = os.path.join(out_dir, 'road_network.png')

    detour_maps_dir = os.path.join(out_dir, 'detour_maps')

    output_gpkg_path = os.path.join(out_dir, 'redundancy-analysis.gpkg')

    return dict(
        out_dir=out_dir,
        osm_graph_output_file=osm_graph_output_file,
        road_network_image_file=road_network_image_file,
        detour_maps_dir=detour_maps_dir,
        output_gpkg_path=output_gpkg_path
    )


def prepare_output_dir(dir_paths):
    out_dir = dir_paths['out_dir']
    detour_maps_dir = dir_paths['detour_maps_dir']

    shutil.rmtree(out_dir, ignore_errors=True)

    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(detour_maps_dir, exist_ok=True)

def create_top_n_detour_maps(edges_gdf, detours_info_df, detour_maps_dir, n=5):
    for idx, edge_id in enumerate(detours_info_df.nlargest(n, 'difference_sec').index):
        render_side_by_side_detour_map_for_edge(
            out_fpath=os.path.join(
                detour_maps_dir,
                f'detour-rank-{idx + 1}.edge_id-{edge_id[0]}_{edge_id[1]}_{edge_id[2]}.png'
            ),
            roadways_gdf=edges_gdf,
            detour_info_df=detours_info_df,
            edge_id=edge_id
        )

        time.sleep(3)


def main(osm_pbf):
    region_geoid = get_region_geoid_from_osm_pbf(osm_pbf=osm_pbf)

    dir_paths = get_convention_paths(region_geoid=region_geoid)

    prepare_output_dir(dir_paths=dir_paths)

    ## Create OSMnx Graph
    enriched_osmnx_graph = create_enriched_osmnx_graph(osm_pbf=osm_pbf)

    g = enriched_osmnx_graph['g']
    edges_gdf = enriched_osmnx_graph['edges_gdf']

    with open(dir_paths['osm_graph_output_file'], 'wb') as pfile:
        pickle.dump(g, pfile, protocol=pickle.HIGHEST_PROTOCOL)

    major_roadways_edge_ids = edges_gdf[edges_gdf['edge_min_roadclass'].between(0, 4, inclusive='both')].index

    detours_info_df = compute_redundancy_for_edges(g=g, edge_ids=major_roadways_edge_ids)

    create_top_n_detour_maps(
        edges_gdf=edges_gdf,
        detours_info_df=detours_info_df,
        detour_maps_dir=dir_paths['detour_maps_dir']
    )

    create_detours_gpkg(
        filename=dir_paths['output_gpkg_path'],
        roadways_gdf=edges_gdf,
        detours_info_df=detours_info_df
    )

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Compute Redundancy using Utah Methodology.')
    parser.add_argument(
        '-f',
        '--filename',
        type=str,
        help='Path to the OSM PBF file.',
        required=True
    )

    args = parser.parse_args()

    osm_pbf = args.filename

    main(osm_pbf=osm_pbf)