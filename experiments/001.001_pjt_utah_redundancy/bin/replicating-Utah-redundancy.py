import os
import sys
import re
import shutil
import pickle
import time
import argparse

import dotenv
from sqlalchemy import create_engine

local_modules_path = os.path.abspath('..')

if local_modules_path not in sys.path:
    sys.path.append(local_modules_path)

from src.utils.osm_helpers import create_enriched_osmnx_graph

from src.utils.visualization import render_side_by_side_detour_map_for_edge
from src.core.redundancy_metric import compute_redundancy_for_edges
from src.utils.io_helpers import create_detours_gpkg
from src.utils.db_helpers import load_detours_analysis_into_postgis

##### Parse command line arguments #####

parser = argparse.ArgumentParser(description='Compute Redundancy using Utah Methodology.')
parser.add_argument('-f', '--filename', type=str, help='Path to the OSM PBF file.', required=True)

args = parser.parse_args()

osm_pbf = args.filename

##### Configuration #####

this_dir = os.path.dirname(
    os.path.abspath(__file__)
)

postgres_env_path = os.path.join(
    this_dir,
    '../config/.env'
)

# Take the first part of the PBF file name that includes a string of at least 5 consecutive digits.
# roadways_prefix_regex = re.compile(r'^vehicle-nonservice-roadways')
geoid_regex = re.compile(r'[0-9]{5,}')

region_id = [
    p for p in os.path.basename(osm_pbf).split('_')
    if re.search(geoid_regex, p)
][0]

region_geoid = re.search(geoid_regex, region_id).group(0)

## Config file paths
out_dir = os.path.join(this_dir, '../redundancy_analysis_output', region_id)
shutil.rmtree(out_dir, ignore_errors=True)
os.makedirs(out_dir, exist_ok=True)

osm_graph_output_file = os.path.join(out_dir, 'osmnx_graph.pkl')
road_network_image_file = os.path.join(out_dir, 'road_network.png')

detour_maps_dir = os.path.join(out_dir, 'detour_maps')
os.makedirs(detour_maps_dir, exist_ok=True)

output_gpkg_path = os.path.join(out_dir, 'redundancy-analysis.gpkg')

## Config DB schema name
postgres_schema = f"e001_001_{region_geoid}"

if len(postgres_schema) > 63:
    raise ValueError(f"postgres_schema name is too long: {postgres_schema}")

## Create OSMnx Graph
enriched_osmnx_graph = create_enriched_osmnx_graph(osm_pbf)

g = enriched_osmnx_graph['g']
edges_gdf = enriched_osmnx_graph['edges_gdf']

with open(osm_graph_output_file, 'wb') as pfile:
    pickle.dump(g, pfile, protocol=pickle.HIGHEST_PROTOCOL)

major_roadways_edge_ids = edges_gdf[edges_gdf['edge_min_roadclass'].between(0, 4, inclusive='both')].index
detours_info_df = compute_redundancy_for_edges(g=g, edge_ids=major_roadways_edge_ids)

for rank, edge_id in enumerate(detours_info_df.nlargest(5, 'difference_sec').index):
    render_side_by_side_detour_map_for_edge(
        out_fpath=os.path.join(
            detour_maps_dir,
            f'detour-rank-{rank + 1}.edge_id-{edge_id[0]}_{edge_id[1]}_{edge_id[2]}.png'
        ),
        roadways_gdf=edges_gdf,
        detour_info_df=detours_info_df,
        edge_id=edge_id
    )

    time.sleep(3)

create_detours_gpkg(
    filename=output_gpkg_path,
    roadways_gdf=edges_gdf,
    detours_info_df=detours_info_df
)

## Write analysis to PostgreSQL DB.

pg_creds = dotenv.dotenv_values(postgres_env_path)

user = f"{pg_creds['PGUSER']}:{pg_creds['PGPASSWORD']}"
mach = f"{pg_creds['PGHOST']}:{pg_creds['PGPORT']}"
pgdb = pg_creds['PGDATABASE']

# Because load_detours_analysis_into_postgis is destructive and not well tested,
# punt if not expected PGDATABASE name.
if pgdb != 'avail_rnr':
    raise BaseException('SAFETY INVARIANT BROKEN: PGDATABASE must be "avail_rnr".')

# Connect to the PostGIS database
postgres_engine = create_engine(
    f"postgresql://{user}@{mach}/{pgdb}",
    connect_args={'connect_timeout': (3600 * 96)}, # 96 hours == 4 days
)

with postgres_engine.connect() as db_conn:
    load_detours_analysis_into_postgis(
        db_conn=db_conn,
        postgres_schema=postgres_schema,
        roadways_gdf=edges_gdf,
        detours_info_df=detours_info_df,
        clean_schema=True
    )
