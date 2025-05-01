import os

from prefect import flow

from experiments.e001_pjt_utah_redundancy import (
    complete_prefect_workflow as e001_utah_redundancy_flow,
)
from experiments.e004_pjt_flood_impact import (
    complete_prefect_workflow as e004_flood_impact_flow,
)
from experiments.e008_pjt_networkx_centrality import (
    complete_prefect_workflow as e008_network_metrics_flow,
)
from experiments.e009_pjt_osm_ris_conflation import (
    complete_prefect_workflow as e009_conflation_flow,
)
from tasks.osm.tasks import extract_osm_region_road_network_task

this_dir = os.path.abspath(os.path.dirname(__file__))

BASE_OSM_PBF = os.path.join(
    this_dir,
    "../../data/processed/osm/nonservice-roadways-buffer-50mi-state-36_us-250101.osm.pbf",
)

FLOODPLAINS_PATH = os.path.join(
    this_dir,
    "../../data/raw/avail/merged_floodplains/hazmit_db.s379_v841_avail_nys_floodplains_merged.1730233335.gpkg.zip",
)
RIS_PATH = os.path.join(
    this_dir, "../../data/raw/nysdot/milepoint_snapshot/lrsn_milepoint.gpkg"
)

NYSDOT_BRIDGES_PATH = os.path.join(
    this_dir, "../../data/raw/nysdot/nysdot_structures/NYSDOT_Bridges.20240909"
)

NYSDOT_LARGE_CULVERS_PATH = os.path.join(
    this_dir, "../../data/raw/nysdot/nysdot_structures/NYSDOT_Large_Culverts.20241111"
)


geoid = "36057"


@flow(name="Experiment Orchestration and Workflow")
def main(
    geoid: str,
):
    osm_pbf = extract_osm_region_road_network_task(
        base_osm_pbf=BASE_OSM_PBF,  #
        geoid=geoid,
        buffer_dist_mi=10,
    )

    e001_gpkg = e001_utah_redundancy_flow(osm_pbf=osm_pbf)
    e004_gpkg = e004_flood_impact_flow(
        osm_pbf=osm_pbf,  #
        floodplains_gpkg=FLOODPLAINS_PATH,
    )
    e008_gpkg = e008_network_metrics_flow(osm_pbf=osm_pbf)
    e009_gpkg = e009_conflation_flow(
        osm_pbf=osm_pbf,
        ris_path=RIS_PATH,
        nysdot_bridges_path=NYSDOT_BRIDGES_PATH,
        nysdot_large_culverts_path=NYSDOT_LARGE_CULVERS_PATH,
    )

    print(e001_gpkg)
    print(e004_gpkg)
    print(e008_gpkg)
    print(e009_gpkg)


if __name__ == "__main__":
    # Optionally, replace with argparse to parse geoid from CLI
    main(geoid)
