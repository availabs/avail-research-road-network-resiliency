#!/bin/bash

# --osm-pbf "../../data/processed/osm/nonservice-roadways-buffer-10mi-county-36001_nonservice-roadways-buffer-50mi-state-36_us-250101.osm.pbf" \
python xtasks/complete_prefect_workflow.py  \
  --osm-pbf "../../data/processed/osm/nonservice-roadways-buffer-10mi-county-36041_nonservice-roadways-buffer-50mi-state-36_us-250101.osm.pbf" \
  --floodplains-gpkg "../../data/raw/avail/merged_floodplains/hazmit_db.s379_v841_avail_nys_floodplains_merged.1730233335.gpkg.zip"
