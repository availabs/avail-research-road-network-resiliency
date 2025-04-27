#!/bin/bash

python -m xtasks.ris_conflation_prefect_workflow \
  --osm-pbf "../../data/processed/osm/nonservice-roadways-buffer-10mi-county-36001_nonservice-roadways-buffer-50mi-state-36_us-250101.osm.pbf" \
  --ris-path "../../data/raw/nysdot/milepoint_snapshot/lrsn_milepoint.gpkg" \
  --nysdot-bridges-path "../../data/raw/nysdot/nysdot_structures/NYSDOT_Bridges.20240909" \
  --nysdot-large-culverts-path "../../data/raw/nysdot/nysdot_structures/NYSDOT_Large_Culverts.20241111" \
  --osrm-host "http://127.0.0.1:5001" \
