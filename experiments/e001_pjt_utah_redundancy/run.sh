#!/bin/bash

python xtasks/complete_prefect_workflow.py \
  --osm-pbf "../../data/processed/osm/nonservice-roadways-buffer-10mi-county-36001_nonservice-roadways-buffer-50mi-state-36_us-250101.osm.pbf" \
  --verbose
