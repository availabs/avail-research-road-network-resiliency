#!/bin/bash

python -m xtasks.network_metrics_prefect_flow \
  --osm-pbf "../../data/processed/osm/nonservice-roadways-buffer-10mi-county-36001_nonservice-roadways-buffer-50mi-state-36_us-250101.osm.pbf"
