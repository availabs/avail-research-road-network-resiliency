#!/bin/bash

python -m tasks.batch_metrics_prefect_workflow \
  --osm-pbf "../../data/processed/osm/nonservice-roadways-buffer-10mi-county-36001_nonservice-roadways-buffer-50mi-state-36_us-250101.osm.pbf"
