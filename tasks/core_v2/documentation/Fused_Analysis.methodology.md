# Methodology for Comprehensive Road Network Resilience Analysis

## 1. Introduction

This document provides a detailed methodology for the fusion of multiple individual, domain-specific analyses into a single, comprehensive dataset for assessing road network resilience.
The primary goal is to combine insights from various risk and criticality factors to create a holistic view of the road network.
This allows for the identification of segments that are not only critical to network function but are also vulnerable to multiple hazards.

The methodology leverages six core domain-specific analyses, which are briefly summarized below.
The results from these analyses are then systematically combined using the DuckDB spatial database engine.
This process involves joining the datasets, calculating a series of normalized scores, and ultimately producing composite scores for criticality, risk, and overall resilience.
The final outputs are delivered in two formats: a complete, fully-attributed GeoPackage for in-depth analysis and a simplified version containing the most salient fields for easier consumption and visualization.

### 1.1. Summaries of Individual Analyses

The comprehensive analysis is built upon the outputs of six distinct, domain-specific studies:

- **E001: Redundancy Analysis**: This analysis quantifies the impact of a road segment's closure on the transportation network. It calculates the additional travel time, in seconds, required to traverse a detour route if a given road segment were to become impassable. A high value indicates a lack of viable alternative routes, making the segment critical for network efficiency.

- **E004: Flood Impact Analysis**: This study identifies road segments susceptible to flooding based on FEMA flood hazard data. It distinguishes between segments directly inundated by 100-year or 500-year flood events and those that, while not flooded themselves, become functionally disconnected or isolated from the main network due to nearby flooded segments.

- **E007: TRANSCOM Flooding Events Analysis**: This analysis leverages historical event data from the Transportation Operations Coordinating Committee (TRANSCOM) to identify road segments frequently impacted by flooding-related incidents. It aggregates data on road closures, flooding, and related repairs to quantify the historical frequency and duration of disruptions on each segment.

- **E008: Network Centrality Analysis**: This analysis evaluates the structural importance of each road segment to the network's connectivity. It uses Edge Betweenness Centrality, calculated within the context of local Core-Based Statistical Areas (CBSAs), to identify segments that act as critical "bridges" for traffic flow within a region. High centrality suggests a segment is vital for the daily functioning of its community.

- **E009: RIS/OSM Conflation**: This process integrates data from the NYSDOT Roadway Inventory System (RIS) with the OpenStreetMap (OSM) road network. The primary purpose is to enrich the OSM network with official state data, such as Annual Average Daily Traffic (AADT) counts and the locations of large culverts, which are important inputs for risk assessment.

- **E010: Hydrography Analysis**: This analysis assesses the exposure of road segments to hydrological features. It identifies and counts the number of intersection points between roadways and flowlines from the National Hydrography Dataset (NHDPlus HR). This serves as a proxy for the baseline hydrological hazard exposure.

## 2. Comprehensive Fusion Methodology

The fusion of the individual analysis layers is performed using a Python script (`runit.py`) that orchestrates a series of SQL queries executed by DuckDB.
This approach is memory-efficient and allows for complex relational operations directly on the GeoPackage source files.

The process consists of the following key steps:

1. **Environment Setup**: The script begins by connecting to an in-memory DuckDB database and loading the necessary `spatial` extension.

2. **Data Ingestion as Views**: Instead of loading entire files into memory, each input GeoPackage layer is registered as a temporary `VIEW` within DuckDB. This includes the base road network and the outputs from the six individual analyses (E001, E004, E007, E008, E009, E010). Only the essential columns for the fusion are selected from each source.

3. **Data Joining**: A single, unified view (`joined_view`) is created by performing a `LEFT OUTER JOIN` from the `base_edges` view to each of the six experiment views. The join key is the unique road segment identifier (`u`, `v`, `key`). In this step, columns from the joined tables are prefixed (e.g., `e001_`, `e004_`) to denote their origin.

4. **Scoring and Classification**: A subsequent view (`final_fused_view`) is built upon the joined data to calculate a suite of analytical scores. This is the core of the fusion process, where raw metrics from individual analyses are transformed into normalized, comparable scores and then combined into composite indicators. This step is detailed further in the "Scoring Framework" section below.

5. **Output Simplification**: For ease of use, a `final_simplified_view` is created. This view selects a curated subset of the most critical attributes from the `final_fused_view`, renaming some for clarity (e.g., `e000_road_name` becomes `road_name`).

6. **Data Export**: Finally, the script executes `COPY` commands to write the contents of the `final_fused_view` and `final_simplified_view` to two new output GeoPackage files. The output files are made read-only to prevent accidental modification.

## 3. Scoring Framework

The scoring framework translates the diverse metrics from the individual analyses into a consistent system for evaluating risk and criticality.
All calculations are performed within DuckDB using SQL expressions.

### 3.1. Individual Score Calculations

- **scores_redundancy_normalized**: Normalizes the raw detour time from the Redundancy analysis (`e001_difference_sec`) onto a 0-10 scale using a min-max formula. A higher score indicates a longer detour and thus lower redundancy.

- **scores_redundancy_bin_weight**: Classifies the raw detour time (e001_difference_sec) into five weighted bins, providing a simpler categorical measure of redundancy impact. This allows for a tiered assessment where a higher weight signifies a more severe lack of redundancy. The bins are defined as follows:

  - **Weight 1**: Detour adds less than 1 minute (0 < `difference_sec` < 60).
  - **Weight 2**: Detour adds 1 to 2 minutes (60 <= `difference_sec` <= 120).
  - **Weight 3**: Detour adds 2 to 5 minutes (120 < `difference_sec` <= 300).
  - **Weight 4**: Detour adds 5 to 10 minutes (300 < `difference_sec` <= 600).
  - **Weight 5**: Detour adds more than 10 minutes (`difference_sec` > 600).

- **scores_flood_risk**: Assigns a risk score based on the predicted flood frequency from the Flood Impact analysis (`e004_nonfunctional_frequency`).

  - A 100-year flood vulnerability receives a score of 2, and
  - a 500-year vulnerability receives a score of 1.

- **scores_flooding_events_risk**: Assigns a risk score based on the historical number of TRANSCOM flooding-related events (`e007_all_events_count`).

  - Segments with more than one event receive a score of 2, and
  - those with one event receive a score of 1.

- **scores_centrality_normalized**: This score directly uses the pre-calculated `e008_all_cbsa_edge_betweenness_rank_normalized` value from the Network Centrality analysis, which reflects a segment's importance across all CBSAs.

- **scores_culvert_risk**: A binary score of 1 is assigned if a road segment is associated with a large culvert (`e009_ris_large_culvert_cin` is not null), otherwise the score is 0. This flags potential risk from culvert failure.

- **scores_hydrography_risk**: A binary score of 1 is assigned if a road segment crosses any NHD flowline (`e010_num_total_intersection_points` > 0), otherwise the score is 0. This indicates general exposure to hydrological features.

### 3.2. Composite Score Calculations

- **scores_total_risk**: This is a composite risk score calculated by summing the individual risk factors: `scores_flood_risk` + `scores_culvert_risk` + `scores_hydrography_risk` + `scores_flooding_events_risk`. It represents a cumulative, multi-hazard risk assessment.

- **scores_criticality** & **scores_criticality_v2**: These scores measure the overall importance of a road segment by combining its network centrality with its lack of redundancy. They are calculated by multiplying the normalized centrality score by the normalized and binned redundancy scores, respectively.

- **scores_resiliency** & **scores_resiliency_v2**: These are the final, top-level scores. They provide an inverse measure of resilience by multiplying the `scores_total_risk` by the `scores_criticality` scores. A high resiliency score indicates that a road segment is both highly critical to the network and subject to numerous risk factors, making it a top priority for resilience-hardening efforts.

## 4. Data Dictionaries

The analysis produces two primary output files, each with a distinct schema.

### 4.1. Complete Fused Output (`fused_experiments_output.gpkg`)

This file contains the full set of joined attributes from the base network and all six analyses, as well as all calculated scores.
Field names are prefixed to indicate their source analysis.

| **Column Name**                               | **Data Type** | **Description**                                                                                                                                           |
| :-------------------------------------------- | :------------ | :-------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `u`, `v`, `key`                               | Integer64     | The unique identifier for the road segment, derived from the OSMnx graph nodes and key.                                                                   |
| `e000_*`                                      | Various       | Base attributes of the road segment from the original road network data (e.g., `e000_road_name`, `e000_length_mi`, `e000_speed_kph`).                     |
| `e001_difference_sec`                         | Real          | The additional travel time (in seconds) incurred by a detour if this segment were closed. From the Redundancy analysis.                                   |
| `e004_*`                                      | Various       | Attributes from the Flood Impact analysis, such as `e004_nonfunctional_reason` and `e004_nonfunctional_frequency`.                                        |
| `e007_*`                                      | Various       | Attributes from the TRANSCOM Events analysis, including event counts (`e007_all_events_count`) and total duration hours.                                  |
| `e008_*`                                      | Various       | Attributes from the Network Centrality analysis, including normalized betweenness centrality and rank (`e008_all_cbsa_edge_betweenness_rank_normalized`). |
| `e009_*`                                      | Various       | Attributes from the RIS Conflation analysis, including AADT (`e009_ris_counts_aadt`) and large culvert IDs (`e009_ris_large_culvert_cin`).                |
| `e010_num_total_intersection_points`          | Integer64     | The total number of times the road segment intersects with a hydrographic flowline. From the Hydrography analysis.                                        |
| `scores_redundancy_normalized`                | Real          | Normalized redundancy score (0-10 scale). Higher is worse.                                                                                                |
| `scores_redundancy_bin_weight`                | Integer       | Binned redundancy score (1-5). Higher is worse.                                                                                                           |
| `scores_flood_risk`                           | Integer       | Calculated flood risk score (0-2).                                                                                                                        |
| `scores_flooding_events_risk`                 | Integer       | Calculated risk score from historical flooding events (0-2).                                                                                              |
| `scores_centrality_normalized`                | Real          | The normalized network centrality score.                                                                                                                  |
| `scores_culvert_risk`                         | Integer       | Binary risk score indicating the presence of a large culvert (0 or 1).                                                                                    |
| `scores_hydrography_risk`                     | Integer       | Binary risk score indicating an intersection with a water flowline (0 or 1).                                                                              |
| `scores_criticality`, `scores_criticality_v2` | Real          | Composite scores combining centrality and redundancy. Higher score indicates higher criticality.                                                          |
| `scores_total_risk`                           | Integer       | Composite score summing multiple risk factors.                                                                                                            |
| `scores_resiliency`, `scores_resiliency_v2`   | Real          | Final composite scores combining total risk and criticality. Higher score indicates lower resilience.                                                     |
| `geom`                                        | Unknown (any) | The geometry of the road segment.                                                                                                                         |

### 4.2. Simplified Output (`simplified_road_network_analysis.gpkg`)

This file provides a curated and more user-friendly subset of the full data, intended for direct visualization and high-level analysis.

| **Column Name**                                      | **Data Type**    | **Description**                                                                                                     |
| :--------------------------------------------------- | :--------------- | :------------------------------------------------------------------------------------------------------------------ |
| `u`, `v`, `key`                                      | Integer64        | The unique identifier for the road segment.                                                                         |
| `openlr_roadclass`                                   | Integer64        | The OpenLR road class classification.                                                                               |
| `road_name`, `from_name`, `to_name`                  | String           | The names of the road and the cross-streets at its start and end.                                                   |
| `length_mi`                                          | Real             | The length of the road segment in miles.                                                                            |
| `utah_redundancy_detour_seconds_diff`                | Real             | The additional travel time (in seconds) for a detour. Sourced from E001.                                            |
| `intra_cbsa_edge_betweenness_rank`                   | Integer64        | The segment's centrality rank within its specific CBSA. Sourced from E008.                                          |
| `all_cbsa_edge_betweenness_rank`                     | Integer64        | The segment's centrality rank across all CBSAs. Sourced from E008.                                                  |
| `e009_ris_counts_aadt`, `..._su_aadt`, `..._cu_aadt` | Real             | Annual Average Daily Traffic for all, single-unit, and combination-unit vehicles. Sourced from E009.                |
| `scores_redundancy_normalized`                       | Real             | Normalized redundancy score (0-10 scale).                                                                           |
| `scores_redundancy_bin_weight`                       | Integer          | Binned redundancy score (1-5).                                                                                      |
| `scores_centrality_normalized`                       | Real             | The normalized network centrality score.                                                                            |
| `scores_flood_risk`                                  | Integer          | Calculated flood risk score.                                                                                        |
| `scores_culvert_risk`                                | Integer          | Binary risk for large culvert presence.                                                                             |
| `scores_hydrography_risk`                            | Integer          | Binary risk for crossing a water flowline.                                                                          |
| `scores_criticality`, `scores_criticality_v2`        | Real             | Composite criticality scores.                                                                                       |
| `scores_total_risk`                                  | Integer          | Composite risk score.                                                                                               |
| `scores_resiliency`, `scores_resiliency_v2`          | Real             | Final composite resiliency scores.                                                                                  |
| `flooding_vulnerability_frequency`                   | String           | If the segment is vulnerable to flooding, this indicates the frequency ('100 YEAR', '500 YEAR'). Sourced from E004. |
| `crosses_national_hydrography_dataset_flowline`      | Integer(Boolean) | A boolean (true/false) flag indicating if the segment crosses a water flowline. Sourced from E010.                  |
| `geom`                                               | Unknown (any)    | The geometry of the road segment.                                                                                   |
