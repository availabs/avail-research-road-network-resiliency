# Preliminary Hydrological Hazard Assessment for Road Network Resilience

**Date:** May 26, 2025
**Prepared for:** Principal Investigator, Department of Geography and Planning
**Prepared by:** [Your Name/Lab Name] & Gemini AI Collaborator

![Roadways/Flowlines Intersections Map Visualization](./images/roadways_x_flowlines.png)

## 0. High-Level Overview of the Analysis

**Value and Purpose:**
This document outlines the methodology and initial findings of an experiment to
assess the preliminary hydrological hazard exposure for segments of the road
network. The core idea is to identify every point where a road crosses or closely
interacts with a waterway (as defined by the National Hydrography Dataset Plus
High Resolution - NHDPlus HR) and then to summarize the key hydrological
characteristics of these interactions at the level of individual road segments.

The value of this analysis lies in creating a foundational GIS layer that quantifies
the baseline hydrological hazard for each road segment based purely on the
characteristics of the flowlines it intersects. This output serves two main
purposes:

1. As a **standalone GIS overlay layer**, it provides an initial, spatially explicit
   assessment of where roadways might be more susceptible to hydrological impacts
   (e.g., flooding, scour potential due to high flow or large drainage areas).
   This can inform preliminary planning and identification of areas requiring
   more detailed investigation.
2. As a **crucial intermediate dataset**, it provides a rich set of
   roadway-level hydrological hazard indicators. This dataset is designed to be
   integrated in future research stages with detailed data on transportation
   infrastructure (such as NYSDOT bridge and culvert conditions) to develop a
   more comprehensive and nuanced model of overall road network risk and
   resilience.

**Inputs:**

1. **OSM Road Network Data**: Processed via OSMnx into a GeoDataFrame of road
   segments (`road_spans_gdf`), indexed by `(u, v, key)` representing unique
   road edges between nodes, and further delineated by `span_idx` and
   `_road_span_type_` (distinguishing 'BRIDGE' from 'NONBRIDGE' spans). The
   geometry is LineString.
2. **NHDPlus High Resolution (HR) Flowline Data**: A GeoDataFrame of NHDPlus HR
   `NetworkNHDFlowline` features (`nhd_flowlines_gdf`) for the study area.
   This contains detailed linear representations of waterways and a rich set of
   attributes describing their hydrological characteristics. The geometry is
   LineString or MultiLineString. Expected to be indexed by
   `permanent_identifier`.

**Outputs:**

1. **Primary Detailed Output (Intermediate Data for Future Use)**:

   - **`detailed_intersections_gdf.gpkg`**: A GeoPackage containing a
     GeoDataFrame where each row represents a unique, exploded part of an
     intersection between an OSM road span and an NHDPlus HR flowline segment.
   - **Index**: `['u', 'v', 'key', 'span_idx', '_road_span_type_', 'permanent_identifier', 'intxn_idx']`.
   - **Geometry**: Point (location of the intersection part).
   - **Attributes**: Includes all raw attributes from the intersected NHDPlus
     flowline (e.g., `ftype`, `fcode`, `streamorde`, `totdasqkm`, `qema`,
     `slope`, `gnis_name`, `wbarea_permanent_identifier`, `lengthkm`).

2. **Aggregated Roadway Hydrological Hazard Indicators (Final Output of this Experiment / GIS Layer)**:
   - **`roadway_hydrological_hazards_v1.gpkg`** (or similar): A GeoPackage
     containing a DataFrame (or GeoDataFrame if road geometries are joined back)
     where each row represents a unique OSM road segment `(u, v, key)`.
   - **Index**: `['u', 'v', 'key']`.
   - **Attributes**: Columns summarizing the hydrological exposure from all
     flowline intersections along that road segment. Examples include:
     - `num_total_intersection_points`: Total number of (exploded)
       intersection parts on the roadway.
     - `num_distinct_flowlines_crossed`: Number of unique NHDPlus flowlines
       crossed.
     - `max_qema_cfs`: Maximum mean annual flow (CFS) from any intersected
       flowline.
     - `max_streamorde`: Maximum stream order intersected.
     - `max_totdasqkm`: Maximum total drainage area (km²) from any
       intersected flowline.
     - `avg_slope`: Average slope of intersected flowlines.
     - `has_perennial_crossing`: Boolean indicating if any intersection is with
       a perennial flowline.
     - `has_named_flowline_crossing`: Boolean.
     - `crosses_areal_waterbody`: Boolean.
     - `dominant_ftype_crossed`: Most common FType of flowlines crossed.
     - **`roadway_hydrological_hazard_score_v1`**: A single, composite
       numerical score (continuous) derived from a weighted sum of normalized
       versions of the above aggregated indicators. This score exists
       alongside the individual factors.

## 1. The NHDPlus High Resolution (HR) Flowlines Dataset

The National Hydrography Dataset Plus High Resolution (NHDPlus HR) is a
state-of-the-art geospatial hydrography framework for the United States,
developed by the U.S. Geological Survey (USGS) and the U.S. Environmental
Protection Agency (EPA). It integrates the best available National
Hydrography Dataset (NHD) at a 1:24,000-scale or better, the Watershed Boundary
Dataset (WBD), and 1/3 arc-second (approximately 10-meter) 3D Elevation Program
(3DEP) data.

The NHDPlus HR provides a nationally seamless, interconnected stream network,
offering significantly greater detail and more current content than its
predecessor, NHDPlus Version 2. Its core component for this analysis is
the **`NetworkNHDFlowline`** feature class.

**Key characteristics of the `NetworkNHDFlowline` data include:**

- **Linear Representation**: It depicts surface water features (rivers, streams,
  canals, artificial paths through waterbodies, etc.) as linear geometries
  (LineString or MultiLineString).
- **Connectivity**: Features are topologically connected to represent the
  hydrographic network, allowing for network tracing and analysis (e.g.,
  upstream/downstream).
- **Rich Attribution**: Each flowline segment carries standard NHD attributes
  (like feature type, name) as well as a comprehensive suite of Value-Added
  Attributes (VAAs). These VAAs are computationally derived and enhance the
  dataset for modeling and analysis. They include variables such as
  drainage area, stream order, slope, and estimates of mean annual streamflow
  and velocity.
- **Catchment Association**: Each flowline (typically) has an associated
  elevation-derived catchment, which is the local land area that drains directly
  to it. This linkage allows for relating landscape characteristics to
  the stream network.

For our research, the `NetworkNHDFlowline` dataset provides the detailed
information about the waterways that road segments intersect, forming the basis of
our hydrological hazard assessment.

## 2. In-Depth Description of Key NHDPlus HR Flowline Columns Used

The following NHDPlus HR `NetworkNHDFlowline` attributes (as identified from
`ogrinfo` and the NHDPlus HR User's Guide) are central to this analysis for
characterizing hydrological hazard at road-waterway intersections. (Note: Units
for flow/velocity VAAs are typically CFS/FPS for EROM attributes as per the User's Guide).

- **`permanent_identifier`** (String):
  - A unique, stable alphanumeric identifier for each NHD flowline feature.
    It serves as the primary key for raw NHD features and ensures consistent
    referencing. Used in this analysis to count distinct flowlines crossed.
- **`gnis_name`** (String):
  - The official feature name from the Geographic Names Information System (GNIS),
    if available (e.g., "Hudson River").
  - _Relevance_: Helps identify significant, named waterways, which might
    correlate with higher public awareness or known flood risk. Used to flag
    if a road crosses a named vs. unnamed flowline.
- **`ftype`** (Integer - NHD Feature Type Code):
  - A code representing the general category of the hydrographic feature.
    Examples relevant to this study:
    - `460`: Stream/River
    - `336`: Canal/Ditch
    - `558`: Artificial Path (often through waterbodies or representing
      connectors)
  - _Relevance_: Critical for distinguishing natural channels from artificial
    ones, which have different hydrological behaviors and risk implications.
    Used to determine the `dominant_ftype_crossed` by a road segment.
- **`fcode`** (Integer - NHD Feature Code):
  - A more detailed classification of the hydrographic feature, building upon
    `ftype`. For `ftype` 460 (Stream/River), common `fcode`s include:
    - `46006`: Stream/River, Perennial (flows year-round).
    - `46003`: Stream/River, Intermittent (flows seasonally).
    - `46007`: Stream/River, Ephemeral (flows only after precipitation).
  - _Relevance_: The flow regime (perennial, intermittent, ephemeral) is a
    direct indicator of water presence likelihood and type of flood risk
    (e.g., consistent inundation potential vs. flash flooding). Used to flag
    `has_perennial_crossing`.
- **`streamorde`** (Integer - Stream Order):
  - A hierarchical classification of streams based on the NHDPlus HR's
    modified Strahler method. Headwater streams are order 1; order
    increases as streams of the same order conjoin.
  - _Relevance_: Higher stream orders generally indicate larger rivers with
    greater discharge capacity and often wider floodplains, thus correlating
    with increased flood risk. We aggregate `max_streamorde` for each road
    segment.
- **`lengthkm`** (Real - Kilometers):
  - The geometric length of the individual `NHDFlowline` segment.
  - _Relevance_: While not directly aggregated in the current V1 score, it's
    a fundamental characteristic of the flowline segment.
- **`totdasqkm`** (Real - Total Drainage Area in Square Kilometers):
  - A Value-Added Attribute representing the total accumulated upslope
    drainage area contributing to the downstream end of the flowline segment.
  - _Relevance_: This is a primary indicator of the potential flow volume a
    stream can carry. Larger drainage areas generally lead to higher peak
    flows and thus higher flood risk potential. We aggregate `max_totdasqkm`
    for each road segment.
- **`slope`** (Real - Unitless, m/m):
  - The average water surface slope for the flowline segment, derived from
    smoothed elevation data.
  - _Relevance_: Influences water velocity, erosive power, and the style of
    flooding (e.g., flashy in steep areas, broad inundation in flat areas).
    We aggregate `avg_slope`.
- **`qema`** (Real - Mean Annual Gage-Adjusted Flow in **Cubic Feet per Second**):
  - A Value-Added Attribute from the NHDPlusEROMMA table, representing the
    mean annual flow (typically for 1971-2000) at the downstream end of the
    flowline, adjusted using nearby streamgage data where available.
    The NHDPlus HR User's Guide indicates this is often considered the "best"
    available mean annual flow estimate.
  - _Relevance_: A direct estimate of the typical volume of water flow,
    crucial for assessing flood hazard. Higher `qema` suggests a greater
    capacity for flooding. We aggregate `max_qema_cfs`.
- **`wbarea_permanent_identifier`** (String):
  - An identifier that links an `NHDFlowline` to an `NHDArea` or
    `NHDWaterbody` feature if the flowline is part of, or represents an
    artificial path through, an areal water body (e.g., lake, reservoir,
    wide river segment).
  - _Relevance_: Indicates that the road is interacting with a potentially
    larger, standing, or slower-moving body of water, which can have different
    risk characteristics (e.g., inundation due to lake level changes,
    backwater effects) than a defined channel. Used to flag
    `crosses_areal_waterbody`.

## 3. Methodology

The analysis involves two main functional stages, encapsulated in Python modules:

**Stage 1: Detailed Intersection Point Generation (`core_logic.py` - `process_road_flowline_intersections` function)**

This stage focuses on the geometric and network operations to identify all unique
points of interaction between the input OSM road network and NHDPlus HR flowlines.

1.  **Input Validation & Preparation**:

    - Input `road_spans_gdf` and `nhd_flowlines_gdf` are validated.
    - Coordinate Reference Systems (CRS) are aligned (NHD flowlines reprojected
      to match road spans' CRS if necessary).
    - For the overlay operation:
      - `road_spans_gdf` is processed to retain only its original index levels
        (which become columns) and its geometry column. Other data attributes
        are dropped to ensure only identifying information for the road span is
        carried forward for this specific join.
      - `nhd_flowlines_gdf` has its index reset (so `permanent_identifier`
        becomes a column) and all its original data attributes (including
        those listed in Section 2) are retained.

2.  **Geometric Intersection**:

    - A spatial overlay (`geopandas.overlay`) is performed between the prepared
      road spans and NHD flowlines using an "intersection" operation. This
      identifies all geometric locations where road segments and flowlines
      coincide or cross.
    - The operation is configured with `keep_geom_type=False` (allowing output
      geometries to be Points, LineStrings, or their multi-part versions) and
      `make_valid=True` (to attempt to resolve any invalid geometries that might
      arise).

3.  **Initial Composite Indexing**:

    - The result of the overlay typically contains columns derived from the
      indexes of both input datasets:

      - `u, v, key, span_idx, _road_span_type_` from roads, and
      - `permanent_identifier`from flowlines.

    - A 6-level MultiIndex
      `['u', 'v', 'key', 'span_idx', '_road_span_type_', 'permanent_identifier']`
      is set on this intersection GeoDataFrame. This index uniquely identifies each
      pairing of an OSM road span component with an NHD flowline it intersects.
      The result is sorted by this index.

4.  **Explode Multi-Geometries & Add Part Index**:

    - Intersection geometries can be multi-part (e.g., a `MultiPoint` if a
      single road span crosses a single flowline at multiple distinct locations,
      or `MultiLineString` for overlaps).
    - The `explode(index_parts=True)` method is applied. This operation converts
      any multi-part geometries into multiple rows, each containing a
      single-part geometry.
    - Crucially, `index_parts=True` adds a new, 7th level to the MultiIndex,
      named `'intxn_idx'`. This `intxn_idx` is a zero-based integer identifying
      each individual part within its original multi-part geometry. For
      geometries that were already single-part, `intxn_idx` will be 0.

5.  **Convert LineString Intersections to Points**:

    - If the overlay and explosion result in `LineString` geometries (e.g.,
      from road segments running alongside or overlapping with flowlines), these
      are converted to representative `Point` geometries.
    - This is achieved by taking the midpoint of each LineString using
      `geometry.interpolate(0.5, normalized=True)`. `Point` geometries remain
      as Points.

6.  **Output of Stage 1**: The `detailed_intersections_gdf`. This GeoDataFrame
    has Point geometries for every exploded part of an intersection and is indexed
    by the 7-level key. It contains all attributes from the NHD flowline involved
    in each specific intersection part. This is saved as an intermediate GIS layer
    for potential future use, particularly for detailed structure matching.

**Stage 2: Aggregation to Roadway Level & Composite Hazard Score Calculation (`road_intersection_risk_assessment_v1.py` - `aggregate_risks` and `calculate_composite_roadway_hazard_score` functions)**

This stage takes the granular `detailed_intersections_gdf` and summarizes
hydrological hazard characteristics at the level of unique road segments `(u,v,key)`.

1. **Aggregation of NHDPlus Attributes**:

   - The `detailed_intersections_gdf` (with its 7-level index) is grouped by
     its first three index levels: `['u', 'v', 'key']`, which define unique
     roadway segments from the OSMnx graph.
   - For each `(u,v,key)` group, various aggregation functions are applied to
     the NHDPlus attributes of all intersection points falling on that roadway
     segment:
     - `size` (on geometry column): To count total intersection parts.
     - `pd.Series.nunique` (on `permanent_identifier` column, after resetting
       it from index): To count distinct flowlines crossed.
     - `max`: For `qema`, `streamorde`, `totdasqkm`.
     - `mean`: For `slope`.
     - Custom functions (`.any()`, `.mode()`, `.notna().any()`): For boolean
       flags like `has_perennial_crossing`, `has_named_flowline_crossing`,
       `crosses_areal_waterbody`, and to find the `dominant_ftype_crossed`.
   - The output is `roadway_aggregated_hazards_df`, indexed by `(u,v,key)`.

2. **Calculation of Composite Hydrological Hazard Score (V1)**:

   - **Normalization/Scaling**: Selected aggregated indicators from
     `roadway_aggregated_hazards_df` (e.g., `max_qema_cfs`, `max_streamorde`,
     `max_totdasqkm`, `has_perennial_crossing`) are transformed to a common
     scale (typically 0-1, where 1 indicates higher contribution to hazard)
     using simple stub functions (e.g., division by a threshold, log scaling for
     wide-ranging values, boolean to int). These become new "normalized score"
     columns.
   - **Weights**: Initial weights are defined (all `1.0` for this V1) for each
     normalized score component.
   - **Composite Score**: A new column,
     `roadway_hydrological_hazard_score_v1`, is calculated as the weighted sum
     of the normalized score components. This provides a single, continuous
     numerical estimate of hydrological hazard for each roadway segment.

3. **Output of Stage 2 (Final for this Experiment)**: The
   `roadway_aggregated_hazards_df` augmented with the normalized score
   components and the `roadway_hydrological_hazard_score_v1`. This DataFrame,
   indexed by `(u,v,key)`, serves as the primary GIS overlay layer for this
   phase of the research. It can be joined back to the original road segment
   geometries for visualization.

## 4. Data Dictionary for Final Output (`roadway_aggregated_hazards_df`)

This DataFrame is indexed by `(u, v, key)`.

| Column Name                            | Data Type        | Description                                                                                                    | Derivation Notes                                                                         |
| :------------------------------------- | :--------------- | :------------------------------------------------------------------------------------------------------------- | :--------------------------------------------------------------------------------------- |
| `num_total_intersection_points`        | Integer          | Total number of exploded intersection parts along this roadway segment `(u,v,key)`.                            | `size` of group from `detailed_intersections_gdf`.                                       |
| `num_distinct_flowlines_crossed`       | Integer          | Number of unique NHDPlus HR `permanent_identifier`s crossed by this roadway segment.                           | `nunique` on `permanent_identifier`.                                                     |
| `max_qema_cfs`                         | Float            | Maximum mean annual gage-adjusted flow (Cubic Feet per Second) from any flowline intersected by this road.     | `max` of `qema`.                                                                         |
| `max_streamorde`                       | Integer          | Maximum NHDPlus Stream Order from any flowline intersected by this road.                                       | `max` of `streamorde`.                                                                   |
| `max_totdasqkm`                        | Float            | Maximum total upstream drainage area (km²) from any flowline intersected by this road.                         | `max` of `totdasqkm`.                                                                    |
| `avg_slope`                            | Float            | Average slope (m/m, unitless) of all flowline segments intersected by this road.                               | `mean` of `slope`.                                                                       |
| `has_perennial_crossing`               | Boolean (or 0/1) | True if this road intersects at least one perennial flowline (`fcode` 46006).                                  | Custom agg: `(series_fcode == 46006).any()`.                                             |
| `has_named_flowline_crossing`          | Boolean (or 0/1) | True if this road intersects at least one flowline with a non-null `gnis_name`.                                | Custom agg: `series_gnis_name.notna().any()`.                                            |
| `crosses_areal_waterbody`              | Boolean (or 0/1) | True if this road intersects at least one flowline linked to an `NHDArea`/`NHDWaterbody`.                      | Custom agg: `series_wbarea_pid.notna().any()`.                                           |
| `dominant_ftype_crossed`               | Integer          | The most frequently occurring NHD `ftype` among flowlines intersected by this road.                            | Custom agg: `series_ftype.mode().iloc[0]`.                                               |
| `norm_max_qema`                        | Float (0-1)      | Normalized score component for `max_qema_cfs`.                                                                 | From `calculate_composite_roadway_hazard_score`.                                         |
| `norm_max_streamorde`                  | Float (0-1)      | Normalized score component for `max_streamorde`.                                                               | From `calculate_composite_roadway_hazard_score`.                                         |
| `norm_max_totdasqkm`                   | Float (0-1)      | Normalized score component for `max_totdasqkm`.                                                                | From `calculate_composite_roadway_hazard_score`.                                         |
| `score_has_perennial`                  | Integer (0/1)    | Numerical score component for `has_perennial_crossing`.                                                        | From `calculate_composite_roadway_hazard_score`.                                         |
| _(Other normalized/scored components)_ | _(Numeric)_      | _(Additional normalized versions of aggregated indicators used in the composite score)_                        | _(From `calculate_composite_roadway_hazard_score`)_                                      |
| `roadway_hydrological_hazard_score_v1` | Float            | Composite hydrological hazard score for the roadway segment `(u,v,key)`, based on weighted NHDPlus indicators. | Sum of weighted, normalized score components. Higher indicates greater estimated hazard. |

_(Note: The exact list of "normalized score" columns will depend on the final
factors included in the V1 composite score calculation stub.)_

This document should provide a clear and comprehensive overview for your Principal
Investigator, suitable for understanding the current experimental stage and its outputs.
