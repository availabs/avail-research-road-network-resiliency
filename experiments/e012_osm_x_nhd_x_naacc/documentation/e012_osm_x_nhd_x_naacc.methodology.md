# **Methodology: Creating a High-Fidelity Road-Stream Crossing Inventory**

## **1. Introduction and Goal**

The primary goal of this analysis is to produce a single, highly accurate, and
information-rich map of all road-stream crossings (such as culverts and small
bridges) within a specific region. This final dataset is intended for
transportation and disaster mitigation planners who need reliable information to
assess network resilience, prioritize infrastructure maintenance, and plan for
future climate events.

To achieve this, we fuse three distinct and powerful datasets, each providing a
unique piece of the puzzle. By combining a comprehensive road map, a detailed
map of all waterways, and on-the-ground survey data of actual crossing
structures, we can create a final product that is more accurate and useful than
any single source alone.

## **2. The Three Core Datasets**

Our methodology is built on the careful integration of three key data sources:

- **OpenStreetMap (OSM): The Digital Road Network**

  - **What It Is:** Think of OpenStreetMap as the "Wikipedia for maps." It is a
    global, collaborative, and open-source project to create a free, editable map
    of the world. It contains an incredibly detailed and up-to-date road network,
    including everything from major highways and state routes to local streets,
    service roads, and even driveways.
  - **Its Role in the Analysis:** OSM provides the foundational road network.
    For this analysis, we use a comprehensive snapshot of this network, ensuring
    we can locate crossings on nearly any type of road a vehicle might travel.

- **National Hydrography Dataset Plus High Resolution (NHDPlus HR): The Waterway Map**

  - **What It Is:** This is the U.S. government's official, high-resolution
    digital map of all surface water. Maintained by the U.S. Geological Survey
    (USGS) and the Environmental Protection Agency (EPA), it depicts the nation's
    rivers, streams, and canals as a connected network of lines.
  - **Its Role in the Analysis:** The NHDPlus dataset tells us where the water
    is. Each waterway in the dataset is rich with information, such as its name,
    its estimated annual water flow, and the size of the land area that drains
    into it. This provides the essential hydrographic context for each crossing.

- **North Atlantic Aquatic Connectivity Collaborative (NAACC): The Ground-Truth Survey**

  - **What It Is:** The NAACC dataset is fundamentally different from the other
    two. It is not a map created from satellite imagery or digital sources, but a
    collection of detailed, on-the-ground surveys. Trained observers physically
    visit thousands of road-stream crossings and meticulously record information
    about the structures they find, such as the material of a culvert, its size,
    its physical condition, and its precise GPS location.

  - **Its Role in the Analysis:** The NAACC data provides the "ground truth." It
    confirms the actual existence of a crossing structure at a specific location
    and gives us detailed information about its physical attributes. Our primary
    goal is to accurately place these real-world structures onto our digital map.

## **3. The Data Fusion Workflow: A Step-by-Step Explanation**

Fusing these three datasets requires a sophisticated, multi-stage process
designed to find the single most accurate location for each real-world crossing.

### **Step 1: Finding All Potential Crossing Locations**

Before we can match the NAACC survey data, we first create a map of every
_potential_ crossing location. This is done by digitally overlaying the
complete road map (from OSM) on top of the complete waterway map (from NHDPlus).
The result is a new dataset of tens of thousands of points, with each point
representing a location where a road line and a stream line intersect on the
map. This gives us a comprehensive set of candidate locations to which we can
match the NAACC data.

### **Step 2: Preparing the Data for Matching**

With our datasets identified, we load them into a high-performance spatial
database. This allows for rapid and complex analysis. During this step, we
also prepare the OSM road network for matching. Since the physical location of
a bridge or culvert is the same regardless of the direction of traffic, we treat
two-way roads as single centerlines. This simplifies the matching process and
prevents a single crossing from being matched twice to the same road.

### **Step 3: The Matching Process — A Two-Pronged Strategy**

The core of the analysis is to find the correct location on our digital map for
each real-world crossing surveyed by NAACC. To do this reliably, we use a
two-pronged approach for every NAACC survey point:

- **Primary Strategy (Intersection Matching):** The system first looks for the
  nearest road-stream intersection point (from Step 1) that is within a close
  proximity (a 30-meter or roughly 100-foot radius). A direct match between a
  survey point and a digital intersection is the most ideal and confident
  scenario.

- **Secondary Strategy (Road Proximity Matching):** Sometimes, a waterway is too
  small to be included in the official NHDPlus map, or the map alignment may be
  slightly off. A common situation where this occurs is with roadside drainage
  ditches that have culverts but are not mapped as official streams. In these
  cases, there might not be a road-stream intersection point near the NAACC survey
  location. As a fallback, our system also finds the closest _road segment_ to
  the NAACC point. This ensures we can still find a likely location for the
  crossing even if a digital water layer is absent.

### **Step 4: Scoring and Selecting the Best Candidate**

A single NAACC survey point might have several potential matches nearby (e.g., a
close intersection and a few nearby road segments). To choose the single best
one, we use an intelligent scoring system that evaluates the quality of every
potential match. A lower score indicates a better match. The score is based on
several factors:

- **Proximity:** Closer is always better. A match that is only a few feet away
  is scored much more favorably than one that is 50 or 100 feet away.

- **Match Type:** A direct match to a road-stream intersection is considered
  more reliable and is given a better score than a simple proximity match to a
  road segment.

- **Road Name Similarity:** The system intelligently compares the road name
  recorded by the NAACC surveyor with the name from the OSM map. This comparison
  is sophisticated; it understands common abbreviations (like "St." vs. "Street"
  or "Rte." vs. "Route") and gives a very high score if route numbers match (e.g.,
  "US 9" and "Route 9"), as this is a strong indicator of a correct match.

- **Contextual Clues:** The system also checks for consistency to avoid
  illogical matches. For example, if a NAACC survey was conducted on what was
  clearly a hiking trail, a potential match to a major paved highway nearby would
  be heavily penalized, making it an unlikely choice. This was a critical
  adjustment, as an initial challenge was preventing the numerous crossings on
  trails from being incorrectly “snapped” to more prominent, but incorrect,
  roadways.

### **Step 5: Finalizing the Matches**

Finally,
a multi-tiered decision process selects the single best match for each NAACC crossing.

1. First, the system identifies and locks in the most unambiguous,
   high-confidence matches. These are typically cases where a single, high-quality
   intersection match exists with a very close distance, a strong road name
   similarity, and no other competing candidates nearby.

2. After these "best-of-the-best" matches are secured, the system moves on to
   the remaining crossings. For these, it simply selects the candidate—whether an
   intersection or a road segment—that received the best overall score from Step 4.

This tiered approach ensures that we are highly confident in our best matches while still providing a logical and data-driven placement for every single crossing in the NAACC dataset.

### **4. The Final Output**

The result of this comprehensive workflow is a single,
unified map layer named `osm_ways_x_nhd_flowlines_x_naacc_crossings`.
Each point on this map represents a real-world,
ground-verified NAACC crossing that has been precisely located on the digital road network.
Furthermore,
each point is enriched with valuable information from all three source datasets.
The following section provides a detailed data dictionary that describes each field available in this final output layer.

### **5. Data Dictionary for Final Output**

This data dictionary describes the fields (columns) in the final `osm_ways_x_nhd_flowlines_x_naacc_crossings` output layer.

#### **Match Quality and Provenance**

| Field Name               | Description                                                                                                                                                      | Source Dataset |
| :----------------------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------- | :------------- |
| `geom`                   | The final, calculated geographic point location of the crossing on the road network.                                                                             | Fused          |
| `match_confidence_score` | A score from 0 to 100 indicating the confidence in the match. Higher scores represent more confident matches based on distance, name similarity, and match type. | Fused          |
| `match_distance_m`       | The distance in meters between the original NAACC survey point and its final matched location on the road network.                                               | Fused          |
| `match_reason`           | A brief text description of the logic used to select the final match (e.g., "High confidence decision" or "Min match rank").                                     | Fused          |
| `match_type`             | The strategy used for the match: 'road_flowline_intersection_match' (preferred) or 'shortest_line_to_road_span' (fallback).                                      | Fused          |

#### **Road and Waterway Names**

| Field Name          | Description                                                                                                 | Source Dataset |
| :------------------ | :---------------------------------------------------------------------------------------------------------- | :------------- |
| `osm_road_name`     | The official or common name of the road from the OpenStreetMap data (e.g., "US Route 9W" or "Main Street"). | OSM            |
| `naacc_road_name`   | The name of the road as recorded by the field surveyor.                                                     | NAACC          |
| `nhd_waterway_name` | The official name of the stream or river from the National Hydrography Dataset, if available.               | NHDPlus        |
| `naacc_stream_name` | The name of the stream as recorded by the field surveyor.                                                   | NAACC          |

#### **NAACC Crossing Attributes**

| Field Name                    | Description                                                                                              | Source Dataset |
| :---------------------------- | :------------------------------------------------------------------------------------------------------- | :------------- |
| `naacc_crossing_code`         | The unique, persistent identifier for the crossing from the NAACC database.                              | NAACC          |
| `naacc_crossing_type`         | The type of crossing structure recorded by the surveyor (e.g., 'Bridge', 'Culvert', 'Multiple Culvert'). | NAACC          |
| `naacc_inlet_structure_type`  | The shape and material of the structure's inlet (e.g., 'Round Culvert', 'Box Culvert').                  | NAACC          |
| `naacc_outlet_structure_type` | The shape and material of the structure's outlet.                                                        | NAACC          |
| `naacc_location_description`  | Text notes from the surveyor describing the location of the crossing.                                    | NAACC          |
| `naacc_crossing_comment`      | General comments about the overall crossing from the surveyor.                                           | NAACC          |
| `naacc_structure_comment`     | Specific comments about the physical structure(s) from the surveyor.                                     | NAACC          |

#### **NHDPlus Waterway Attributes**

| Field Name                                         | Description                                                                                                                                         | Source Dataset |
| :------------------------------------------------- | :-------------------------------------------------------------------------------------------------------------------------------------------------- | :------------- |
| `nhd_flowline_permanent_identifier`                | The unique identifier for the NHDPlus flowline that the crossing is on.                                                                             | NHDPlus        |
| `nhd_waterway_type_description`                    | A human-readable description of the waterway type (e.g., 'StreamRiver', 'CanalDitch').                                                              | NHDPlus        |
| `nhd_stream_type_description`                      | A more detailed description of the stream, indicating if it is perennial (flows year-round), intermittent, or ephemeral.                            | NHDPlus        |
| `nhd_mean_annual_gage_adjusted_flow_cu_ft_per_sec` | The estimated mean annual flow of water in the stream, measured in cubic feet per second. This provides a measure of the waterway's size and power. | NHDPlus        |

#### **Internal Identifiers and Debugging Fields**

| Field Name                                 | Description                                                                                                        | Source Dataset |
| :----------------------------------------- | :----------------------------------------------------------------------------------------------------------------- | :------------- |
| `osm_road_u`, `osm_road_v`, `osm_road_key` | Internal identifiers that uniquely define the road segment in the OpenStreetMap network graph.                     | OSM            |
| `osm_road_span_type`                       | Indicates if the matched road segment was classified as a 'BRIDGE' or 'NONBRIDGE' span.                            | Fused          |
| `match_score`                              | The raw internal score calculated for the match (lower is better). Used to determine the `match_confidence_score`. | Fused          |
| `match_road_name_similarity_score`         | The raw similarity score (0-100) between the OSM and NAACC road names.                                             | Fused          |
