import csv
import os


def create_data_dictionaries():
    """
    Generates two CSV files containing the detailed data dictionaries for the
    complete and simplified road network resilience analysis outputs.
    """

    # --- Data for the Complete Fused Output ---
    full_data_dictionary_data = [
        # Base Road Network Attributes (e000)
        {
            "Column Name": "u",
            "Data Type": "Integer64",
            "Description": "The starting node identifier for the road segment from the OSMnx graph.",
        },
        {
            "Column Name": "v",
            "Data Type": "Integer64",
            "Description": "The ending node identifier for the road segment from the OSMnx graph.",
        },
        {
            "Column Name": "key",
            "Data Type": "Integer64",
            "Description": "The key differentiating multiple edges between the same u and v nodes.",
        },
        {
            "Column Name": "e000_access",
            "Data Type": "String",
            "Description": "OSM tag describing access restrictions for the road segment.",
        },
        {
            "Column Name": "e000_highway",
            "Data Type": "String",
            "Description": "The primary OSM highway classification tag (e.g., motorway, primary, residential).",
        },
        {
            "Column Name": "e000_lanes",
            "Data Type": "String",
            "Description": "The number of traffic lanes on the road segment.",
        },
        {
            "Column Name": "e000_maxspeed",
            "Data Type": "String",
            "Description": "The posted speed limit for the road segment.",
        },
        {
            "Column Name": "e000_name",
            "Data Type": "String",
            "Description": "The official or common name of the road.",
        },
        {
            "Column Name": "e000_oneway",
            "Data Type": "String",
            "Description": "Indicates if the road segment is one-way.",
        },
        {
            "Column Name": "e000_surface",
            "Data Type": "String",
            "Description": "The surface material of the road (e.g., asphalt, gravel).",
        },
        {
            "Column Name": "e000_length_mi",
            "Data Type": "Real",
            "Description": "The length of the road segment in miles.",
        },
        {
            "Column Name": "e000_road_name",
            "Data Type": "String",
            "Description": "A cleaned-up version of the road name.",
        },
        {
            "Column Name": "e000_from_name",
            "Data Type": "String",
            "Description": "The name of the intersection at the starting node (u).",
        },
        {
            "Column Name": "e000_to_name",
            "Data Type": "String",
            "Description": "The name of the intersection at the ending node (v).",
        },
        {
            "Column Name": "e000_travel_time",
            "Data Type": "Real",
            "Description": "Estimated travel time in seconds for the road segment based on length and speed limit.",
        },
        # Redundancy Analysis (e001)
        {
            "Column Name": "e001_difference_sec",
            "Data Type": "Real",
            "Description": "The additional travel time (in seconds) incurred by taking a detour if this segment were closed.",
        },
        # Flood Impact Analysis (e004)
        {
            "Column Name": "e004_nonfunctional_reason",
            "Data Type": "String",
            "Description": "Reason the segment is unusable in a flood scenario (e.g., 'FLOODED', 'BRIDGE_FUNCTIONALLY_DISCONNECTED').",
        },
        {
            "Column Name": "e004_nonfunctional_risk_level",
            "Data Type": "Integer64",
            "Description": "The numeric FEMA risk level causing the non-functionality.",
        },
        {
            "Column Name": "e004_nonfunctional_frequency",
            "Data Type": "String",
            "Description": "The flood frequency ('100 YEAR' or '500 YEAR') associated with the non-functionality.",
        },
        {
            "Column Name": "e004_isolated_edge_risk_level",
            "Data Type": "Real",
            "Description": "The numeric FEMA risk level at which a non-flooded segment becomes isolated.",
        },
        {
            "Column Name": "e004_isolated_edge_frequency",
            "Data Type": "String",
            "Description": "The flood frequency ('100 YEAR' or '500 YEAR') causing the isolation.",
        },
        # TRANSCOM Events Analysis (e007)
        {
            "Column Name": "e007_all_events_ids",
            "Data Type": "String",
            "Description": "A list of unique TRANSCOM event IDs that affected this segment.",
        },
        {
            "Column Name": "e007_all_events_count",
            "Data Type": "Integer64",
            "Description": "The total count of unique TRANSCOM events on this segment.",
        },
        {
            "Column Name": "e007_all_incidents_count",
            "Data Type": "Integer64",
            "Description": "The count of distinct, continuous time periods (islands) of disruption on this segment.",
        },
        {
            "Column Name": "e007_all_incidents_total_duration_hours",
            "Data Type": "Real",
            "Description": "Total duration in hours of all disruptive incidents, with overlapping times merged.",
        },
        {
            "Column Name": "e007_road_closed_incidents_count",
            "Data Type": "Integer64",
            "Description": 'Count of distinct "road closed" incidents.',
        },
        {
            "Column Name": "e007_road_closed_total_duration_hours",
            "Data Type": "Real",
            "Description": 'Total duration in hours of "road closed" incidents.',
        },
        {
            "Column Name": "e007_road_flooded_incidents_count",
            "Data Type": "Integer64",
            "Description": 'Count of distinct "road flooded" incidents.',
        },
        {
            "Column Name": "e007_road_flooded_total_duration_hours",
            "Data Type": "Real",
            "Description": 'Total duration in hours of "road flooded" incidents.',
        },
        # Network Centrality Analysis (e008)
        {
            "Column Name": "e008_cbsa_geoid",
            "Data Type": "String",
            "Description": "The GEOID of the Core-Based Statistical Area (CBSA) for which centrality was calculated.",
        },
        {
            "Column Name": "e008_cbsa_name",
            "Data Type": "String",
            "Description": "The name of the CBSA.",
        },
        {
            "Column Name": "e008_cbsa_edge_betweenness_normalized",
            "Data Type": "Real",
            "Description": "The edge betweenness centrality score, normalized within its home CBSA.",
        },
        {
            "Column Name": "e008_all_cbsa_edge_betweenness_rank_normalized",
            "Data Type": "Real",
            "Description": "The rank of the centrality score, normalized across all CBSAs.",
        },
        # RIS Conflation Analysis (e009)
        {
            "Column Name": "e009_ris_counts_aadt",
            "Data Type": "Real",
            "Description": "Annual Average Daily Traffic from NYSDOT RIS.",
        },
        {
            "Column Name": "e009_ris_counts_su_aadt",
            "Data Type": "Real",
            "Description": "Single-Unit truck Annual Average Daily Traffic from NYSDOT RIS.",
        },
        {
            "Column Name": "e009_ris_counts_cu_aadt",
            "Data Type": "Real",
            "Description": "Combination-Unit truck Annual Average Daily Traffic from NYSDOT RIS.",
        },
        {
            "Column Name": "e009_ris_large_culvert_cin",
            "Data Type": "String",
            "Description": "The Culvert Identification Number (CIN) if a large culvert from NYSDOT RIS is on this segment.",
        },
        # Hydrography Analysis (e010)
        {
            "Column Name": "e010_num_total_intersection_points",
            "Data Type": "Integer64",
            "Description": "The total number of times the road segment intersects with a National Hydrography Dataset flowline.",
        },
        # Calculated Scores
        {
            "Column Name": "scores_redundancy_normalized",
            "Data Type": "Real",
            "Description": "Normalized redundancy score (0-10 scale). A higher score indicates lower redundancy (worse).",
        },
        {
            "Column Name": "scores_redundancy_bin_weight",
            "Data Type": "Integer",
            "Description": "Binned redundancy score (1-5). A higher score indicates lower redundancy (worse).",
        },
        {
            "Column Name": "scores_flood_risk",
            "Data Type": "Integer",
            "Description": "Calculated flood risk score based on FEMA data (100-year=2, 500-year=1).",
        },
        {
            "Column Name": "scores_flooding_events_risk",
            "Data Type": "Integer",
            "Description": "Calculated risk score from historical TRANSCOM flooding events (1 event=1, >1 event=2).",
        },
        {
            "Column Name": "scores_centrality_normalized",
            "Data Type": "Real",
            "Description": "The normalized network centrality score, indicating structural importance.",
        },
        {
            "Column Name": "scores_culvert_risk",
            "Data Type": "Integer",
            "Description": "Binary risk score indicating the presence of a large culvert (0 or 1).",
        },
        {
            "Column Name": "scores_hydrography_risk",
            "Data Type": "Integer",
            "Description": "Binary risk score indicating an intersection with a water flowline (0 or 1).",
        },
        {
            "Column Name": "scores_criticality",
            "Data Type": "Real",
            "Description": "Composite score combining centrality and redundancy. Higher score indicates higher criticality.",
        },
        {
            "Column Name": "scores_criticality_v2",
            "Data Type": "Real",
            "Description": "Alternate composite criticality score using binned redundancy weight.",
        },
        {
            "Column Name": "scores_total_risk",
            "Data Type": "Integer",
            "Description": "Composite score summing multiple hazard risk factors (flood, culvert, hydrography, events).",
        },
        {
            "Column Name": "scores_resiliency",
            "Data Type": "Real",
            "Description": "Final inverse resilience score. A higher score indicates a segment is highly critical AND at high risk.",
        },
        {
            "Column Name": "scores_resiliency_v2",
            "Data Type": "Real",
            "Description": "Alternate final inverse resilience score using binned criticality.",
        },
        {
            "Column Name": "geom",
            "Data Type": "Geometry",
            "Description": "The geometry of the road segment.",
        },
    ]

    # --- Data for the Simplified Output ---
    simplified_data_dictionary_data = [
        {
            "Column Name": "u",
            "Data Type": "Integer64",
            "Description": "The unique identifier for the road segment's starting node.",
        },
        {
            "Column Name": "v",
            "Data Type": "Integer64",
            "Description": "The unique identifier for the road segment's ending node.",
        },
        {
            "Column Name": "key",
            "Data Type": "Integer64",
            "Description": "The key for the road segment edge.",
        },
        {
            "Column Name": "openlr_roadclass",
            "Data Type": "Integer64",
            "Description": "The OpenLR road class classification.",
        },
        {
            "Column Name": "road_name",
            "Data Type": "String",
            "Description": "The common name of the road.",
        },
        {
            "Column Name": "from_name",
            "Data Type": "String",
            "Description": "The name of the intersection at the starting node.",
        },
        {
            "Column Name": "to_name",
            "Data Type": "String",
            "Description": "The name of the intersection at the ending node.",
        },
        {
            "Column Name": "length_mi",
            "Data Type": "Real",
            "Description": "The length of the road segment in miles.",
        },
        {
            "Column Name": "utah_redundancy_detour_seconds_diff",
            "Data Type": "Real",
            "Description": "The additional travel time (in seconds) for a detour. Sourced from E001.",
        },
        {
            "Column Name": "intra_cbsa_edge_betweenness_rank",
            "Data Type": "Integer64",
            "Description": "The segment's centrality rank within its specific CBSA. Sourced from E008.",
        },
        {
            "Column Name": "all_cbsa_edge_betweenness_rank",
            "Data Type": "Integer64",
            "Description": "The segment's centrality rank across all CBSAs. Sourced from E008.",
        },
        {
            "Column Name": "e009_ris_counts_aadt",
            "Data Type": "Real",
            "Description": "Annual Average Daily Traffic. Sourced from E009.",
        },
        {
            "Column Name": "e009_ris_counts_su_aadt",
            "Data Type": "Real",
            "Description": "Single-Unit truck Annual Average Daily Traffic. Sourced from E009.",
        },
        {
            "Column Name": "e009_ris_counts_cu_aadt",
            "Data Type": "Real",
            "Description": "Combination-Unit truck Annual Average Daily Traffic. Sourced from E009.",
        },
        {
            "Column Name": "scores_redundancy_normalized",
            "Data Type": "Real",
            "Description": "Normalized redundancy score (0-10 scale).",
        },
        {
            "Column Name": "scores_redundancy_bin_weight",
            "Data Type": "Integer",
            "Description": "Binned redundancy score (1-5).",
        },
        {
            "Column Name": "scores_centrality_normalized",
            "Data Type": "Real",
            "Description": "The normalized network centrality score.",
        },
        {
            "Column Name": "scores_flood_risk",
            "Data Type": "Integer",
            "Description": "Calculated flood risk score.",
        },
        {
            "Column Name": "scores_culvert_risk",
            "Data Type": "Integer",
            "Description": "Binary risk for large culvert presence.",
        },
        {
            "Column Name": "scores_hydrography_risk",
            "Data Type": "Integer",
            "Description": "Binary risk for crossing a water flowline.",
        },
        {
            "Column Name": "scores_criticality",
            "Data Type": "Real",
            "Description": "Composite criticality score.",
        },
        {
            "Column Name": "scores_criticality_v2",
            "Data Type": "Real",
            "Description": "Alternate composite criticality score.",
        },
        {
            "Column Name": "scores_total_risk",
            "Data Type": "Integer",
            "Description": "Composite risk score.",
        },
        {
            "Column Name": "scores_resiliency",
            "Data Type": "Real",
            "Description": "Final inverse resilience score.",
        },
        {
            "Column Name": "scores_resiliency_v2",
            "Data Type": "Real",
            "Description": "Alternate final inverse resilience score.",
        },
        {
            "Column Name": "flooding_vulnerability_frequency",
            "Data Type": "String",
            "Description": "If the segment is vulnerable to flooding, this indicates the frequency ('100 YEAR', '500 YEAR'). Sourced from E004.",
        },
        {
            "Column Name": "crosses_national_hydrography_dataset_flowline",
            "Data Type": "Integer(Boolean)",
            "Description": "A boolean (true/false) flag indicating if the segment crosses a water flowline. Sourced from E010.",
        },
        {
            "Column Name": "geom",
            "Data Type": "Geometry",
            "Description": "The geometry of the road segment.",
        },
    ]

    # --- Write the Full Data Dictionary CSV ---
    full_output_filename = "full_data_dictionary.csv"
    try:
        with open(full_output_filename, "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = ["Column Name", "Data Type", "Description"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for row in full_data_dictionary_data:
                writer.writerow(row)
        print(f"Successfully created '{full_output_filename}'")
    except IOError as e:
        print(f"Error writing to file {full_output_filename}: {e}")

    # --- Write the Simplified Data Dictionary CSV ---
    simplified_output_filename = "simplified_data_dictionary.csv"
    try:
        with open(
            simplified_output_filename, "w", newline="", encoding="utf-8"
        ) as csvfile:
            fieldnames = ["Column Name", "Data Type", "Description"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for row in simplified_data_dictionary_data:
                writer.writerow(row)
        print(f"Successfully created '{simplified_output_filename}'")
    except IOError as e:
        print(f"Error writing to file {simplified_output_filename}: {e}")


if __name__ == "__main__":
    create_data_dictionaries()
