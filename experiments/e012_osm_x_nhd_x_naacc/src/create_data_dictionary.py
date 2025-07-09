import csv


def create_data_dictionary_csv():
    """
    Generates a CSV file containing the data dictionary for the final output layer.

    The data is hardcoded within this function and written to a file named
    'data_dictionary.csv'.
    """
    # The data dictionary, structured as a list of dictionaries.
    # The order of fields matches the output of `ogrinfo`.
    data_dictionary = [
        {
            "Category": "Road and Waterway Names",
            "Field Name": "osm_road_name",
            "Data Type": "String",
            "Description": 'The official or common name of the road from the OpenStreetMap data (e.g., "US Route 9W" or "Main Street").',
            "Source Dataset": "OSM",
        },
        {
            "Category": "Internal Identifiers and Debugging Fields",
            "Field Name": "osm_from_name",
            "Data Type": "String",
            "Description": "The name of the road from which the current road segment originates.",
            "Source Dataset": "OSM",
        },
        {
            "Category": "Internal Identifiers and Debugging Fields",
            "Field Name": "osm_to_name",
            "Data Type": "String",
            "Description": "The name of the road to which the current road segment connects.",
            "Source Dataset": "OSM",
        },
        {
            "Category": "Road and Waterway Names",
            "Field Name": "naacc_road_name",
            "Data Type": "String",
            "Description": "The name of the road as recorded by the field surveyor.",
            "Source Dataset": "NAACC",
        },
        {
            "Category": "NAACC Crossing Attributes",
            "Field Name": "naacc_crossing_comment",
            "Data Type": "String",
            "Description": "General comments about the overall crossing from the surveyor.",
            "Source Dataset": "NAACC",
        },
        {
            "Category": "NAACC Crossing Attributes",
            "Field Name": "naacc_location_description",
            "Data Type": "String",
            "Description": "Text notes from the surveyor describing the location of the crossing.",
            "Source Dataset": "NAACC",
        },
        {
            "Category": "Road and Waterway Names",
            "Field Name": "naacc_stream_name",
            "Data Type": "String",
            "Description": "The name of the stream as recorded by the field surveyor.",
            "Source Dataset": "NAACC",
        },
        {
            "Category": "Road and Waterway Names",
            "Field Name": "nhd_waterway_name",
            "Data Type": "String",
            "Description": "The official name of the stream or river from the National Hydrography Dataset, if available.",
            "Source Dataset": "NHDPlus",
        },
        {
            "Category": "Match Quality and Provenance",
            "Field Name": "match_distance_m",
            "Data Type": "Real",
            "Description": "The distance in meters between the original NAACC survey point and its final matched location on the road network.",
            "Source Dataset": "Fused",
        },
        {
            "Category": "Match Quality and Provenance",
            "Field Name": "match_confidence_score",
            "Data Type": "Real",
            "Description": "A score from 0 to 100 indicating the confidence in the match. Higher scores represent more confident matches based on distance, name similarity, and match type.",
            "Source Dataset": "Fused",
        },
        {
            "Category": "NAACC Crossing Attributes",
            "Field Name": "naacc_crossing_code",
            "Data Type": "String",
            "Description": "The unique, persistent identifier for the crossing from the NAACC database.",
            "Source Dataset": "NAACC",
        },
        {
            "Category": "NAACC Crossing Attributes",
            "Field Name": "naacc_crossing_type",
            "Data Type": "String",
            "Description": "The type of crossing structure recorded by the surveyor (e.g., 'Bridge', 'Culvert', 'Multiple Culvert').",
            "Source Dataset": "NAACC",
        },
        {
            "Category": "NAACC Crossing Attributes",
            "Field Name": "naacc_inlet_structure_type",
            "Data Type": "String",
            "Description": "The shape and material of the structure's inlet (e.g., 'Round Culvert', 'Box Culvert').",
            "Source Dataset": "NAACC",
        },
        {
            "Category": "NAACC Crossing Attributes",
            "Field Name": "naacc_outlet_structure_type",
            "Data Type": "String",
            "Description": "The shape and material of the structure's outlet.",
            "Source Dataset": "NAACC",
        },
        {
            "Category": "NAACC Crossing Attributes",
            "Field Name": "naacc_structure_comment",
            "Data Type": "String",
            "Description": "Specific comments about the physical structure(s) from the surveyor.",
            "Source Dataset": "NAACC",
        },
        {
            "Category": "NHDPlus Waterway Attributes",
            "Field Name": "nhd_flowline_permanent_identifier",
            "Data Type": "String",
            "Description": "The unique identifier for the NHDPlus flowline that the crossing is on.",
            "Source Dataset": "NHDPlus",
        },
        {
            "Category": "NHDPlus Waterway Attributes",
            "Field Name": "nhd_waterway_type",
            "Data Type": "Integer",
            "Description": "A numeric code representing the general category of the hydrographic feature (e.g., 460 for Stream/River).",
            "Source Dataset": "NHDPlus",
        },
        {
            "Category": "NHDPlus Waterway Attributes",
            "Field Name": "nhd_waterway_type_description",
            "Data Type": "String",
            "Description": "A human-readable description of the waterway type (e.g., 'StreamRiver', 'CanalDitch').",
            "Source Dataset": "NHDPlus",
        },
        {
            "Category": "NHDPlus Waterway Attributes",
            "Field Name": "nhd_stream_type_description",
            "Data Type": "String",
            "Description": "A more detailed description of the stream, indicating if it is perennial (flows year-round), intermittent, or ephemeral.",
            "Source Dataset": "NHDPlus",
        },
        {
            "Category": "NHDPlus Waterway Attributes",
            "Field Name": "nhd_mean_annual_gage_adjusted_flow_cu_ft_per_sec",
            "Data Type": "Real",
            "Description": "The estimated mean annual flow of water in the stream, measured in cubic feet per second. This provides a measure of the waterway's size and power.",
            "Source Dataset": "NHDPlus",
        },
        {
            "Category": "Internal Identifiers and Debugging Fields",
            "Field Name": "match_score",
            "Data Type": "Real",
            "Description": "The raw internal score calculated for the match (lower is better). Used to determine the `match_confidence_score`.",
            "Source Dataset": "Fused",
        },
        {
            "Category": "Internal Identifiers and Debugging Fields",
            "Field Name": "osm_road_name_normalized",
            "Data Type": "String",
            "Description": "The normalized version of the OSM road name used for similarity comparison.",
            "Source Dataset": "Fused",
        },
        {
            "Category": "Internal Identifiers and Debugging Fields",
            "Field Name": "naacc_road_name_normalized",
            "Data Type": "String",
            "Description": "The normalized version of the NAACC road name used for similarity comparison.",
            "Source Dataset": "Fused",
        },
        {
            "Category": "Internal Identifiers and Debugging Fields",
            "Field Name": "match_road_name_similarity_score",
            "Data Type": "Real",
            "Description": "The raw similarity score (0-100) between the OSM and NAACC road names.",
            "Source Dataset": "Fused",
        },
        {
            "Category": "Match Quality and Provenance",
            "Field Name": "match_type",
            "Data Type": "String",
            "Description": "The strategy used for the match: 'road_flowline_intersection_match' (preferred) or 'shortest_line_to_road_span' (fallback).",
            "Source Dataset": "Fused",
        },
        {
            "Category": "Internal Identifiers and Debugging Fields",
            "Field Name": "osm_road_class",
            "Data Type": "Integer64",
            "Description": "Numeric road classification based on the OpenLR standard, where lower numbers indicate higher-order roads.",
            "Source Dataset": "OSM",
        },
        {
            "Category": "Internal Identifiers and Debugging Fields",
            "Field Name": "osm_road_type",
            "Data Type": "String",
            "Description": "The highway type string corresponding to the primary road class.",
            "Source Dataset": "OSM",
        },
        {
            "Category": "Internal Identifiers and Debugging Fields",
            "Field Name": "osm_is_roadway",
            "Data Type": "Boolean",
            "Description": "A boolean flag indicating if the OSM way is a standard roadway (not a service road, trail, etc.).",
            "Source Dataset": "Fused",
        },
        {
            "Category": "Internal Identifiers and Debugging Fields",
            "Field Name": "osm_is_service_road",
            "Data Type": "Boolean",
            "Description": "A boolean flag indicating if the OSM way is a service road.",
            "Source Dataset": "Fused",
        },
        {
            "Category": "Internal Identifiers and Debugging Fields",
            "Field Name": "naacc_is_trail",
            "Data Type": "Boolean",
            "Description": "A boolean flag indicating if the NAACC crossing is on a trail.",
            "Source Dataset": "Fused",
        },
        {
            "Category": "Internal Identifiers and Debugging Fields",
            "Field Name": "naacc_is_unnamed_road",
            "Data Type": "Boolean",
            "Description": "A boolean flag indicating if the NAACC road is unnamed.",
            "Source Dataset": "Fused",
        },
        {
            "Category": "Internal Identifiers and Debugging Fields",
            "Field Name": "naacc_is_driveway",
            "Data Type": "Boolean",
            "Description": "A boolean flag indicating if the NAACC crossing is on a driveway.",
            "Source Dataset": "Fused",
        },
        {
            "Category": "Internal Identifiers and Debugging Fields",
            "Field Name": "match_has_inconsistent_road_types",
            "Data Type": "Boolean",
            "Description": "A boolean flag indicating if there is a significant inconsistency between the OSM and NAACC road types (e.g., a roadway matched to a trail).",
            "Source Dataset": "Fused",
        },
        {
            "Category": "Match Quality and Provenance",
            "Field Name": "match_reason",
            "Data Type": "String",
            "Description": 'A brief text description of the logic used to select the final match (e.g., "High confidence decision" or "Min match rank").',
            "Source Dataset": "Fused",
        },
        {
            "Category": "Internal Identifiers and Debugging Fields",
            "Field Name": "osm_road_u",
            "Data Type": "Integer64",
            "Description": "Internal identifier for the starting node of the road segment in the OpenStreetMap network graph.",
            "Source Dataset": "OSM",
        },
        {
            "Category": "Internal Identifiers and Debugging Fields",
            "Field Name": "osm_road_v",
            "Data Type": "Integer64",
            "Description": "Internal identifier for the ending node of the road segment in the OpenStreetMap network graph.",
            "Source Dataset": "OSM",
        },
        {
            "Category": "Internal Identifiers and Debugging Fields",
            "Field Name": "osm_road_key",
            "Data Type": "Integer64",
            "Description": "Internal identifier for parallel edges between the same two nodes in the OpenStreetMap network graph.",
            "Source Dataset": "OSM",
        },
        {
            "Category": "Internal Identifiers and Debugging Fields",
            "Field Name": "osm_road_span_type",
            "Data Type": "String",
            "Description": "Indicates if the matched road segment was classified as a 'BRIDGE' or 'NONBRIDGE' span.",
            "Source Dataset": "Fused",
        },
        {
            "Category": "Internal Identifiers and Debugging Fields",
            "Field Name": "osm_road_span_idx",
            "Data Type": "Integer64",
            "Description": "The index of the road span within a larger road segment.",
            "Source Dataset": "Fused",
        },
        {
            "Category": "Internal Identifiers and Debugging Fields",
            "Field Name": "osm_road_span_nhd_flowline_intxn_idx",
            "Data Type": "Integer64",
            "Description": "The index of the intersection within a road span, for cases where one span has multiple intersections.",
            "Source Dataset": "Fused",
        },
    ]

    # Define the output filename.
    output_filename = "data_dictionary.csv"

    # Define the headers for the CSV file.
    headers = ["Field Name", "Data Type", "Category", "Description", "Source Dataset"]

    try:
        # Open the file in write mode with newline='' to prevent extra blank rows.
        with open(output_filename, "w", newline="", encoding="utf-8") as csvfile:
            # Create a DictWriter object to write dictionaries to CSV.
            writer = csv.DictWriter(csvfile, fieldnames=headers)

            # Write the header row.
            writer.writeheader()

            # Write the data rows.
            writer.writerows(data_dictionary)

        print(f"Successfully created '{output_filename}'")

    except IOError as e:
        print(f"Error writing to file: {e}")


if __name__ == "__main__":
    create_data_dictionary_csv()
