import csv

filename = "e009_001_ris_conflation_data_dictionary.csv"

data_dict = {
    "u": {
        "data_type": "int",
        "description": "Start node ID (OSMnx simplified graph) for the road segment.",
    },
    "v": {
        "data_type": "int",
        "description": "End node ID (OSMnx simplified graph) for the road segment.",
    },
    "key": {
        "data_type": "int",
        "description": "Edge key (OSMnx, usually 0 after simplification).",
    },
    "geometry": {
        "data_type": "geometry",
        "description": "Linestring geometry of the simplified OSM road segment (CRS: EPSG:4326).",
    },
    "_idx_": {
        "data_type": "int",
        "description": "Internal row identifier from intermediate processing.",
    },
    "ris_route_id": {
        "data_type": "varchar",
        "description": "The NYSDOT RIS `ROUTE_ID` matched to this OSM segment.",
    },
    "ris_min_from_mi": {
        "data_type": "double",
        "description": "Estimated starting milepoint on the `route_id` corresponding to this OSM segment's matched portion.",
    },
    "ris_max_to_mi": {
        "data_type": "double",
        "description": "Estimated ending milepoint on the `route_id` corresponding to this OSM segment's matched portion.",
    },
    "ris_fclass_overlap_dist_mi": {
        "data_type": "double",
        "description": "Length (miles) of overlap between the matched milepoint range and the selected Functional Class record's range.",
    },
    "ris_fclass_nysdot_fclass": {
        "data_type": "int64",
        "description": "NYSDOT Functional Classification code. Selected based on maximum overlap. Codes:\n- `01`: Rural - Principal Arterial - Interstate\n- `02`: Rural - Principal Arterial - Other Freeway/Expressway\n- `04`: Rural - Principal Arterial - Other\n- `06`: Rural - Minor Arterial\n- `07`: Rural - Major Collector\n- `08`: Rural - Minor Collector\n- `09`: Rural - Local\n- `11`: Urban - Principal Arterial - Interstate\n- `12`: Urban - Principal Arterial - Other Freeway/Expressway\n- `14`: Urban - Principal Arterial - Other\n- `16`: Urban - Minor Arterial\n- `17`: Urban - Major Collector\n- `18`: Urban - Minor Collector\n- `19`: Urban - Local\n- `00`: Not a Highway",
    },
    "ris_fclass_fhwa_fclass": {
        "data_type": "int64",
        "description": "Derived FHWA Functional Classification code. Mapping from NYSDOT code: 1->1, 2->2, 4->3, 6->4, 7->5, 8->6, 9->7. Selected based on maximum overlap.",
    },
    "ris_fclass_federal_aid_flag": {
        "data_type": "varchar",
        "description": "Federal Aid system relationship associated with the functional class. (Details in RIS documentation).",
    },
    "ris_fclass_class_hierarchy": {
        "data_type": "varchar",
        "description": "Functional class hierarchy level. (Details in RIS documentation).",
    },
    "ris_strahnet_overlap_dist_mi": {
        "data_type": "double",
        "description": "Overlap distance (miles) with the selected STRAHNET record.",
    },
    "ris_strahnet_code": {
        "data_type": "varchar",
        "description": "Strategic Highway Network (STRAHNET) code. Selected based on maximum overlap. Codes:\n- `0`: Not a STRAHNET route\n- `1`: Interstate STRAHNET route\n- `2`: Non-Interstate STRAHNET route\n- `3`: STRAHNET Connector route\n- `N`: Not a highway",
    },
    "ris_strahnet_description": {
        "data_type": "varchar",
        "description": "Text description of the STRAHNET code.",
    },
    "ris_trk_rte_overlap_dist_mi": {
        "data_type": "double",
        "description": "Overlap distance (miles) with the selected Truck Route record.",
    },
    "ris_trk_rte_code": {
        "data_type": "varchar",
        "description": "Truck Route designation code. Selected based on maximum overlap. (Codes defined in RIS Domain Dictionary).",
    },
    "ris_trk_rte_code_description": {
        "data_type": "varchar",
        "description": "Text description of the Truck Route code.",
    },
    "ris_bridge_overlap_dist_mi": {
        "data_type": "double",
        "description": "Overlap distance (miles) with the selected Bridge Inventory record.",
    },
    "ris_bridge_bin": {
        "data_type": "varchar",
        "description": "Bridge Identification Number (BIN) of the overlapping bridge structure. Selected based on lowest condition rating.",
    },
    "ris_nysdot_bridge_carried": {
        "data_type": "varchar",
        "description": "Feature carried by the bridge. (From NYSDOT Bridges dataset).",
    },
    "ris_nysdot_bridge_crossed": {
        "data_type": "varchar",
        "description": "Feature crossed by the bridge. (From NYSDOT Bridges dataset).",
    },
    "ris_nysdot_bridge_primary_owner": {
        "data_type": "varchar",
        "description": "Primary owner code. (Codes: 10=NYSDOT, 30=County, 40=Town, 41=Village, 42=City, etc.). (From NYSDOT Bridges dataset).",
    },
    "ris_nysdot_bridge_primary_maintainer": {
        "data_type": "varchar",
        "description": "Primary maintaining agency code. (Codes same as owner). (From NYSDOT Bridges dataset).",
    },
    "ris_nysdot_bridge_gtms_structure": {
        "data_type": "varchar",
        "description": "Bridge General Type Main Span structure code (e.g., 01:Slab, 02:Stringer, 03:Girder/Floorbeam, 05:Box Beam Multiple, 06:Box Beam Single, 09:Truss-Deck, 10:Truss-Thru, 11:Arch-Deck, 19:Culvert). (From NYSDOT Bridges dataset).",
    },
    "ris_nysdot_bridge_gtms_material": {
        "data_type": "varchar",
        "description": "Bridge General Type Main Span material code (e.g., 1:Concrete, 3:Steel, 5:Prestressed Concrete, 7:Timber). (From NYSDOT Bridges dataset).",
    },
    "ris_nysdot_bridge_number_of_spans": {
        "data_type": "int",
        "description": "Number of spans in the bridge. (From NYSDOT Bridges dataset).",
    },
    "ris_nysdot_bridge_condition_rating": {
        "data_type": "float",
        "description": "NYSDOT bridge condition rating (1.0 - 7.0 scale, lower=worse). (From NYSDOT Bridges dataset).",
    },
    "ris_nysdot_bridge_bridge_length": {
        "data_type": "float",
        "description": "Total length of the bridge structure (feet). (From NYSDOT Bridges dataset).",
    },
    "ris_nysdot_bridge_deck_area_sq_ft": {
        "data_type": "float",
        "description": "Bridge deck area (square feet). (From NYSDOT Bridges dataset).",
    },
    "ris_nysdot_bridge_aadt": {
        "data_type": "int",
        "description": "Average Annual Daily Traffic reported for the bridge structure. (From NYSDOT Bridges dataset).",
    },
    "ris_nysdot_bridge_year_built": {
        "data_type": "int",
        "description": "Year bridge was built. (From NYSDOT Bridges dataset).",
    },
    "ris_nysdot_bridge_posted_load": {
        "data_type": "float",
        "description": "Posted load limit (tons) or code (88=R-Permit Restricted, 98=Closed Construction/Seasonal, 99=Closed). (From NYSDOT Bridges dataset).",
    },
    "ris_nysdot_bridge_nbi_deck_condition": {
        "data_type": "varchar",
        "description": "NBI Deck Condition Code (0-9, N). (From NYSDOT Bridges dataset).",
    },
    "ris_nysdot_bridge_nbi_substructure_condition": {
        "data_type": "varchar",
        "description": "NBI Substructure Condition Code (0-9, N). (From NYSDOT Bridges dataset).",
    },
    "ris_nysdot_bridge_nbi_superstructure_condition": {
        "data_type": "varchar",
        "description": "NBI Superstructure Condition Code (0-9, N). (From NYSDOT Bridges dataset).",
    },
    "ris_nysdot_bridge_fhwa_condition": {
        "data_type": "varchar",
        "description": "Overall FHWA Condition ('Good' >= 7, 'Fair' = 5 or 6, 'Poor' <= 4) based on NBI ratings. (From NYSDOT Bridges dataset).",
    },
    "ris_large_culvert_along_mi": {
        "data_type": "double",
        "description": "Milepoint location of the overlapping large culvert structure. (From RIS `Ev_Str_LargeCulvert`).",
    },
    "ris_large_culvert_cin": {
        "data_type": "varchar",
        "description": "Culvert Identification Number (CIN) of the overlapping large culvert. Selected based on lowest condition rating.",
    },
    "ris_nysdot_large_culvert_crossed": {
        "data_type": "varchar",
        "description": "Feature crossed by the large culvert. (From NYSDOT Large Culverts dataset).",
    },
    "ris_nysdot_large_culvert_primary_owner": {
        "data_type": "varchar",
        "description": "Primary owner code. (From NYSDOT Large Culverts dataset).",
    },
    "ris_nysdot_large_culvert_primary_maintainer": {
        "data_type": "varchar",
        "description": "Primary maintaining agency code. (From NYSDOT Large Culverts dataset).",
    },
    "ris_nysdot_large_culvert_gtms_structure": {
        "data_type": "varchar",
        "description": "Structure type code (e.g., 19, 07). (From NYSDOT Large Culverts dataset).",
    },
    "ris_nysdot_large_culvert_gtms_material": {
        "data_type": "varchar",
        "description": "Material type code. (From NYSDOT Large Culverts dataset).",
    },
    "ris_nysdot_large_culvert_number_of_spans": {
        "data_type": "int",
        "description": "Number of spans/barrels. (From NYSDOT Large Culverts dataset).",
    },
    "ris_nysdot_large_culvert_condition_rating": {
        "data_type": "float",
        "description": "NYSDOT condition rating (1.0 - 7.0 scale). (From NYSDOT Large Culverts dataset).",
    },
    "ris_nysdot_large_culvert_type_max_span": {
        "data_type": "varchar",
        "description": "Code for type of maximum span. (Details likely in BDIS documentation).",
    },
    "ris_nysdot_large_culvert_year_built": {
        "data_type": "int",
        "description": "Year built. (From NYSDOT Large Culverts dataset).",
    },
    "ris_nysdot_large_culvert_abutment_type": {
        "data_type": "varchar",
        "description": "Abutment type code. (From NYSDOT Large Culverts dataset).",
    },
    "ris_nysdot_large_culvert_stream_bed_material": {
        "data_type": "varchar",
        "description": "Stream bed material code. (From NYSDOT Large Culverts dataset).",
    },
    "ris_nysdot_large_culvert_maintenance_responsibility_primary": {
        "data_type": "varchar",
        "description": "Primary maintenance agency code. (From NYSDOT Large Culverts dataset).",
    },
    "ris_nysdot_large_culvert_abutment_height": {
        "data_type": "float",
        "description": "Abutment height (feet). (From NYSDOT Large Culverts dataset).",
    },
    "ris_nysdot_large_culvert_culvert_skew": {
        "data_type": "int",
        "description": "Skew angle (degrees). (From NYSDOT Large Culverts dataset).",
    },
    "ris_nysdot_large_culvert_out_to_out_width": {
        "data_type": "float",
        "description": "Out-to-out width (feet). (From NYSDOT Large Culverts dataset).",
    },
    "ris_nysdot_large_culvert_span_length": {
        "data_type": "float",
        "description": "Span/barrel length (feet). (From NYSDOT Large Culverts dataset).",
    },
    "ris_nysdot_large_culvert_structure_length": {
        "data_type": "float",
        "description": "Total structure length along roadway (feet). (From NYSDOT Large Culverts dataset).",
    },
    "ris_nysdot_large_culvert_general_recommendation": {
        "data_type": "varchar",
        "description": "General recommendation code (1-7 scale). (From NYSDOT Large Culverts dataset).",
    },
    "ris_nysdot_large_culvert_regional_economic_development_council": {
        "data_type": "varchar",
        "description": "REDC code. (From NYSDOT Large Culverts dataset).",
    },
    "ris_counts_overlap_dist_mi": {
        "data_type": "double",
        "description": "Overlap distance (miles) with the selected Traffic Count record.",
    },
    "ris_counts_federal_direction": {
        "data_type": "int64",
        "description": "Federal direction code for count (1=N, 3=E, 5=S, 7=W, 9=N/S, 0=E/W). Selected based on maximum overlap.",
    },
    "ris_counts_full_count": {
        "data_type": "varchar",
        "description": "'Y' if count covers full roadway width, blank if directional.",
    },
    "ris_counts_oneway_flag": {
        "data_type": "varchar",
        "description": "'Y' if segment is one-way.",
    },
    "ris_counts_calculation_year": {
        "data_type": "int64",
        "description": "Year the AADT/DHV statistics apply to.",
    },
    "ris_counts_aadt": {
        "data_type": "int64",
        "description": "Annual Average Daily Traffic.",
    },
    "ris_counts_dhv": {
        "data_type": "int64",
        "description": "Design Hour Volume (30th highest hour).",
    },
    "ris_counts_ddhv": {
        "data_type": "int64",
        "description": "Directional Design Hour Volume.",
    },
    "ris_counts_su_aadt": {
        "data_type": "int64",
        "description": "Single Unit vehicle (F4-F7) AADT.",
    },
    "ris_counts_cu_aadt": {
        "data_type": "int64",
        "description": "Combination Unit vehicle (F8-F13) AADT.",
    },
    "ris_counts_k_factor": {
        "data_type": "double",
        "description": "Ratio of DHV to AADT (%).",
    },
    "ris_counts_d_factor": {
        "data_type": "double",
        "description": "Ratio of peak direction volume to total volume during design hour (%).",
    },
    "ris_counts_avg_truck_percent": {
        "data_type": "double",
        "description": "Avg Weekday Truck % (F4-F13).",
    },
    "ris_counts_avg_su_percent": {
        "data_type": "double",
        "description": "Avg Weekday Single Unit % (F4-F7).",
    },
    "ris_counts_avg_cu_percent": {
        "data_type": "double",
        "description": "Avg Weekday Combination Unit % (F8-F13).",
    },
    "ris_counts_avg_motorcycle_percent": {
        "data_type": "double",
        "description": "Avg Weekday Motorcycle % (F1).",
    },
    "ris_counts_avg_car_percent": {
        "data_type": "double",
        "description": "Avg Weekday Passenger Car % (F2).",
    },
    "ris_counts_avg_light_truck_percent": {
        "data_type": "double",
        "description": "Avg Weekday Light Truck % (F3).",
    },
    "ris_counts_avg_bus_percent": {
        "data_type": "double",
        "description": "Avg Weekday Bus % (F4).",
    },
    "ris_counts_avg_weekday_f5_7": {
        "data_type": "double",
        "description": "Avg Weekday Single Unit Truck % (F5-F7).",
    },
    "ris_counts_axle_factor": {
        "data_type": "double",
        "description": "Axle correction factor.",
    },
    "ris_counts_su_peak": {
        "data_type": "double",
        "description": "Peak hour SU volume as % of AADT.",
    },
    "ris_counts_cu_peak": {
        "data_type": "double",
        "description": "Peak hour CU volume as % of AADT.",
    },
    "ris_counts_avg_k_factor": {
        "data_type": "double",
        "description": "Average K Factor for the factor group.",
    },
    "ris_counts_avg_d_factor": {
        "data_type": "double",
        "description": "Average D Factor for the factor group.",
    },
    "ris_counts_truck_aadt": {
        "data_type": "int64",
        "description": "Truck AADT (F4-F13).",
    },
    "ris_counts_morning_highest_value": {
        "data_type": "int64",
        "description": "Highest hourly volume in morning.",
    },
    "ris_counts_afternoon_highest_value": {
        "data_type": "int64",
        "description": "Highest hourly volume in afternoon.",
    },
    "ris_counts_evening_highest_value": {
        "data_type": "int64",
        "description": "Highest hourly volume in evening.",
    },
}


def main():
    # Define the header row for the CSV
    header = ["Column Name", "Data Type", "Description"]

    # Open the CSV file for writing
    # 'newline=''' is important to prevent extra blank rows
    # 'encoding='utf-8'' handles potential special characters in descriptions
    with open(filename, "w", newline="", encoding="utf-8") as csvfile:
        # Create a CSV writer object
        csv_writer = csv.writer(csvfile)

        # Write the header row
        csv_writer.writerow(header)

        # Iterate through the dictionary and write each column's info
        # Sort by column name for consistent output
        for column_name, column_info in data_dict.items():
            # Get data type and description, handle potential missing keys gracefully
            data_type = column_info.get("data_type", "N/A")
            description = column_info.get("description", "")

            # Write the row to the CSV
            csv_writer.writerow([column_name, data_type, description])

    print(f"Data dictionary successfully written to {filename}")


# This block will only run when the script is executed directly
if __name__ == "__main__":
    # Call the function to write the dictionary to a CSV
    main()
