import csv

# Define the data dictionary
data_dictionary = {
    "nonfunctional_reason": {
        "description": (
            "Specifies why a road edge was lost. Possible reasons include: "
            "'NOT_IMPACTED' (the edge was not impacted by flooding), "
            "'FLOODED' (edge inundated by floodwaters), 'BRIDGE_FUNCTIONALLY_DISCONNECTED' (edge from a disconnected bridge)."
        ),
        "data_type": "string",
        "possible_values": "NOT_IMPACTED, FLOODED, BRIDGE_FUNCTIONALLY_DISCONNECTED",
    },
    "nonfunctional_risk_level": {
        "description": (
            "Numeric indicator of the flood risk level associated with the lost road edge. "
            "Lower values indicate higher risk; risk levels are based on the FEMA-based ranking system."
        ),
        "data_type": "integer",
        "possible_values": "Risk level values (1 to 18, where lower is higher risk)",
    },
    "nonfunctional_frequency": {
        "description": (
            "Designation of flood frequency for the lost road edge, determined by its flood zone classification. "
            "If the road edge falls within a flood zone category starting with 'VE', 'V', 'AE', 'AO', 'AH', or 'A', "
            "it is marked as '100 YEAR' (1% annual chance flood). Otherwise, it is marked as '500 YEAR' (0.2% annual chance flood)."
        ),
        "data_type": "string",
        "possible_values": "100 YEAR, 500 YEAR",
    },
    "isolated_edge_risk_level": {
        "description": (
            "Numeric indicator of the flood risk level assigned to the road edge that became isolated. "
            "Lower values indicate higher risk; risk levels are based on the FEMA-based ranking system."
        ),
        "data_type": "integer",
        "possible_values": "Risk level values (1 to 18, where lower is higher risk)",
    },
    "isolated_edge_frequency": {
        "description": (
            "Designation of flood frequency for the isolated road edge, determined by its flood zone classification. "
            "If the road edge falls within a flood zone category starting with 'VE', 'V', 'AE', 'AO', 'AH', or 'A', "
            "it is marked as '100 YEAR' (1% annual chance flood). Otherwise, it is marked as '500 YEAR' (0.2% annual chance flood)."
        ),
        "data_type": "string",
        "possible_values": "100 YEAR, 500 YEAR",
    },
}

# Specify CSV file name
csv_file_name = "e004_flooding_impact.data_dictionary.csv"

# Write the data dictionary to CSV
with open(csv_file_name, "w", newline="") as file:
    fieldnames = ["column_name", "description", "data_type", "possible_values"]
    writer = csv.DictWriter(file, fieldnames=fieldnames)

    writer.writeheader()
    for column_name, attributes in data_dictionary.items():
        writer.writerow(
            {
                "column_name": column_name,
                "description": attributes["description"],
                "data_type": attributes["data_type"],
                "possible_values": attributes["possible_values"],
            }
        )

print(f"Data dictionary has been written to {csv_file_name}.")
