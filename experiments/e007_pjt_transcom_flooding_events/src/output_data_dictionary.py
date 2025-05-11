# data_dictionary_exporter.py

import os  # For path manipulation if needed
from typing import Any, Dict, List

import pandas as pd

# Data dictionary for the 'aggregated_transcom_flood_events_data' layer
# Based on ogrinfo output and previously established descriptions.
# Note: 'geom' is included conceptually; its representation in a non-spatial format
# like CSV would typically be WKT or similar, or it might be omitted.
AGGREGATED_DATA_DICTIONARY: List[Dict[str, Any]] = [
    {
        "Column Name": "u",
        "Data Type": "Integer64",
        "Description": "Starting node identifier of the OSM road segment that forms the primary key part.",
    },
    {
        "Column Name": "v",
        "Data Type": "Integer64",
        "Description": "Ending node identifier of the OSM road segment that forms the primary key part.",
    },
    {
        "Column Name": "key",
        "Data Type": "Integer64",
        "Description": "Key differentiating multiple edges between the same u and v nodes (typically 0), forming part of the primary key.",
    },
    {
        "Column Name": "geom",  # As per 'Geometry Column = geom'
        "Data Type": "LineString",  # As per 'Geometry: Line String'
        "Description": "The original geometry of the OSM road segment. Stored in EPSG:4326 (WGS 84).",
    },
    {
        "Column Name": "all_events_ids",
        "Data Type": "String",  # Represents a list of strings, e.g., "['ID1', 'ID2']"
        "Description": "A sorted list of unique TRANSCOM event IDs for all pre-filtered events (relevant to flooding, road closures, or specific repairs) that affected this road segment.",
    },
    {
        "Column Name": "all_incidents_count",
        "Data Type": "Integer64",
        "Description": "The count of distinct, continuous time periods (merged intervals or 'incidents') during which this road segment was affected by any relevant pre-filtered event.",
    },
    {
        "Column Name": "all_incidents_total_duration_hours",
        "Data Type": "Real",
        "Description": "The total duration in hours that this road segment was affected by any pre-filtered relevant event, after merging all overlapping event times.",
    },
    {
        "Column Name": "road_closed_events_ids",
        "Data Type": "String",  # Represents a list of strings
        "Description": "A sorted list of unique TRANSCOM event IDs associated with 'road closed' conditions (where the 'event_closed_road' flag was true) affecting this road segment.",
    },
    {
        "Column Name": "road_closed_incidents_count",
        "Data Type": "Integer64",
        "Description": "The count of distinct, continuous time periods during which 'road closed' conditions affected this road segment.",
    },
    {
        "Column Name": "road_closed_total_duration_hours",
        "Data Type": "Real",
        "Description": "The total duration in hours this road segment was affected by 'road closed' conditions, with overlapping event times merged.",
    },
    {
        "Column Name": "road_flooded_events_ids",
        "Data Type": "String",  # Represents a list of strings
        "Description": "A sorted list of unique TRANSCOM event IDs specifically classified as 'road_flooded' (where the 'road_flooded' flag was true, typically from 'WEATHER HAZARD') affecting this segment.",
    },
    {
        "Column Name": "road_flooded_incidents_count",
        "Data Type": "Integer64",
        "Description": "The count of distinct, continuous time periods during which 'road_flooded' conditions affected this road segment.",
    },
    {
        "Column Name": "road_flooded_total_duration_hours",
        "Data Type": "Real",
        "Description": "The total duration in hours this road segment was affected by 'road_flooded' conditions, with overlapping event times merged.",
    },
    {
        "Column Name": "road_repairs_events_ids",
        "Data Type": "String",  # Represents a list of strings
        "Description": "A sorted list of unique TRANSCOM event IDs related to 'road_repairs' (where the 'road_repairs' flag was true, from washout repairs) affecting this segment.",
    },
    {
        "Column Name": "road_repairs_incidents_count",
        "Data Type": "Integer64",
        "Description": "The count of distinct, continuous time periods during which 'road_repairs' conditions affected this road segment.",
    },
    {
        "Column Name": "road_repairs_total_duration_hours",
        "Data Type": "Real",
        "Description": "The total duration in hours this road segment was affected by 'road_repairs' conditions, with overlapping event times merged.",
    },
]


def export_data_dictionary_to_csv(
    data_dictionary_list: List[Dict[str, Any]], output_filepath: str
) -> None:
    """
    Exports a data dictionary, provided as a list of dictionaries, to a CSV file.

    Each dictionary in the list represents a row in the data dictionary,
    typically defining a data column with attributes like 'Column Name',
    'Data Type', and 'Description'.

    Args:
        data_dictionary_list: A list of dictionaries, where each dictionary
                              describes a field in the dataset.
        output_filepath: The path (including filename, e.g., 'data_dict.csv')
                         where the CSV file will be saved.

    Raises:
        IOError: If there's an issue writing the file.
        Exception: For other pandas-related errors during DataFrame creation or CSV export.
    """
    if not data_dictionary_list:
        print("Data dictionary is empty. No CSV file will be created.")
        return

    try:
        df = pd.DataFrame(data_dictionary_list)
        # Ensure consistent column order in CSV
        ordered_columns = ["Column Name", "Data Type", "Description"]
        # Include other columns if they exist, though our current dict only has these three
        df = df[[col for col in ordered_columns if col in df.columns]]

        df.to_csv(output_filepath, index=False)
        print(f"Data dictionary successfully exported to: {output_filepath}")
    except Exception as e:
        print(f"Error exporting data dictionary to CSV at '{output_filepath}': {e}")
        raise  # Re-raise the exception so the caller is aware of the failure


if __name__ == "__main__":
    # Get the directory of the current script
    current_script_directory = os.path.dirname(os.path.abspath(__file__))

    # Define the output filename
    output_csv_filename = "e007_002_transcom_flooding_events.data_dictionary.csv"

    # Create the full path for the output CSV in the same directory as the script
    output_csv_filepath = os.path.join(current_script_directory, output_csv_filename)

    print(f"Attempting to export data dictionary to: {output_csv_filepath}")
    export_data_dictionary_to_csv(AGGREGATED_DATA_DICTIONARY, output_csv_filepath)

    # Example of how to read it back (optional, for verification)
    # try:
    #     df_read = pd.read_csv(output_csv_filepath)
    #     print("\n--- CSV Content (First 5 Rows) ---")
    #     print(df_read.head())
    #     print("------------------------------------")
    # except FileNotFoundError:
    #     print(f"\nFile {output_csv_filepath} not found. Ensure export was successful.")
    # except Exception as e:
    #     print(f"\nError reading exported CSV: {e}")
