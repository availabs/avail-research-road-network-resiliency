import os
import stat
from csv import QUOTE_NONNUMERIC
from os import PathLike
from pathlib import Path
from typing import Dict

import geopandas as gpd
import numpy as np
import pandas as pd
from prefect import get_run_logger, task

from experiments.e001_pjt_utah_redundancy.src.core.utils import (
    restore_detours_info_df_from_gpkg,
)

read_only_perms = stat.S_IREAD | stat.S_IRGRP | stat.S_IROTH


@task(log_prints=True)
def process_base_edges_task(
    roadways_gdf: gpd.GeoDataFrame,  #
) -> gpd.GeoDataFrame:
    """
    Processes the base road network edges GeoDataFrame.

    Args:
        roadways_gdf: Input GeoDataFrame of base road edges. Edges outside the
                      the specific region are removed using __intersects_region__.

    Returns:
        Processed GeoDataFrame filtered for the region, indexed by (u, v, key),
        and with the 'e000_' prefix applied (including to '_intersects_region_').

    Raises:
        AssertionError: If required columns are missing or if the index is not ['u', 'v', 'key'].
        ValueError: If the input GDF becomes empty after filtering.
    """
    logger = get_run_logger()
    logger.info("Processing base edges (e000)...")

    if roadways_gdf.index.names != ["u", "v", "key"]:
        raise ValueError(
            f"Input GDF index names must be ['u', 'v', 'key'], got {roadways_gdf.index.names}"
        )

    # Assert '_intersects_region_' is boolean type
    assert pd.api.types.is_bool_dtype(roadways_gdf["_intersects_region_"]), (
        "'_intersects_region_' column must be boolean type."
    )

    region_roadways_gdf = roadways_gdf[roadways_gdf["_intersects_region_"]].copy()

    region_roadways_gdf["segment_search_text"] = [
        (
            str(coord[0][1])
            + ", "
            + str(coord[0][0])
            + " to "
            + str(coord[1][1])
            + ", "
            + str(coord[1][0])
        )
        if coord
        else None
        for coord in region_roadways_gdf.geometry.map(lambda geom: geom.coords)
    ]

    cols_to_round = ["length", "speed_kph", "travel_time"]
    region_roadways_gdf[cols_to_round] = region_roadways_gdf[cols_to_round].round(3)

    cols_to_drop = ["timestamp", "tags", "osm_way_along_info", "merged_edges"]
    cols_to_drop_existing = [
        col for col in cols_to_drop if col in region_roadways_gdf.columns
    ]
    region_roadways_gdf.drop(columns=cols_to_drop_existing, inplace=True)

    region_roadways_gdf = region_roadways_gdf.add_prefix("e000_")
    region_roadways_gdf.rename(columns={"e000_geometry": "geometry"}, inplace=True)

    # Ensure index is sorted for efficient joining later
    region_roadways_gdf.sort_index(inplace=True)

    logger.info("Done processing base edges (e000)...")

    return region_roadways_gdf


@task(log_prints=True)
def process_e001_task(output_gpkg: PathLike) -> pd.DataFrame:
    """
    Processes the output GPKG from experiment e001 (Redundancy).
    Asserts required columns exist and that the data is indexed by ['u','v','key'].
    Uses 'restore_detours_info_df_from_gpkg'.

    Args:
        output_gpkg: Path to the e001 GPKG file.

    Returns:
        Processed Pandas DataFrame indexed by (u, v, key) with 'e001_' prefix.

    Raises: See underlying functions (FileNotFoundError, AssertionError, KeyError, etc.)
    """
    logger = get_run_logger()
    logger.info(f"Processing e001 GPKG: {output_gpkg}")

    # Let FileNotFoundError propagate if file missing.
    df = restore_detours_info_df_from_gpkg(output_gpkg)

    assert not df.empty, f"e001 GPKG {output_gpkg} loaded empty."

    # Assert the index is already set correctly (as expected from helper)
    assert df.index.names == ["u", "v", "key"], (
        f"e001 DataFrame index names must be ['u', 'v', 'key'], got {df.index.names}"
    )

    assert df.index.is_unique, "e001 DataFrame index is not unique."

    # Create search text (will raise KeyError if cols missing - checked above)
    df["orig_dest_search_text"] = (
        df["origin_lat"].astype(str)
        + ", "
        + df["origin_lon"].astype(str)
        + " to "
        + df["destination_lat"].astype(str)
        + ", "
        + df["destination_lon"].astype(str)
    )

    # Drop columns (will raise KeyError if missing)
    cols_to_drop = ["base_path_edge_ids", "detour_path_edge_ids"]
    # Check which columns actually exist to avoid KeyErrors for optional ones
    cols_to_drop_existing = [col for col in cols_to_drop if col in df.columns]

    if cols_to_drop_existing:
        df.drop(columns=cols_to_drop_existing, inplace=True)

    df = df.add_prefix("e001_")

    logger.info(f"Processed e001 data shape: {df.shape}")

    return df


@task(log_prints=True)
def process_e004_task(output_gpkg: PathLike) -> pd.DataFrame:
    """
    Processes the output GPKG from experiment e004 (Flood Impact).

    Reads the 'flood_impact' layer. Asserts required columns exist.

    Args:
        output_gpkg: Path to the e004 GPKG file (can be zipped).

    Returns:
        Processed Pandas DataFrame indexed by (u, v, key) with 'e004_' prefix.

    Raises:
        FileNotFoundError: If the output_gpkg or layer does not exist.
        AssertionError: If required columns ('u', 'v', 'key') are missing.
        KeyError: If other expected columns in 'columns_to_keep' are missing.
        Exception: Propagates other exceptions from geopandas.
    """
    logger = get_run_logger()
    logger.info(f"Processing e004 GPKG: {output_gpkg}")

    layer_name = "flood_impact"

    # Let FileNotFoundError propagate if file/layer missing
    df = gpd.read_file(
        filename=output_gpkg,  #
        layer=layer_name,
        engine="pyogrio",
    )

    assert not df.empty, (
        f"e004 GPKG {output_gpkg} (layer: {layer_name}) loaded as an empty DataFrame."
    )

    # Drop geometry if it exists
    if "geometry" in df.columns:
        df = df.drop(columns=["geometry"])

    # --- Input Validation ---
    # Assert index columns exist
    assert all(col in df.columns for col in ["u", "v", "key"]), (
        "Missing index columns 'u', 'v', 'key' in e004 GPKG."
    )

    df.set_index(keys=["u", "v", "key"], inplace=True, verify_integrity=True)

    # --- Processing ---
    # Select required columns (will raise KeyError if any are missing)
    columns_to_keep = [
        # u,v,key are now in index
        "nonfunctional_reason",
        "nonfunctional_risk_level",
        "nonfunctional_frequency",
        "isolated_edge_risk_level",
        "isolated_edge_frequency",
    ]

    # Check which columns actually exist to avoid KeyError for optional ones
    df = df[columns_to_keep].add_prefix("e004_")

    logger.info(f"Processed e004 data shape: {df.shape}")
    return df


@task(log_prints=True)
def process_e007_task(
    output_gpkg: str,  #
    output_layer: str,  #
) -> pd.DataFrame:
    """
    Processes the output GPKG from experiment e007 (TRANSCOM Events).
    Reads 'output_layer'. Verifies required columns exist, sets index.

    Args:
        output_gpkg: Path to the e007 GPKG file (can be zipped).
        output_layer: Layer in the e007 GPKG file containing the results.

    Returns:
        Processed DataFrame indexed by (u, v, key) with 'e007_' prefix.

    Raises: See underlying functions (FileNotFoundError, AssertionError, KeyError, etc.)
    """
    logger = get_run_logger()
    logger.info(f"Processing e007 GPKG: {output_gpkg}, layer_name={output_layer}")

    # Let FileNotFoundError propagate
    df = gpd.read_file(
        filename=output_gpkg,  #
        layer=output_layer,
        engine="pyogrio",
    )

    if "geometry" in df.columns:
        df = df.drop(columns=["geometry"])

    # Assert index columns exist in columns
    if not all(col in df.columns for col in ["u", "v", "key"]):
        raise ValueError(
            "Missing columns 'u', 'v', 'key' needed for index in e007 GPKG."
        )

    # Set index based on columns (reverted to set_index as requested)
    df.set_index(
        keys=["u", "v", "key"],  #
        inplace=True,
        verify_integrity=True,
    )

    # Assert index is now correct after setting
    if not isinstance(df.index, pd.MultiIndex) and df.index.names == [
        "u",
        "v",
        "key",
    ]:
        raise ValueError("Index setting failed for e007 DataFrame.")

    # Check which columns actually exist
    df = df.add_prefix("e007_")

    return df


@task(log_prints=True)
def process_e008_task(output_gpkg: str) -> pd.DataFrame:
    """
    Processes the output GPKG from experiment e008 (Network Metrics).
    Reads 'roads_with_centrality_metrics' layer. Asserts required columns exist, sets index.

    Args:
        output_gpkg: Path to the e008 GPKG file (can be zipped).

    Returns:
        Processed DataFrame indexed by (u, v, key) with 'e008_' prefix.

    Raises: See underlying functions (FileNotFoundError, AssertionError, KeyError, etc.)
    """
    logger = get_run_logger()
    logger.info(f"Processing e008 GPKG: {output_gpkg}")
    layer_name = "roads_with_centrality_metrics"

    # Let FileNotFoundError propagate
    df = gpd.read_file(filename=output_gpkg, layer=layer_name, engine="pyogrio")

    assert not df.empty, f"e008 GPKG {output_gpkg} (layer: {layer_name}) loaded empty."

    if "geometry" in df.columns:
        df = df.drop(columns=["geometry"])

    # Assert index columns exist in columns
    assert all(col in df.columns for col in ["u", "v", "key"]), (
        "Missing columns 'u', 'v', 'key' needed for index in e008 GPKG."
    )

    # Set index based on columns (reverted to set_index as requested)
    df.set_index(
        keys=["u", "v", "key"],  #
        inplace=True,
        verify_integrity=True,
    )

    # Assert index is now correct after setting
    assert isinstance(df.index, pd.MultiIndex) and df.index.names == [
        "u",
        "v",
        "key",
    ], "Index setting failed for e008 DataFrame."

    # Select specific columns (will raise KeyError if missing)
    columns_to_keep = [
        # u,v,key are now in index
        "node_betweenness_centrality_u",
        "node_betweenness_centrality_v",
        "edge_betweenness_centrality",
        "closeness_centrality_u",
        "closeness_centrality_v",
        "louvain_community_u",
        "louvain_community_v",
    ]
    # Check which columns actually exist
    df = df[columns_to_keep].add_prefix("e008_")

    return df


@task(log_prints=True)
def process_e009_task(output_gpkg: str) -> pd.DataFrame:
    """
    Processes the output GPKG from experiment e009 (RIS Conflation).
    Reads 'osm_edges_with_ris_meta' layer. Asserts required columns exist, sets index.

    Args:
        gpkg_path: Path to the e009 GPKG file (can be zipped).

    Returns:
        Processed DataFrame indexed by (u, v, key) with 'e009_' prefix.

    Raises: See underlying functions (FileNotFoundError, AssertionError, KeyError, etc.)
    """
    logger = get_run_logger()
    logger.info(f"Processing e009 GPKG: {output_gpkg}")
    layer_name = "osm_edges_with_ris_meta"

    # Let FileNotFoundError propagate
    df = gpd.read_file(
        output_gpkg,  #
        layer=layer_name,
        engine="pyogrio",
    )

    assert not df.empty, (
        f"e009 GPKG {output_gpkg} (layer: {layer_name}) loaded empty. Returning empty DF."
    )

    # Drop optional columns if they exist
    cols_to_drop = ["_idx_", "geometry"]
    cols_to_drop_existing = [col for col in cols_to_drop if col in df.columns]

    if cols_to_drop_existing:
        df.drop(columns=cols_to_drop_existing, inplace=True)

    # Assert index columns exist in columns after potential rename
    assert all(col in df.columns for col in ["u", "v", "key"]), (
        "Missing columns 'u', 'v', 'key' needed for index in e009 GPKG after rename."
    )

    # Set index based on columns (reverted to set_index as requested)
    df.set_index(
        keys=["u", "v", "key"],  #
        inplace=True,
        verify_integrity=True,
    )

    # Assert index is now correct after setting
    assert isinstance(df.index, pd.MultiIndex) and df.index.names == [
        "u",
        "v",
        "key",
    ], "Index setting failed for e009 DataFrame."

    # Keep all remaining columns after setting index and add prefix
    df = df.add_prefix("e009_")

    logger.info(f"Processed e009 data shape: {df.shape}")

    return df


@task(log_prints=True)
def join_results_task(
    region_roadways_gdf: gpd.GeoDataFrame,
    processed_experiment_dfs: Dict[str, pd.DataFrame],
) -> gpd.GeoDataFrame:
    """
    Joins processed experiment DataFrames onto the processed base GeoDataFrame.

    Args:
        region_roadways_gdf: Processed base edges GeoDataFrame (e000), indexed by (u, v, key).
        processed_experiment_dfs: Dict of experiment IDs to processed DataFrames, indexed by (u, v, key).

    Returns:
        A single GeoDataFrame containing the joined data.

    Raises:
        ValueError: If the base GeoDataFrame is empty or invalid.
        AssertionError: If indices are not as expected for joining.
        TypeError: If an item in processed_experiment_dfs is not a DataFrame.
        Exception: Propagates exceptions from pandas join operation.
    """
    logger = get_run_logger()
    logger.info("Joining processed results...")

    if (
        not isinstance(region_roadways_gdf, gpd.GeoDataFrame)
        or region_roadways_gdf.empty
    ):
        raise ValueError("Base GeoDataFrame for join cannot be empty or invalid.")

    # Assert base GDF has the correct index
    assert isinstance(
        region_roadways_gdf.index, pd.MultiIndex
    ) and region_roadways_gdf.index.names == ["u", "v", "key"], (
        "region_roadways_gdf GDF for join must be indexed by ['u', 'v', 'key']"
    )

    assert region_roadways_gdf.index.is_unique

    assert region_roadways_gdf["e000__intersects_region_"].dtype == bool, (
        "Column 'e000__intersects_region_' is not of boolean type"
    )

    assert region_roadways_gdf["e000__intersects_region_"].all(), (
        "Not all values in column 'e000__intersects_region_' are True"
    )

    aggregated_gdf = region_roadways_gdf.copy()

    # Sort experiment keys for deterministic join order
    sorted_exp_keys = sorted(processed_experiment_dfs.keys())

    for exp_id in sorted_exp_keys:
        exp_df = processed_experiment_dfs[exp_id]

        if isinstance(exp_df, pd.DataFrame):
            # Assert experiment DF has the correct index
            assert isinstance(exp_df.index, pd.MultiIndex) and exp_df.index.names == [
                "u",
                "v",
                "key",
            ], (
                f"Experiment {exp_id} DataFrame must be indexed by ['u', 'v', 'key'] for join."
            )

            # Sort indices if needed before join (improves performance)
            if not aggregated_gdf.index.is_monotonic_increasing:
                aggregated_gdf.sort_index(inplace=True)

            if not exp_df.index.is_monotonic_increasing:
                exp_df.sort_index(inplace=True)

            assert exp_df.index.is_unique

            logger.info(f"Joining {exp_id} data (Shape: {exp_df.shape})")

            # Let join raise errors on major issues
            aggregated_gdf = aggregated_gdf.join(exp_df, how="left")
        else:
            # Fail fast if an expected result is invalid
            raise TypeError(
                f"Invalid data type received for experiment {exp_id}: {type(exp_df)}"
            )

    logger.debug(f"Final aggregated shape: {aggregated_gdf.shape}")

    assert set(region_roadways_gdf.index) == set(aggregated_gdf.index), (
        "region_roadways_gdf index entries were not preserved."
    )

    return aggregated_gdf


# --- Saving Task ---


@task(log_prints=True)
def save_results_task(
    final_aggregated_gdf: gpd.GeoDataFrame, fused_results_gpkg: PathLike
) -> Dict[str, str]:
    """
    Saves the final aggregated GeoDataFrame to GPKG and CSV formats.

    Args:
        final_aggregated_gdf: The fully joined GeoDataFrame.
        output_dir: The directory to save the output files.
        region_name: The name of the region for naming files.

    Returns:
        A dictionary containing the paths (as strings) to the saved GPKG and CSV files.

    Raises:
        ValueError: If the input GeoDataFrame is invalid or empty.
        OSError: If the output directory cannot be created.
        Exception: Propagates exceptions from file saving operations.
    """
    if (
        not isinstance(final_aggregated_gdf, gpd.GeoDataFrame)
        or final_aggregated_gdf.empty
    ):
        raise ValueError("Cannot save empty or invalid final GeoDataFrame.")

    fused_results_gpkg_fpath = Path(fused_results_gpkg)
    fused_results_csv_fpath = fused_results_gpkg_fpath.with_suffix(".csv")

    output_dir = fused_results_gpkg_fpath.parent.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    logger = get_run_logger()
    logger.info(f"Saving final results to directory: {output_dir}")

    # Prepare GDF for saving (reset index, create _id_)
    gdf_to_save = final_aggregated_gdf.copy()

    # Reset index if it's the expected MultiIndex
    assert isinstance(gdf_to_save.index, pd.MultiIndex) and gdf_to_save.index.names == [
        "u",
        "v",
        "key",
    ], "final_aggregated_gdf does not have the expected (u, v, key) index"

    gdf_to_save.sort_index(inplace=True)

    assert gdf_to_save.index.is_unique

    gdf_to_save.reset_index(inplace=True, allow_duplicates=False)

    gdf_to_save.index = range(1, len(gdf_to_save) + 1)

    gdf_to_save.rename_axis("_id_", axis=0, inplace=True)

    # --- Save GPKG ---
    # Let exceptions propagate from to_file
    logger.info(f"Saving aggregated GPKG to {fused_results_gpkg_fpath}")

    fused_results_gpkg_fpath.unlink()

    gdf_to_save.to_file(
        filename=fused_results_gpkg_fpath,
        layer="road_network_resiliency_analysis",
        engine="pyogrio",
    )

    os.chmod(fused_results_gpkg_fpath, read_only_perms)

    logger.info("GPKG saved successfully.")

    # --- Save CSV ---
    # Let exceptions propagate from to_csv
    logger.info(f"Saving aggregated CSV to {fused_results_csv_fpath}")
    df_to_save = gdf_to_save.drop(columns="geometry").replace(
        to_replace=np.nan,
        value=None,
    )

    fused_results_csv_fpath.unlink()

    df_to_save.to_csv(
        fused_results_csv_fpath,
        index=True,
        index_label=df_to_save.index.name,  # Use the actual index name (_id_ or other)
        quoting=QUOTE_NONNUMERIC,
    )

    os.chmod(fused_results_csv_fpath, read_only_perms)

    logger.info("CSV saved successfully.")

    return {
        "gpkg_path": str(fused_results_gpkg_fpath),
        "csv_path": str(fused_results_csv_fpath),
    }
