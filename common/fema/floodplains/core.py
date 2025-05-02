import hashlib
import logging
import os
import pathlib
import pickle
from typing import Dict, List, Tuple

import geopandas as gpd
import numpy as np
import pyogrio
import shapely
from shapely.geometry import MultiPolygon, Polygon, box
from shapely.ops import unary_union
from shapely.strtree import STRtree

logger = logging.getLogger(__name__)

FEMA_FLOODPLAINS_GDF_VERSION = "0.1.0"
GDF_VERSION_ATTR = (
    "FEMA_FLOODPLAINS_GDF_VERSION"  # Attribute key for storing the version
)


# --- Cache Configuration ---
# PARENTS: floodplains -> fema -> common -> project root
PROJECT_ROOT = pathlib.Path(__file__).parent.parent.parent.parent.resolve()
CACHE_DIR = PROJECT_ROOT / "data/pickles/fema/floodplains"
CACHE_DIR.mkdir(parents=True, exist_ok=True)  # Ensure cache directory exists

"""
Flood Zone Risk Hierarchy:

This dictionary maps FEMA flood zone (fld_zone, zone_subty) pairs to their relative risk ranks.
Lower rank values indicate higher flood risk.

Ranking Logic:

The ranking is based on the severity and frequency of flooding, considering factors such as:
- Coastal vs. riverine flooding: Coastal high-hazard areas (V and VE zones) are ranked highest due to the destructive force of wave action and storm surge.
- Floodways: Floodways (AE with FLOODWAY or FW sub-types) are ranked high due to the high velocity of floodwaters.
- Base Flood Elevation (BFE): Zones with BFEs (AE, VE) are ranked higher than those without (A). VE zones are ranked higher than V zones.
- Shallow flooding: AO and AH zones indicate shallow flooding and are ranked accordingly.
- 0.2% annual chance flood hazard (X zone): These areas are ranked lower than 1% annual chance flood hazard areas (A, AE, VE, etc.).
- Levee protection: Areas with reduced flood risk due to levees (X zone) are ranked lower.
- Open water: Open water is ranked the lowest.

Specific ranking:
1.  VE zones with riverine floodways in coastal zones: Highest risk due to combined wave action and riverine flooding.
2.  VE zones in coastal floodplains: High risk due to wave action.
3.  General VE zones: High risk due to wave action.
4.  V zones: Coastal high hazard, but no specific wave height information.
5.  AE zones with riverine floodways in coastal zones: High risk, but lower than V/VE due to absence of wave action.
6.  AE zones with riverine floodplains in coastal areas.
7.  AE zones with floodways: High risk due to high-velocity floodwaters.
8.  AE zones within coastal floodplains.
9.  AO zones within coastal floodplains.
10. AO zones: Shallow flooding.
11. AE zones: General floodplains.
12. AH zones: Shallow ponding.
13. A zones: 1% annual chance of flooding, but no BFE.
14. X zones with 0.2% annual chance flood hazard.
15. X zones with shallow flooding (less than 1 foot).
16. X zones with levee protection.
17. General X zones: Minimal flood hazard.
18. Open water.

Citations and Resources:
- FEMA Flood Zones: https://www.fema.gov/flood-zones
- FEMA Flood Map Service Center: https://msc.fema.gov/portal/home
- FEMA Guidelines and Standards for Flood Risk Analysis and Mapping: https://www.fema.gov/guidelines-and-standards-flood-risk-analysis-and-mapping
- FEMA P-259, Engineering Principles and Practices for Retrofitting Flood-Prone Residential Structures: https://www.fema.gov/sites/default/files/2020-08/fema259_complete_rev.pdf
- NOAA Coastal Flood Hazard: https://oceanservice.noaa.gov/hazards/natural-hazards/
- Association of State Floodplain Managers (ASFPM): https://www.floods.org/
"""
flood_zone_risk_hierarchy = {
    ("VE", "RIVERINE FLOODWAY SHOWN IN COASTAL ZONE"): 1,
    ("VE", "COASTAL FLOODPLAIN"): 2,
    ("VE", None): 3,
    ("V", None): 4,
    ("AE", "RIVERINE FLOODWAY SHOWN IN COASTAL ZONE"): 5,
    ("AE", "RIVERINE FLOODPLAIN IN COASTAL AREA"): 6,
    ("AE", "FLOODWAY"): 7,
    ("AE", "FW"): 7,  # Treat FW as equivalent to FLOODWAY
    ("AE", "COASTAL FLOODPLAIN"): 8,
    ("AO", "COASTAL FLOODPLAIN"): 9,
    ("AO", None): 10,
    ("AE", None): 11,
    ("AH", None): 12,
    ("A", None): 13,
    ("X", "0.2 PCT ANNUAL CHANCE FLOOD HAZARD"): 14,
    ("X", "1 PCT DEPTH LESS THAN 1 FOOT"): 15,
    ("X", "AREA WITH REDUCED FLOOD RISK DUE TO LEVEE"): 16,
    ("X", None): 17,
    ("OPEN WATER", None): 18,
}


def get_highest_ranked_flood_zone(flood_zone_pairs):
    """
    Determines the highest ranked flood zone from a list of (fld_zone, zone_subty) pairs.

    The ranking is based on the severity and frequency of flooding, considering factors such as:
    - Coastal vs. riverine flooding: Coastal areas (V and VE zones) are generally ranked higher due to the destructive force of wave action and storm surge.
    - Floodways: Floodways (AE with FLOODWAY or FW sub-types) are ranked high due to the high velocity of floodwaters.
    - Base Flood Elevation (BFE): Zones with BFEs (AE, VE) are ranked higher than those without (A).
    - Shallow flooding: AO and AH zones indicate shallow flooding and are ranked accordingly.
    - 0.2% annual chance flood hazard (X zone): These areas are ranked lower than 1% annual chance flood hazard areas (A, AE, VE, etc.).
    - Levee protection: Areas with reduced flood risk due to levees (X zone) are ranked lower.
    - Open water: Open water is ranked the lowest.

    Specific ranking logic:
    1.  VE zones with riverine floodways in coastal zones are ranked highest due to the combined impact of wave action and riverine flooding.
    2.  VE zones in coastal floodplains are ranked next due to the impact of wave action.
    3.  General VE zones are ranked third due to wave action.
    4.  AE zones with riverine floodways in coastal zones are ranked high, but lower than VE zones due to the absence of wave action.
    5.  AE zones with riverine floodplains in coastal areas are ranked next.
    6.  AE zones with floodways are ranked high due to high-velocity floodwaters.
    7.  AE zones within coastal floodplains are ranked next.
    8.  V zones are ranked next, as they are coastal high hazard, but do not provide specific wave height information.
    9.  AO zones within coastal floodplains are ranked next.
    10. AO zones are ranked next, they represent shallow flooding.
    11. AE zones are ranked next, representing general floodplains.
    12. AH zones are ranked next, representing shallow ponding.
    13. A zones are ranked next, they represent areas with 1% annual chance of flooding, but no BFE.
    14. X zones with 0.2% annual chance flood hazard are ranked next.
    15. X zones with shallow flooding (less than 1 foot) are ranked next.
    16. X zones with levee protection are ranked next.
    17. General X zones are ranked next, representing minimal flood hazard.
    18. Open water is ranked lowest.

    Citations and Resources:
    - FEMA Flood Zones: https://www.fema.gov/flood-zones
    - FEMA Flood Map Service Center: https://msc.fema.gov/portal/home
    - FEMA Guidelines and Standards for Flood Risk Analysis and Mapping: https://www.fema.gov/guidelines-and-standards-flood-risk-analysis-and-mapping
    - FEMA P-259, Engineering Principles and Practices for Retrofitting Flood-Prone Residential Structures: https://www.fema.gov/sites/default/files/2020-08/fema259_complete_rev.pdf
    - NOAA Coastal Flood Hazard: https://oceanservice.noaa.gov/hazards/natural-hazards/
    - Association of State Floodplain Managers (ASFPM): https://www.floods.org/

    Args:
        flood_zone_pairs: A list of tuples, where each tuple is (fld_zone, zone_subty).

    Returns:
        A tuple (fld_zone, zone_subty) representing the highest ranked flood zone, or None if the list is empty.
    """
    if not flood_zone_pairs:
        return None

    highest_rank = float("inf")  # Initialize with a very high value
    highest_ranked_zone = None

    for zone_pair in flood_zone_pairs:
        rank_level = flood_zone_risk_hierarchy.get(
            zone_pair, float("inf")
        )  # If not found, treat as very low risk level.

        if rank_level < highest_rank:
            highest_rank = rank_level
            highest_ranked_zone = zone_pair

    return highest_ranked_zone


# Make sure that for every fld_zone, there is an entry in the flood_zone_risk_hierarchy where zone_subty is None.
assert all(
    [
        (fld_zone, None) in flood_zone_risk_hierarchy
        for fld_zone, _ in flood_zone_risk_hierarchy.keys()
    ]
)


def convert_len_value_to_meters(len_value, len_unit):
    """
    Convert a length value to meters based on its unit, handling both coded values and descriptions.

    Args:
        len_value (float, int, or None): The length value in the specified unit.
        len_unit (str or None): The unit, which can be a coded value (e.g., 'FT') or description (e.g., 'Feet').

    Returns:
        float or None: The length in meters, or None if conversion is not possible (e.g., null inputs, invalid unit).

    Raises:
        ValueError: If len_value is non-numeric (except None) or len_unit is invalid and not None.
    """
    # Define conversion factors and mappings based on D_Length_Units
    unit_mappings = {
        # Coded Value: (Description, Conversion Factor to Meters)
        "CM": ("Centimeters", 0.01),
        "FT": ("Feet", 0.3048),
        "IN": ("Inches", 0.0254),
        "KM": ("Kilometers", 1000.0),
        "M": ("Meters", 1.0),
        "MI": ("Miles", 1609.344),
        "MM": ("Millimeters", 0.001),
        "USFT": ("U.S. Survey Feet", 0.3048006096),
        "NP": ("NP", None),
    }

    # Create case-insensitive lookup dictionaries
    code_to_factor = {k.upper(): v[1] for k, v in unit_mappings.items()}
    desc_to_code = {v[0].upper(): k for k, v in unit_mappings.items()}
    # Add coded values to desc_to_code for cases where len_unit might be a coded value
    for code in unit_mappings:
        desc_to_code[code.upper()] = code

    # Handle None inputs
    if len_value is None or len_unit is None:
        return None

    # Validate len_value is numeric
    if not isinstance(len_value, (int, float)):
        raise ValueError("len_value must be a numeric value (int or float) or None.")

    # Handle special case: -9999 as a "no data" value
    if len_value == -9999:
        return None

    # Convert len_unit to uppercase for case-insensitive matching
    len_unit_upper = len_unit.upper()

    # Determine the coded value
    if len_unit_upper in code_to_factor:
        # len_unit is a coded value
        code = len_unit_upper
    elif len_unit_upper in desc_to_code:
        # len_unit is a description
        code = desc_to_code[len_unit_upper]
    else:
        raise ValueError(
            f"Invalid len_unit '{len_unit}'. Must match a coded value or description from D_Length_Units."
        )

    # Handle NP case
    if code == "NP":
        return None

    # Perform conversion
    meters = len_value * code_to_factor[code]
    return meters


def convert_velocity_value_to_meters_per_second(velocity, vel_unit):
    """
    Convert a velocity value to meters per second based on its unit.

    Args:
        velocity (float, int, or None): The velocity value in the specified unit.
        vel_unit (str or None): The unit, which can be a coded value (e.g., 'FPS') or description (e.g., 'Feet per Second').

    Returns:
        float or None: The velocity in meters per second, or None if conversion is not possible.

    Raises:
        ValueError: If velocity is non-numeric (except None) or vel_unit is invalid and not None.
    """
    # Define conversion factors and mappings based on D_Velocity_Units
    unit_mappings = {
        # Coded Value: (Description, Conversion Factor to Meters per Second)
        "FPS": ("Feet per Second", 0.3048),
        "MPS": ("Meters per Second", 1.0),
        "NP": ("NP", None),
    }

    # Create case-insensitive lookup dictionaries
    code_to_factor = {k.upper(): v[1] for k, v in unit_mappings.items()}
    desc_to_code = {v[0].upper(): k for k, v in unit_mappings.items()}
    # Add coded values to desc_to_code for cases where vel_unit might be a coded value
    for code in unit_mappings:
        desc_to_code[code.upper()] = code

    # Handle None inputs
    if velocity is None or vel_unit is None:
        return None

    # Validate velocity is numeric
    if not isinstance(velocity, (int, float)):
        raise ValueError("velocity must be a numeric value (int or float) or None.")

    # Handle special cases: -9999 and -8888 as "no data" values
    if velocity in (-9999, -8888):
        return None

    # Convert vel_unit to uppercase for case-insensitive matching
    vel_unit_upper = vel_unit.upper()

    # Determine the coded value
    if vel_unit_upper in code_to_factor:
        # vel_unit is a coded value
        code = vel_unit_upper
    elif vel_unit_upper in desc_to_code:
        # vel_unit is a description
        code = desc_to_code[vel_unit_upper]
    else:
        raise ValueError(
            f"Invalid vel_unit '{vel_unit}'. Must match a coded value or description from D_Velocity_Units."
        )

    # Handle NP case
    if code == "NP":
        return None

    # Perform conversion
    meters_per_second = velocity * code_to_factor[code]
    return meters_per_second


def _assign_flood_risk_level(
    floodplains_gdf: gpd.GeoDataFrame,  #
) -> gpd.GeoDataFrame:
    """
    NOTE: Mutates the input floodplains_gdf

    Assigns a numerical flood risk level based on FEMA flood zone codes using the
    flood_zone_risk_hierarchy dictionary.

    Fails with AssertionError if floodplains_gdf contains unexpected (fld_zone, zone_subty) pairs.

    Args:
        floodplains_gdf: gpd.GeoDataFrame
    """
    unexpected_pairs = {
        (row.fld_zone, row.zone_subty)
        for row in floodplains_gdf[["fld_zone", "zone_subty"]].itertuples()
        if (row.fld_zone, row.zone_subty) not in flood_zone_risk_hierarchy
    }

    assert not unexpected_pairs, (
        f"Encountered unexpected (fld_zone, zone_subty) pairs: ({unexpected_pairs})"
    )

    floodplains_gdf["_flood_risk_level_"] = [
        flood_zone_risk_hierarchy[(row.fld_zone, row.zone_subty)]
        for row in floodplains_gdf[["fld_zone", "zone_subty"]].itertuples()
    ]


def calculate_md5(
    filepath: pathlib.Path,  #
    buffer_size: int = 65536,
) -> str:
    """Calculates the MD5 hash of a file."""
    md5 = hashlib.md5()

    with open(filepath, "rb") as f:
        while True:
            data = f.read(buffer_size)
            if not data:
                break
            md5.update(data)

    return md5.hexdigest()


def calculate_gdf_geom_hash(
    gdf: gpd.GeoDataFrame,  #
) -> str:
    """
    Calculates a SHA256 hash based on the WKB representation of geometries
    in a GeoDataFrame, ensuring order independence.
    """
    if gdf is None or gdf.empty or "geometry" not in gdf.columns:
        return hashlib.sha256(b"empty_or_invalid_gdf").hexdigest()

    try:
        # Get WKB, sort to ensure order independence, concatenate, hash
        all_wkb = sorted([geom.wkb_hex for geom in gdf.geometry if geom is not None])
        combined_wkb_str = "".join(all_wkb)
        return hashlib.sha256(combined_wkb_str.encode("utf-8")).hexdigest()
    except Exception as e:
        logger.warning(
            f"Could not generate geometry hash: {e}. Returning default hash."
        )
        # Return a default hash if WKB fails for any reason
        return hashlib.sha256(b"geometry_hashing_error").hexdigest()


def get_clipped_floodplain_data(
    floodplains_gpkg_path: os.PathLike,  # Changed type hint to os.PathLike
    buffered_region_gdf: gpd.GeoDataFrame,
    # Define _assign_flood_risk_level or import it
) -> gpd.GeoDataFrame:
    """
    Extracts and clips floodplain data for a specific region, using a file-based
    cache keyed by input file MD5 and clipping mask geometry hash.

    Parameters:
        floodplains_gpkg_path (os.PathLike): Path (str or pathlib.Path) to the
                                             geopackage containing floodplain data.
        buffered_region_gdf (GeoDataFrame): Buffer area around the region of interest (used as mask).

    Returns:
        GeoDataFrame: Floodplain data clipped to the region boundaries, in the
                      original CRS of buffered_region_gdf.

    Raises:
        ValueError: If input GeoDataFrames are empty or required paths don't exist.
        FileNotFoundError: If floodplains_gpkg_path does not exist.
    """
    # Convert input path to a pathlib.Path object for consistent handling
    floodplains_path = pathlib.Path(floodplains_gpkg_path)

    logger.info(f"Getting clipped floodplain data for GPKG: {floodplains_path.name}")
    logger.info(f"Using FEMA Floodplain GDF Version: {FEMA_FLOODPLAINS_GDF_VERSION}")

    # --- Input Validation ---
    if not floodplains_path.is_file():
        raise FileNotFoundError(
            f"Floodplain GeoPackage not found at: {floodplains_path}"
        )
    if buffered_region_gdf is None or buffered_region_gdf.empty:
        raise ValueError("Buffered region GeoDataFrame is empty or None")
    if buffered_region_gdf.crs is None:
        raise ValueError("Buffered region GeoDataFrame must have a CRS defined.")

    original_crs = buffered_region_gdf.crs  # Store original CRS for final output

    # --- Generate Cache Key Components ---
    try:
        gpkg_md5 = calculate_md5(floodplains_path)  # Use the Path object
        logger.debug(f"GPKG MD5: {gpkg_md5}")
    except Exception as e:
        logger.error(f"Failed to calculate MD5 for {floodplains_path}: {e}")
        raise  # Re-raise error, cannot proceed without MD5

    mask_geom_hash = calculate_gdf_geom_hash(buffered_region_gdf)
    logger.debug(f"Mask Geometry Hash: {mask_geom_hash}")

    target_crs_wkt = original_crs.to_wkt()  # Use WKT for consistency
    logger.debug(
        f"Target CRS WKT (part of key): {target_crs_wkt[:50]}..."
    )  # Log truncated WKT

    # Combine components into a single string for the final key hash
    cache_key_string = f"{gpkg_md5}-{mask_geom_hash}-{target_crs_wkt}"
    cache_filename = (
        hashlib.sha256(cache_key_string.encode("utf-8")).hexdigest() + ".pkl"
    )
    cache_filepath = CACHE_DIR / cache_filename
    logger.debug(f"Cache filepath: {cache_filepath}")

    # --- Cache Check ---
    if cache_filepath.is_file():
        logger.info(f"Potential cache hit found: {cache_filepath}")
        try:
            with open(cache_filepath, "rb") as f:
                cached_gdf = pickle.load(f)

            # --- Cache Validation ---
            if isinstance(cached_gdf, gpd.GeoDataFrame):
                # 1. Check Version
                cached_version = cached_gdf.attrs.get(GDF_VERSION_ATTR)

                if cached_version == FEMA_FLOODPLAINS_GDF_VERSION:
                    # 2. Check CRS
                    if cached_gdf.crs == original_crs:
                        logger.info(
                            "Successfully loaded GeoDataFrame from cache (version and CRS match)."
                        )
                        return cached_gdf  # Cache is valid, return it
                    else:
                        logger.warning(
                            f"Cached GDF CRS mismatch! Expected {original_crs}, got {cached_gdf.crs}. Invalidating cache."
                        )
                else:
                    logger.warning(
                        f"Cached GDF version mismatch! Expected {FEMA_FLOODPLAINS_GDF_VERSION}, got {cached_version}. Invalidating cache."
                    )
            else:
                logger.warning(
                    f"Cached file {cache_filepath} did not contain a GeoDataFrame. Invalidating cache."
                )

            # If we reach here, the cache was invalid (wrong type, version, or CRS)
            # Delete the invalid cache file before recalculating
            logger.warning(f"Deleting invalid cache file: {cache_filepath}")
            try:
                cache_filepath.unlink(missing_ok=True)  # Delete stale/invalid cache
            except OSError as e:
                logger.error(
                    f"Failed to delete invalid cache file {cache_filepath}: {e}"
                )
                # Proceed with recalculation anyway

        except (pickle.UnpicklingError, EOFError, Exception) as e:
            logger.warning(
                f"Failed to load or validate cache file {cache_filepath}: {e}. Recalculating."
            )
            # Attempt to remove corrupted cache file
            try:
                cache_filepath.unlink(missing_ok=True)
            except OSError:
                pass  # Ignore errors during cleanup

    # --- Cache Miss or Load Failure: Perform Calculation ---
    logger.info("Cache miss or invalid cache. Performing clipping operation...")

    # Read metadata to get source CRS (required for pyogrio mask)
    try:
        floodplains_info = pyogrio.read_info(
            floodplains_path,  # Use the Path object
            layer="merged_floodplains",  # Assuming this layer name
        )
        source_crs = floodplains_info["crs"]
        if source_crs is None:
            raise ValueError("Could not determine CRS from floodplain source.")
        logger.debug(f"Source Floodplain CRS: {source_crs}")
    except Exception as e:
        logger.error(f"Failed to read info from {floodplains_path}: {e}")
        raise

    # Reproject mask to source CRS for reading/clipping
    mask_gdf_reprojected = buffered_region_gdf.to_crs(source_crs)

    # Read relevant part of floodplains using bbox, then clip precisely
    try:
        logger.info("Reading floodplains with bbox filter...")
        # Remove geometry_type="MultiPolygon" as it causes warnings and isn't effective here
        floodplains_subset_gdf = gpd.read_file(
            filename=floodplains_path,  # Use the Path object
            engine="pyogrio",
            layer="merged_floodplains",  # Assuming this layer name
            bbox=tuple(mask_gdf_reprojected.total_bounds),
        )
        logger.info(f"Read {len(floodplains_subset_gdf)} features within bounding box.")

        if floodplains_subset_gdf.empty:
            logger.warning("No floodplain features found within the bounding box.")
            # Create empty GDF with expected columns if needed downstream
            clipped_floodplains_gdf = gpd.GeoDataFrame(
                columns=floodplains_subset_gdf.columns.tolist()
                + ["_flood_risk_level_"],
                geometry=[],
                crs=source_crs,
            )
        else:
            logger.info("Performing overlay (intersection)...")
            # Perform overlay using the reprojected mask
            # NOTE: floodplains_subset_gdf was created using a bounding box,
            #       now we perform a more precise geospatial intersection.
            clipped_floodplains_gdf = floodplains_subset_gdf.overlay(
                right=mask_gdf_reprojected,
                how="intersection",
                keep_geom_type=True,  # Keep original type where possible
                make_valid=True,  # Attempt to fix invalid geometries
            )
            logger.info(
                f"Overlay complete. Result has {len(clipped_floodplains_gdf)} features."
            )

            # Assign risk level (assuming this function exists and modifies inplace or returns)
            logger.info("Assigning flood risk level...")

            # Make sure _assign_flood_risk_level handles or returns the GDF
            _assign_flood_risk_level(
                floodplains_gdf=clipped_floodplains_gdf
            )  # Modify in place?

            # Ensure the required column exists after assignment
            if "_flood_risk_level_" not in clipped_floodplains_gdf.columns:
                logger.error("'_flood_risk_level_' column missing after assignment.")
                # Handle error appropriately, maybe return empty or raise
                raise ValueError(
                    "'_flood_risk_level_' column missing after assignment."
                )

    except Exception as e:
        logger.error(f"Error during floodplain reading or clipping: {e}", exc_info=True)
        raise

    # Convert back to the original CRS of the input buffer
    logger.info(f"Converting result back to original CRS: {original_crs}")

    final_gdf = clipped_floodplains_gdf.to_crs(original_crs)

    # --- Save to Cache ---
    try:
        # Add version attribute before saving
        final_gdf.attrs[GDF_VERSION_ATTR] = FEMA_FLOODPLAINS_GDF_VERSION
        logger.debug(
            f"Added version {FEMA_FLOODPLAINS_GDF_VERSION} to GDF attributes before caching."
        )

        logger.info(f"Saving result to cache file: {cache_filepath}")
        with open(cache_filepath, "wb") as f:
            pickle.dump(
                final_gdf,  #
                f,
                protocol=pickle.HIGHEST_PROTOCOL,
            )
        logger.info("Successfully saved result to cache.")
    except Exception as e:
        logger.warning(f"Failed to save result to cache file {cache_filepath}: {e}")
        # Don't raise error here, just warn, as the main result is still available

    return final_gdf


def quadrat_cut_geometry(
    geometry: Polygon | MultiPolygon, quadrat_width: float
) -> List[Polygon]:
    """
    Divides a Shapely Polygon or MultiPolygon geometry into smaller rectangular quadrats (tiles)
    of a specified width. This technique is useful for simplifying complex geometries and
    improving the performance of spatial operations like intersections.

    Args:
        geometry (shapely.Polygon | shapely.MultiPolygon): The input geometry to be subdivided.
        quadrat_width (float): The desired width of each square quadrat tile in the geometry's units.

    Returns:
        List[shapely.Polygon]: A list of Shapely Polygon geometries representing the resulting quadrats.

    Explanation:
    1.  Bounding Box:
        - The function first calculates the bounding box of the input geometry using `geometry.bounds`.
        - The bounding box is a rectangle that encloses the entire geometry, defined by its minimum and maximum x and y coordinates (minx, miny, maxx, maxy).
    2.  Grid Creation:
        - It then creates a grid of rectangular quadrats within the bounding box.
        - The number of quadrats in the x and y directions is determined by dividing the width and height of the bounding box by the specified `quadrat_width`.
        - The `range()` function is used to iterate over the grid cells.
    3.  Quadrat Bounds:
        - For each grid cell, the function calculates the bounds of the corresponding quadrat.
        - The bounds are determined by adding multiples of `quadrat_width` to the minimum x and y coordinates of the bounding box.
    4.  Quadrat Geometry:
        - The `shapely.geometry.box(*quadrat_bounds)` function is used to create a Shapely Polygon geometry representing the rectangular quadrat.
    5.  Intersection Check:
        - The `geometry.intersects(quadrat)` method checks if the input geometry intersects the current quadrat.
        - This is an efficient way to determine if the quadrat is within or partially within the input geometry.
    6.  Intersection Geometry:
        - If the quadrat intersects the input geometry, the `geometry.intersection(quadrat)` method calculates the actual intersection geometry.
        - This ensures that only the portions of the quadrats that are within the input geometry are included in the result.
    7.  Empty Intersection Check:
        - The `intersection.is_empty` attribute checks if the intersection geometry is empty.
        - This can happen if the quadrat only touches the boundary of the input geometry or if the intersection is a line or point.
        - If the intersection is not empty, the quadrat geometry is added to the `quadrats` list.
    8.  Return Quadrats:
        - Finally, the function returns the `quadrats` list, which contains all the resulting quadrat geometries.

    Use Case:
    - This function is particularly useful for simplifying complex geometries and speeding up spatial operations.
    - By dividing a large, complex geometry into smaller, simpler quadrats, you can reduce the computational cost of operations like intersections and unions.
    - This technique is commonly used in spatial indexing and query optimization.
    - See: https://www.stevencanplan.com/2017/12/the-genius-of-using-st_subdivide-to-speed-up-postgis-intersection-comparisons/
    """
    minx, miny, maxx, maxy = geometry.bounds
    quadrats = []

    for x in range(int((maxx - minx) / quadrat_width) + 1):
        for y in range(int((maxy - miny) / quadrat_width) + 1):
            quadrat_bounds = (
                minx + x * quadrat_width,
                miny + y * quadrat_width,
                minx + (x + 1) * quadrat_width,
                miny + (y + 1) * quadrat_width,
            )

            quadrat = box(*quadrat_bounds)

            if geometry.intersects(quadrat):
                intersection = geometry.intersection(quadrat)

                if not intersection.is_empty:
                    quadrats.append(intersection)

    return quadrats


def create_subdivided_spatial_index(
    poly_gdf: gpd.GeoDataFrame, quadrat_width: float
) -> Tuple[STRtree, np.array, Dict[int, int]]:
    """
    Creates a spatial index (STRtree) from a GeoDataFrame of polygons by subdividing
    each polygon into smaller rectangular quadrats. This approach is beneficial for
    improving the performance of spatial operations on complex or large geometries.

    Args:
        poly_gdf (gpd.GeoDataFrame): The GeoDataFrame containing the polygon geometries.
        quadrat_width (float): The desired width of each square quadrat tile in the geometry's units.

    Returns:
        Tuple[STRtree, np.array, Dict[int, int]]: A tuple containing:
            - STRtree: The spatial index built from the subdivided quadrats.
            - np.array: A NumPy array of Shapely Polygon geometries representing the quadrats.
            - Dict[int, int]: A dictionary mapping each quadrat index to the index of the original polygon it came from.

    Explanation:
    1.  Subdivision of Polygons:
        - The function iterates through each polygon geometry in the input GeoDataFrame.
        - For each polygon, it calls the `quadrat_cut_geometry` function (assumed to be defined elsewhere) to subdivide the polygon into smaller rectangular quadrats.
        - The resulting quadrats are appended to the `all_quadrats` list.
    2.  Lookup Map Creation:
        - A dictionary called `lookup_map` is created to keep track of which original polygon each quadrat originated from.
        - This is important for later analysis or processing, as it allows you to trace back the quadrats to their source polygons.
        - The `lookup_map` keys are sequential integers corresponding to the index of each quadrat in the `all_quadrats` list.
        - The `lookup_map` values are the indices of the original polygons in the input GeoDataFrame.
    3.  NumPy Array Conversion:
        - The `all_quadrats` list, which contains Shapely Polygon geometries, is converted to a NumPy array (`quadrat_geoms`).
        - NumPy arrays provide efficient storage and manipulation of numerical data, which is beneficial for spatial operations.
    4.  STRtree Index Creation:
        - An `STRtree` spatial index is created from the `quadrat_geoms` array.
        - STRtree (Spatial Tree) is a spatial indexing data structure that allows for efficient querying of geometries based on their spatial relationships (e.g., intersection, containment).
        - It organizes the geometries in a hierarchical tree structure, which enables fast retrieval of geometries that are spatially close to a query geometry.
        - See: https://shapely.readthedocs.io/en/2.0.4/strtree.html
    5.  Return Values:
        - The function returns a tuple containing the `STRtree` index, the `quadrat_geoms` array, and the `lookup_map` dictionary.

    Use Case:
    - This function is useful for preprocessing complex polygon datasets to improve the performance of spatial operations.
    - By subdividing polygons and creating a spatial index, you can significantly reduce the number of intersection checks required for spatial joins or other spatial queries.
    - The `lookup_map` allows you to trace back the quadrats to their original polygons, which can be useful for analysis or visualization.
    """
    all_quadrats = []
    lookup_map = {}  # To track which original polygon each quadrat came from.

    # Subdivide floodplain polygons
    quadrat_counter = 0
    for index, geom in enumerate(poly_gdf.geometry):
        quadrats = quadrat_cut_geometry(geom, quadrat_width)

        all_quadrats.extend(quadrats)

        for q_index in range(len(quadrats)):
            lookup_map[quadrat_counter] = index
            quadrat_counter = quadrat_counter + 1

    quadrat_geoms = np.array(all_quadrats)

    # Build STRtree index
    tree = STRtree(quadrat_geoms)

    return tree, quadrat_geoms, lookup_map


def spatial_join_quadrat_strtree_vectorized(
    roads_gdf: gpd.GeoDataFrame,
    floodplains_gdf: gpd.GeoDataFrame,
    floodplains_subdivided_spatial_index: Tuple[STRtree, np.array, Dict[int, int]],
) -> gpd.GeoDataFrame:
    """
    Performs a spatial join between road segments and floodplain polygons using quadrat subdivision,
    STRtree spatial indexing, and vectorized operations.

    This function efficiently identifies intersections between road segments and floodplains,
    clips the road segments to the intersecting floodplain geometries, and returns a GeoDataFrame
    containing the union of these clipped road segments for each road-floodplain pair.

    Args:
        roads_gdf (gpd.GeoDataFrame):
            GeoDataFrame containing road segment geometries (LineStrings or MultiLineStrings).
        floodplains_gdf (gpd.GeoDataFrame):
            GeoDataFrame containing floodplain polygon geometries (Polygons or MultiPolygons).
        floodplains_subdivided_spatial_index (Tuple[STRtree, np.array, Dict[int, int]]):
            A tuple containing the pre-computed spatial index of the subdivided floodplains.
            It consists of:
                - STRtree: The spatial index of the subdivided floodplain quadrats.
                - np.array: An array of quadrat geometries.
                - Dict[int, int]: A dictionary mapping quadrat indices to floodplain indices.

    Returns:
        gpd.GeoDataFrame:
            A GeoDataFrame with a MultiIndex ('road_idx', 'floodplain_idx') representing the
            road-floodplain pairs and a 'geometry' column containing the union of clipped road segments
            (MultiLineStrings) for each pair.

    Workflow:
        1.  Extracts the STRtree, quadrat geometries, and lookup map from the input spatial index.
        2.  Iterates through each road segment in the 'roads_gdf'.
        3.  Queries the STRtree to find potential intersecting quadrats.
        4.  Performs a vectorized intersection check between the road segment and potential quadrats.
        5.  Identifies the indices of intersecting quadrats and their corresponding floodplain indices.
        6.  Calculates the intersection between the road segment and each intersecting floodplain polygon.
        7.  If the intersection is not empty, it's added to a dictionary grouping intersections by floodplain.
        8.  Calculates the unary union of the clipped road segments for each floodplain.
        9.  Creates a GeoDataFrame from the results, setting the MultiIndex and geometry column.
        10. Returns the resulting GeoDataFrame.

    Index Integrity:
        The resulting GeoDataFrame's MultiIndex ('road_idx', 'floodplain_idx') is created with
        `verify_integrity=True`. This ensures that the index is lexically sorted and that there are
        no duplicate index entries. If duplicate index entries are found, a ValueError is raised.
    """
    tree, quadrat_geoms, lookup_map = floodplains_subdivided_spatial_index

    results = []

    for road_idx in range(len(roads_gdf)):
        road_geom = roads_gdf.iloc[road_idx].geometry

        possible_intersects_idx = tree.query(
            geometry=road_geom,
            predicate="intersects",
            distance=0,
        )

        if len(possible_intersects_idx):
            possible_intersects_geoms = quadrat_geoms[possible_intersects_idx]

            intersects = shapely.intersects(
                road_geom,
                possible_intersects_geoms,
            )

            intersecting_indices = np.array(possible_intersects_idx)[intersects]

            floodplain_road_map: Dict[int, list[shapely.Geometry]] = {}

            # Vectorized intersection and empty check
            floodplain_idxs = [lookup_map[idx] for idx in intersecting_indices]
            floodplain_geoms = floodplains_gdf.loc[floodplain_idxs, "geometry"].values

            intersections = shapely.intersection(
                road_geom,
                floodplain_geoms,
            )

            not_empty = ~shapely.is_empty(intersections)

            for i, floodplain_idx in enumerate(floodplain_idxs):
                if not_empty[i]:
                    if floodplain_idx not in floodplain_road_map:
                        floodplain_road_map[floodplain_idx] = []
                    floodplain_road_map[floodplain_idx].append(intersections[i])

            for floodplain_idx, road_geoms_list in floodplain_road_map.items():
                union_geometry = unary_union(road_geoms_list)
                results.append(
                    {
                        "road_idx": road_idx,
                        "floodplain_idx": floodplain_idx,
                        "geometry": union_geometry,
                    }
                )

    gdf = gpd.GeoDataFrame(data=results, geometry="geometry", crs=roads_gdf.crs)
    if not gdf.empty:
        gdf.set_index(
            ["road_idx", "floodplain_idx"], inplace=True, verify_integrity=True
        )
    return gdf
