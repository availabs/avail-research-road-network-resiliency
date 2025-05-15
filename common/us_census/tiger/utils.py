# TODO: Add constraint: buffer_dist_mi must be integer. Will make file name patterns cleaner.

# NOTE: Including support for nationwide geographies because wider geographic support
#       will provide us more disasters to investigate and use for model training.

# NOTE: Currently omitting the following Census Geographic Entities (geolevels)
#       These include:
#           * Core_Based_Statistical_Area
#           * Combined_Statistical_Area
#           * Metropolitan_Division
#           * ZIP_Code_Tabulation_Area_5_Digit_20
#           * Urban_Area_20
#       Because these geographic levels do not nest within States,
#           they make implementing some functionality much more complicated.

# SEE: https://www2.census.gov/geo/pdfs/reference/geodiagram.pdf

# https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.html
# https://www.census.gov/programs-surveys/geography/guidance/tiger-data-products-guide.html
# https://www2.census.gov/geo/pdfs/maps-data/maps/reference/us_regdiv.pdf
# TIGER/Line with Selected Demographic and Economic Data
#   https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-data.html

# https://github.com/osm-flex/osm-flex/src/osm_flex/clip.py
# https://github.com/osm-flex/osm-flex/blob/87c3031dd0882ebe10f45678e5649132bf05493b/src/osm_flex/clip.py#L204-L230
# https://www2.census.gov/geo/pdfs/maps-data/data/tiger/tgrshp2023/2023_TIGER_GDB_Record_Layouts.pdf

import math
import os
import re
from enum import Enum
from pprint import pprint
from textwrap import dedent
from types import MappingProxyType

import geopandas as gpd
import pyproj
from shapely import box

this_dir = os.path.dirname(os.path.abspath(__file__))

data_dir = os.path.join(this_dir, "../../../data/raw/us_census_bureau/tiger")

# FIXME: Config file? Hardcoding these paths does not seem wise.
#        May be better to use soft links with stable names?
#        Eventually we will need to fetching this data. At that time we will fix this.
country_level_gdb = os.path.join(data_dir, "cb_2023_us_nation_5m.zip")
region_level_gdb = os.path.join(data_dir, "cb_2023_us_region_500k.zip")
# state_level_gdb = os.path.join(data_dir, 'cb_2023_us_state_500k.zip')
us_substategeo_gdb = os.path.join(data_dir, "tlgdb_2024_a_us_substategeo.gdb.zip")
# NOTE: For now only supporting New York State because entire country is too large.
nys_tiger_gdb = os.path.join(data_dir, "tlgdb_2022_a_36_ny.gdb.zip")

FEET_PER_MILE = 5280

# FIXME?: No longer valid since we added Nation and Region geolevels.
#         For now, any generated subsets must be at the State Level or below.
valid_geoid_re = re.compile(r"^(\d{2,15})$|^(\d{15}[A-Z])$")
valid_geoid_prefix_re = re.compile(r"^(\d{2,15})$|^(\d{15}[A-Z])$")

nad83_ft_crs_name_re = re.compile(r"^NAD83 .* \(ftUS\)$")


# Census Geographic Entities
class GeoLevel(Enum):
    COUNTRY = "country"
    REGION = "region"
    STATE = "state"
    COUNTY = "county"
    CENSUS_DESIGNATED_PLACE = "census-designated-place"
    INCORPORATED_PLACE = "incorporated-place"
    COUNTY_SUBDIVISION = "county-subdivision"
    CENSUS_TRACT = "census-tract"
    CENSUS_BLOCK_GROUP = "census-block-group"
    CENSUS_BLOCK = "census-block"


# NOTE: Excluding Country and Region levels because they are not nested within States.
#       This corresponds to the tlgdb_2024_a_us_substategeo.gdb.zip geodatabase.
# GeoLevels at the State level and below.
substate_geolevels = tuple(
    [geolevel for geolevel in GeoLevel][2:]
)  # Excluding Country and Region levels.

# So CLI scripts can map human friendly strings to the appropriate GeoLevel Enum entry.
# NOTE: This is a list of strings, not Enum entries.
#       Can be used as a list of valid geolevel names.
geolevel_names = tuple(g.value for g in GeoLevel)

# So CLI scripts can map human friendly strings to the appropriate GeoLevel Enum entry.
geolevel_name_to_enum_entry = {e.value: e for e in GeoLevel}

# https://www.census.gov/programs-surveys/geography/guidance/geo-identifiers.html
# Mapping of geoid length to the GeoLevels that have that length.
geoid_length_to_geolevels = MappingProxyType(
    dict(
        [
            (0, [GeoLevel.COUNTRY]),
            (1, [GeoLevel.REGION]),
            (2, [GeoLevel.STATE]),
            (5, [GeoLevel.COUNTY]),
            (7, [GeoLevel.CENSUS_DESIGNATED_PLACE, GeoLevel.INCORPORATED_PLACE]),
            (10, [GeoLevel.COUNTY_SUBDIVISION]),
            (11, [GeoLevel.CENSUS_TRACT]),
            (12, [GeoLevel.CENSUS_BLOCK_GROUP]),
            (15, [GeoLevel.CENSUS_BLOCK]),
            (16, [GeoLevel.CENSUS_BLOCK]),
        ]
    )
)

# Mapping of GeoLevels to the Geodatabase handlers.
geolevel_to_gdb = MappingProxyType(
    {
        GeoLevel.COUNTRY: country_level_gdb,
        GeoLevel.REGION: region_level_gdb,
        GeoLevel.STATE: us_substategeo_gdb,
        GeoLevel.COUNTY: us_substategeo_gdb,
        GeoLevel.CENSUS_DESIGNATED_PLACE: us_substategeo_gdb,
        GeoLevel.INCORPORATED_PLACE: us_substategeo_gdb,
        GeoLevel.COUNTY_SUBDIVISION: us_substategeo_gdb,
        GeoLevel.CENSUS_TRACT: us_substategeo_gdb,
        GeoLevel.CENSUS_BLOCK_GROUP: us_substategeo_gdb,
        GeoLevel.CENSUS_BLOCK: nys_tiger_gdb,
    }
)

# Human-friendly geolevel names to the Geodatabase layer names.
geolevel_to_layer_name = MappingProxyType(
    {
        GeoLevel.COUNTRY: "cb_2023_us_nation_5m",
        GeoLevel.REGION: "cb_2023_us_region_500k",
        GeoLevel.STATE: "State",
        GeoLevel.COUNTY: "County",
        GeoLevel.CENSUS_DESIGNATED_PLACE: "Census_Designated_Place",
        GeoLevel.INCORPORATED_PLACE: "Incorporated_Place",
        GeoLevel.COUNTY_SUBDIVISION: "County_Subdivision",
        GeoLevel.CENSUS_TRACT: "Census_Tract",
        GeoLevel.CENSUS_BLOCK_GROUP: "Block_Group",
        GeoLevel.CENSUS_BLOCK: "Block20",
    }
)

us_census_regions = (1, 2, 3, 4)


def geoid_to_gdal_potential_gdbs(geoid):
    """
    Get the potential GDAL handlers for the given geoid.
    """
    # NOTE: While it is currently the case that len(geoid) maps 1-to-1 with Census Geodatabases
    #   that may not hold true if we add more GeoLevels such as
    #   Statistical Areas, Metropolitan Divisions, and Urbanized Areas.
    #   Therefore, we future-proof the code by returning a list rather than a scalar value.
    #   Maybe YAGNI, but it's a small change. Changing APIs is a pain.

    return [
        geolevel_to_gdb[geolevel] for geolevel in geoid_length_to_geolevels[len(geoid)]
    ]


def get_geolevel_for_geoid(geoid):
    """
    Get the geolevel for the given geoid.
    """
    if not re.search(valid_geoid_re, geoid):
        raise BaseException(f"Invalid geoid: {geoid}")

    sql_template = """
        SELECT EXISTS (
            SELECT
                1
            FROM "{layer_name}"
            WHERE ( "GEOID" = '{geoid}' )
            ORDER BY 1
        ) AS geoid_exists
    """

    for vsizip_handler in geoid_to_gdal_potential_gdbs(geoid):
        for geolevel in geoid_length_to_geolevels[len(geoid)]:
            layer_name = geolevel_to_layer_name[geolevel]

            sql = sql_template.format(layer_name=layer_name, geoid=geoid)

            exists = gpd.read_file(
                filename=vsizip_handler,
                engine="pyogrio",
                sql_dialect="SQLITE",
                sql=sql,
                ignore_geometry=True,
            ).loc[0, "geoid_exists"]

            if exists:
                return geolevel

    raise BaseException(f"Unable to find geo level for geoid {geoid}")


def verify_buffer_dist_mi_is_integer(buffer_dist_mi):
    """
    Verify that buffer_dist_mi is an integer.
    """
    if not int(buffer_dist_mi) == buffer_dist_mi:
        raise BaseException("INVARIANT BROKEN: buffer_dist_mi must be an integer.")


def get_geography_region_name(geoid, buffer_dist_mi=None):
    """
    Generate a unique name for the geography region with the given geoid.
    If buffer_dist_mi is provided, the name will include the buffer distance.
    """

    if buffer_dist_mi:
        verify_buffer_dist_mi_is_integer(buffer_dist_mi=buffer_dist_mi)

    geolevel_name = get_geolevel_for_geoid(geoid).value

    buffer_prefix = f"buffer-{buffer_dist_mi}mi-" if buffer_dist_mi else ""

    return f"{buffer_prefix}{geolevel_name}-{geoid}".lower()


def parse_geography_region_name(geography_region_name: str | os.PathLike) -> dict:
    """
    Parse a geography region name into its encoded components.

    The geography_region_name parameter may be the filename of an OSM extract file name,
    in which case the geography_region_name is extracted from the file name
    and this function will then return the parameters used during the extract's creation.

    The canonical format is produced by get_geography_region_name and is one of:
      "buffer-<buffer_dist>mi-<geolevel>-<geoid>" or "<geolevel>-<geoid>".
    OSM extract file names are built by prepending the geography_region_name to an
    input OSM file name. In doing so, the genealogy of extracts is preserved by
    chaining these region names. Therefore, the file name may contain multiple
    such patterns. This function locates and returns the first valid instance.

    An external prefix (e.g. "paved-nonservice-roadways-") may be prepended to the
    canonical region name. This function removes any such prefix before parsing.

    The allowed geolevel values are defined by the GeoLevel enum
    (e.g., "country", "region", "state", "county", "census_tract", etc.).

    Args:
        geography_region_name (str or os.PathLike): The full region name or file name
            (possibly with extra prefixes or chained patterns) to parse.

    Returns:
        dict: A dictionary with the following keys:
              "geolevel": the GeoLevel enum corresponding to the region,
              "geolevel_name": the geolevel name (str),
              "geoid": the geographic identifier (str),
              "buffer_dist_mi": the buffer distance in miles (int, 0 if not present),
              "external_prefix": the external prefix (str, None if not present).

    Raises:
        ValueError: If no valid canonical pattern ("buffer-<buffer_dist>mi-<geolevel>-<geoid>"
                    or "<geolevel>-<geoid>") is found in the input.
    """

    # Lowercase the input to ensure case-insensitive matching.
    s = os.path.basename(geography_region_name).lower()

    allowed_geolevel_pattern = r"(?:%s)" % "|".join(
        re.escape(g) for g in geolevel_names
    )

    # Revised regex:
    # - Matches an optional external prefix consisting of letters and hyphens, followed by a hyphen.
    # - Matches an optional buffer prefix of the form "buffer-<digits>mi-".
    # - Then matches a required geolevel (from allowed values) followed by a hyphen.
    # - Then matches a required geoid.
    # Note: The regex is not anchored so that it finds the first instance in a chained name.
    pattern = re.compile(
        r"^(?P<external_prefix>[a-z-]+-)?"  # Optional external prefix
        r"(?P<buffer>buffer-(?P<buffer_dist>\d+)mi-)?"  # Optional buffer prefix
        r"(?P<geolevel>" + allowed_geolevel_pattern + r")-"  # Required GeoLevel
        r"(?P<geoid>(\d{15}[a-z]|\d{2,15}))"  # Geoid pattern
    )

    match = re.search(pattern, s)

    if match is None:
        raise ValueError(
            f"Invalid geography region name: {geography_region_name}. "
            "geography_region_names must contain the the following pattern: "
            '"buffer-<buffer_dist>mi-<geolevel>-<geoid>" or "<geolevel>-<geoid>" '
            'where geolevel is one of: "country", "region", "state", "county", etc,'
            "and geoid is a valid US Census GEOID."
        )

    groups = match.groupdict()

    return {
        "geolevel": geolevel_name_to_enum_entry[groups["geolevel"]],
        "geolevel_name": groups["geolevel"],
        "geoid": groups["geoid"],
        "buffer_dist_mi": int(groups["buffer_dist"]) if groups["buffer_dist"] else 0,
        "external_prefix": groups["external_prefix"],
    }


def get_geoids_with_prefix_for_geolevel(geoid_prefix, geolevel):
    """
    This function only supports geoid_prefixes for State level and below.
    In the census geographic entities hierarchy, not all entities are
    encloses within states and therefore geoid prefixes do not
    encode the hierarchical nesting of geographic regions.
    """

    if isinstance(geolevel, str):
        geolevel = geolevel_name_to_enum_entry[geolevel]

    if geolevel not in substate_geolevels:
        raise BaseException(
            dedent(f"""
                Invalid geolevel: {geolevel}.
                  This function currently supports only geolevels at the State level and below.
                  Acceptable values:
                    * STATE, 
                    * COUNTY
                    * CENSUS_DESIGNATED_PLACE
                    * INCORPORATED_PLACE
                    * COUNTY_SUBDIVISION
                    * CENSUS_TRACT
                    * CENSUS_BLOCK_GROUP
                    * CENSUS_BLOCK
            """)
        )

    if not re.search(valid_geoid_prefix_re, geoid_prefix):
        raise BaseException(
            f"Invalid geoid prefix: {geoid_prefix}. Currently, only prefixes for State level and below are supported."
        )

    layer_name = geolevel_to_layer_name[geolevel]

    vsizip_handler = geolevel_to_gdb[geolevel]

    geoids = gpd.read_file(
        filename=vsizip_handler,
        engine="pyogrio",
        columns=["GEOID"],
        layer=layer_name,
        where=f"GEOID LIKE '{geoid_prefix}%'",
        ignore_geometry=True,
    )["GEOID"].tolist()

    return geoids


def get_laea_crs_for_region(
    region_gdf: gpd.GeoDataFrame #
) -> pyproj.CRS:
    assert region_gdf.crs.is_geographic, "region_gdf CRS MUST be geographic"

    # Compute the centroid of the region (using the union of all geometries).
    region_centroid = region_gdf.unary_union.centroid

    # Create a Lambert Azimuthal Equal Area projection centered at the region's centroid.
    laea_proj = (
        f"+proj=laea +lat_0={region_centroid.y} +lon_0={region_centroid.x} "
        "+datum=WGS84 +units=m +no_defs"
    )
    laea_crs = pyproj.CRS.from_proj4(laea_proj)

    return laea_crs


def get_buffered_region_gdf(region_gdf, buffer_dist_mi=10):
    """
    Buffer the region GeoDataFrame by the specified distance in miles using a
    locally optimized projection.

    This function creates a copy of the input GeoDataFrame (region_gdf) and
    ensures it is in a geographic CRS (EPSG:4326). It then computes the centroid
    of the region and constructs a Lambert Azimuthal Equal Area (LAEA) projection
    centered at that point. This projection is used to minimize distance distortions
    when buffering the region. The buffer distance (in miles) is converted to meters,
    the geometries are buffered, and then the GeoDataFrame is reprojected back to its
    original CRS.

    Args:
        region_gdf (GeoDataFrame): A GeoDataFrame representing the region boundary.
        buffer_dist_mi (int, optional): The buffer distance in miles. Defaults to 10.

    Returns:
        GeoDataFrame: A new GeoDataFrame with the buffered region geometry in the
                      original CRS.

    Raises:
        ValueError: If region_gdf is empty.
    """

    # Check if the GeoDataFrame is empty.
    if region_gdf.empty:
        raise ValueError("Input region_gdf is empty.")

    # Create a copy to avoid modifying the input.
    buffered_region_gdf = region_gdf.copy()

    # Ensure the GeoDataFrame is in a geographic CRS (EPSG:4326).
    if not buffered_region_gdf.crs.is_geographic:
        buffered_region_gdf.to_crs("EPSG:4326", inplace=True)

    laea_crs = get_laea_crs_for_region(region_gdf=buffered_region_gdf)

    # Re-project the GeoDataFrame to the LAEA projection.
    buffered_region_gdf.to_crs(laea_crs, inplace=True)

    # Convert buffer distance from miles to meters.
    miles_to_meters = 1609.344
    buffer_distance_m = buffer_dist_mi * miles_to_meters

    # Buffer the geometry using the calculated distance (in meters).
    buffered_region_gdf["geometry"] = buffered_region_gdf["geometry"].buffer(
        buffer_distance_m
    )

    # Re-project back to the original CRS.
    buffered_region_gdf.to_crs(region_gdf.crs, inplace=True)

    return buffered_region_gdf


def get_region_boundary_gdf(
    geoid: str,  #
    crs: str = "EPSG:4326",
):
    """
    Get the GeoDataFrame for the region with the given geoid.
    """

    geolevel = get_geolevel_for_geoid(geoid)

    d = gpd.read_file(
        filename=geolevel_to_gdb[geolevel],
        layer=geolevel_to_layer_name[geolevel],
        engine="pyogrio",
        columns=["GEOID"],  # Must provide GEOID because of "where" parameter.
        where=f"GEOID = '{geoid}'",
    )

    # NOTE: All output converted to EPSG:4326.
    #       This may not be the best choice for precision analysis.
    #       ChatGPT recommended "Lambert Conformal Conic".
    d.to_crs(crs, inplace=True)

    if not d.empty:
        return d

    raise Exception(f"No region found for geoid={geoid}")


def get_region_boundary_geometry(geoid):
    """
    Get the geometry for the region with the given geoid.
    """

    region_gdf = get_region_boundary_gdf(geoid=geoid)

    if region_gdf is None:
        raise Exception(f"No region for geoid={geoid}.")

    return region_gdf.loc[0, "geometry"]


## # https://www2.census.gov/geo/pdfs/reference/geodiagram.pdf
## census_geographic_entities_hierarchy = {
##     'country': {
##         'enclosed_by': None,
##     },
##     'region': {
##         'enclosed_by': 'country',
##     },
##     'state': {
##         'enclosed_by': 'region',
##     },
##     'incorporated-place': {
##         'enclosed_by': 'state',
##     },
##     'census-designated-place': {
##         'enclosed_by': 'state',
##     },
##     'county': {
##         'enclosed_by': 'state',
##     },
##     'county-subdivision': {
##         'enclosed_by': 'county',
##     },
##     'census-tract': {
##         'enclosed_by': 'county',
##     },
##     'census-block-group': {
##         'enclosed_by': 'county',
##     },
##     'census-block': {
##         'enclosed_by': 'county',
##     },
## }

## # https://www2.census.gov/geo/pdfs/maps-data/maps/reference/us_regdiv.pdf
## us_census_regions_to_state_geoids = (
##     (), # Place holder so nested tuple indexes correspond to region number.
##
##     # Region  1: Northeast
##     (
##         # New England Division
##         "09",  # Connecticut
##         "23",  # Maine
##         "25",  # Massachusetts
##         "33",  # New Hampshire
##         "44",  # Rhode Island
##         "50",  # Vermont
##         # Middle Atlantic Division
##         "34",  # New Jersey
##         "36",  # New York
##         "42"   # Pennsylvania
##     ),
##
##     # Region 2: Midwest
##     (
##         # East North Central Division
##         "17",  # Illinois
##         "18",  # Indiana
##         "26",  # Michigan
##         "39",  # Ohio
##         "55",  # Wisconsin
##         # West North Central Division
##         "19",  # Iowa
##         "20",  # Kansas
##         "27",  # Minnesota
##         "29",  # Missouri
##         "31",  # Nebraska
##         "38",  # North Dakota
##         "46"   # South Dakota
##     ),
##
##     # Region 3: South
##     (
##         # South Atlantic Division
##         "10",  # Delaware
##         "11",  # District of Columbia
##         "12",  # Florida
##         "13",  # Georgia
##         "24",  # Maryland
##         "37",  # North Carolina
##         "45",  # South Carolina
##         "51",  # Virginia
##         "54",  # West Virginia
##         # East South Central Division
##         "01",  # Alabama
##         "21",  # Kentucky
##         "28",  # Mississippi
##         "47",  # Tennessee
##         # West South Central Division
##         "05",  # Arkansas
##         "22",  # Louisiana
##         "40",  # Oklahoma
##         "48"   # Texas
##     ),
##
##     # Region 4: West
##     (
##         # Mountain Division
##         "04",  # Arizona
##         "08",  # Colorado
##         "16",  # Idaho
##         "30",  # Montana
##         "32",  # Nevada
##         "35",  # New Mexico
##         "49",  # Utah
##         "56",  # Wyoming
##         # Pacific Division
##         "02",  # Alaska
##         "06",  # California
##         "15",  # Hawaii
##         "41",  # Oregon
##         "53"   # Washington
##     )
## )

## def parse_geography_region_name(geography_region_name):
##     """
##     Parse the geography_region_name into its constituent parts.
##     NOTE: This will only work for State level and below. Does not yet support Country or Region levels.
##     """
##
##     # NOTE: mutated_admin_region_id gets chopped up as geography_region_name is parsed.
##     mutated_admin_region_id = geography_region_name
##
##     # NOTE: Geography regions with  buffer_dist_mi=0 will have no prefix.
##     buffer_dist_prefix_re = re.compile(r"^buffer-(\d{1,})mi-")
##
##     buffer_prefix_match = re.search(buffer_dist_prefix_re, geography_region_name)
##
##     buffer_dist_mi = int(buffer_prefix_match.group(1)) if buffer_prefix_match else 0
##
##     if buffer_dist_mi > 0:
##         prefix_len = len(buffer_prefix_match.group(0))
##
##         mutated_admin_region_id = mutated_admin_region_id[prefix_len:]
##
##     geoid_re = re.compile(r"(\d{2,15})$|^(\d{15}[A-Z])$")
##
##     geoid = re.search(geoid_re, mutated_admin_region_id).group(1)
##
##     geolevel_name = mutated_admin_region_id[: -len(f"-{geoid}")]
##
##     geolevel = geolevel_name_to_enum_entry[geolevel_name]
##
##     if geolevel not in GeoLevel:
##         raise BaseException(f"Unrecognized geolevel: {geolevel}")
##
##     return dict(
##         geolevel=geolevel,
##         geolevel_name=geolevel_name,
##         geoid=geoid,
##         buffer_dist_mi=buffer_dist_mi,
##     )

## def get_buffered_region_gdf(region_gdf, buffer_dist_mi=10):
##     """
##     Buffer the region_gdf by buffer_dist_mi miles.
##     """
##     buffered_region_gdf = region_gdf.copy()
##
##     print()
##     print()
##
##     # This is necessary to create the AreaOfInterest object, then use it to query CRS info.
##     if not buffered_region_gdf.crs.is_geographic:
##         print('Setting CRS to "EPSG:4326"')
##         buffered_region_gdf.to_crs("EPSG:4326", inplace=True)
##
##     aoi = pyproj.aoi.AreaOfInterest(*buffered_region_gdf.iloc[0].geometry.bounds)
##
##     # https://gis.stackexchange.com/a/482678
##     all_crs_info = pyproj.database.query_crs_info(
##         auth_name="EPSG",
##         area_of_interest=aoi,
##         contains=True,
##         pj_types="PROJECTED_CRS",
##         allow_deprecated=False,
##     )
##
##     if not all_crs_info:
##         all_crs_info = pyproj.database.query_crs_info(
##             auth_name="EPSG",
##             area_of_interest=aoi,
##             pj_types="PROJECTED_CRS",
##             allow_deprecated=False,
##         )
##
##     if not all_crs_info:
##         raise BaseException(
##             f"Unable to find a suitable CRS for the given area of interest: {aoi}"
##         )
##
##     nad83_ft_crs = sorted(
##         [c for c in all_crs_info if re.search(nad83_ft_crs_name_re, c.name)],
##         key=lambda c: (
##             abs(c.area_of_use.west - c.area_of_use.east)
##             * abs(c.area_of_use.north - c.area_of_use.south)
##         ),
##     )[0]
##
##     buffered_region_gdf.to_crs(f"EPSG:{nad83_ft_crs.code}", inplace=True)
##
##     buffered_region_gdf["geometry"] = buffered_region_gdf["geometry"].buffer(
##         buffer_dist_mi * FEET_PER_MILE
##     )
##
##     buffered_region_gdf.to_crs(region_gdf.crs, inplace=True)
##
##     return buffered_region_gdf
##
##

## def get_buffered_region_gdf(region_gdf, buffer_dist_mi=10):
##     """
##     Buffer the region GeoDataFrame by the specified distance in miles.
##
##     This function creates a copy of the input GeoDataFrame (region_gdf), ensures
##     it is in a geographic CRS (EPSG:4326), and uses its bounds to query for an
##     appropriate projected CRS (preferably NAD83 with feet as units) via pyproj.
##     The GeoDataFrame is then reprojected to that CRS, buffered by converting the
##     given miles to feet, and finally reprojected back to the original CRS. This
##     process aims to provide the highest level of precision when buffering a region.
##
##     Args:
##         region_gdf (GeoDataFrame): A GeoDataFrame representing the region boundary.
##         buffer_dist_mi (int, optional): The buffer distance in miles. Defaults to 10.
##
##     Returns:
##         GeoDataFrame: A new GeoDataFrame with the buffered region geometry in the
##                       original CRS.
##
##     Raises:
##         ValueError: If region_gdf is empty.
##         RuntimeError: If no suitable projected CRS can be found for buffering.
##     """
##
##     # Check if the GeoDataFrame is empty.
##     if region_gdf.empty:
##         raise ValueError("Input region_gdf is empty.")
##
##     # Create a copy to avoid modifying the input.
##     buffered_region_gdf = region_gdf.copy()
##
##     # Ensure the GeoDataFrame is in a geographic CRS (EPSG:4326) to query CRS info.
##     if not buffered_region_gdf.crs.is_geographic:
##         buffered_region_gdf.to_crs("EPSG:4326", inplace=True)
##
##     # Use total_bounds to capture the full extent of the region.
##     bounds = buffered_region_gdf.total_bounds  # [minx, miny, maxx, maxy]
##     aoi = pyproj.aoi.AreaOfInterest(bounds[0], bounds[1], bounds[2], bounds[3])
##
##     # Query for projected CRSs that cover the AOI.
##     all_crs_info = pyproj.database.query_crs_info(
##         auth_name="EPSG",
##         area_of_interest=aoi,
##         contains=True,
##         pj_types="PROJECTED_CRS",
##         allow_deprecated=False,
##     )
##
##     if not all_crs_info:
##         # If no CRSs are found, try again without the 'contains' constraint.
##         # This is required when the AOI is very large, such as the entirety of the NYS.
##         all_crs_info = pyproj.database.query_crs_info(
##             auth_name="EPSG",
##             area_of_interest=aoi,
##             pj_types="PROJECTED_CRS",
##             allow_deprecated=False,
##         )
##
##     if not all_crs_info:
##         raise RuntimeError("Unable to find a suitable CRS for the given area.")
##
##     pprint(all_crs_info)
##
##     # Choose the NAD83-ft CRS with the smallest area-of-use extent.
##     nad83_ft_crs_candidates = [
##         c for c in all_crs_info if re.search(nad83_ft_crs_name_re, c.name)
##     ]
##
##     if not nad83_ft_crs_candidates:
##         raise RuntimeError("No NAD83-ft CRS candidate found.")
##
##     nad83_ft_crs = sorted(
##         nad83_ft_crs_candidates,
##         key=lambda c: (
##             abs(c.area_of_use.west - c.area_of_use.east)
##             * abs(c.area_of_use.north - c.area_of_use.south)
##         ),
##     )[0]
##
##     # Reproject to the chosen CRS (in feet).
##     buffered_region_gdf.to_crs(f"EPSG:{nad83_ft_crs.code}", inplace=True)
##
##     # Buffer by converting miles to feet.
##     buffered_region_gdf["geometry"] = buffered_region_gdf["geometry"].buffer(
##         buffer_dist_mi * FEET_PER_MILE
##     )
##
##     # Reproject back to the original CRS.
##     buffered_region_gdf.to_crs(region_gdf.crs, inplace=True)
##
##     return buffered_region_gdf
##

## def get_buffered_region_gdf(region_gdf, buffer_dist_mi=10):
##     """
##     Buffer the region GeoDataFrame by the specified distance in miles.
##
##     This function creates a copy of the input GeoDataFrame (region_gdf), ensures
##     it is in a geographic CRS (EPSG:4326), and uses its full extent (via total_bounds)
##     to query for an appropriate projected CRS for buffering. Ideally, a NAD83-ft CRS
##     is used, but for large regions (e.g., the entire state of NY) if no such candidate is
##     found, a meter-based CRS is selected instead. The buffer distance is then converted
##     using the appropriate conversion factor (FEET_PER_MILE for foot-based CRSs or
##     1609.344 for meter-based CRSs) before reprojecting back to the original CRS.
##
##     Args:
##         region_gdf (GeoDataFrame): A GeoDataFrame representing the region boundary.
##         buffer_dist_mi (int, optional): The buffer distance in miles. Defaults to 10.
##
##     Returns:
##         GeoDataFrame: A new GeoDataFrame with the buffered region geometry in the
##                       original CRS.
##
##     Raises:
##         ValueError: If region_gdf is empty.
##         RuntimeError: If no suitable projected CRS can be found for buffering.
##     """
##     import re
##
##     import pyproj
##
##     # Check if the GeoDataFrame is empty.
##     if region_gdf.empty:
##         raise ValueError("Input region_gdf is empty.")
##
##     # Create a copy to avoid modifying the input.
##     buffered_region_gdf = region_gdf.copy()
##
##     # Ensure the GeoDataFrame is in a geographic CRS (EPSG:4326).
##     if not buffered_region_gdf.crs.is_geographic:
##         buffered_region_gdf.to_crs("EPSG:4326", inplace=True)
##
##     # Use total_bounds to capture the full extent of the region.
##     bounds = buffered_region_gdf.total_bounds  # [minx, miny, maxx, maxy]
##     aoi = pyproj.aoi.AreaOfInterest(bounds[0], bounds[1], bounds[2], bounds[3])
##
##     # Query for projected CRSs that cover the AOI.
##     all_crs_info = pyproj.database.query_crs_info(
##         auth_name="EPSG",
##         area_of_interest=aoi,
##         contains=True,
##         pj_types="PROJECTED_CRS",
##         allow_deprecated=False,
##     )
##     if not all_crs_info:
##         all_crs_info = pyproj.database.query_crs_info(
##             auth_name="EPSG",
##             area_of_interest=aoi,
##             pj_types="PROJECTED_CRS",
##             allow_deprecated=False,
##         )
##     if not all_crs_info:
##         raise RuntimeError(
##             f"Unable to find a suitable CRS for the given area of interest: {aoi}"
##         )
##
##     sort_crs = pyproj.CRS.from_user_input("ESRI:102009")
##
##     pprint(all_crs_info)
##
##     all_crs_info = sorted(
##         all_crs_info,
##         key=lambda c: pyproj.Transformer.from_crs(f"{c.auth_name}:{c.code}", sort_crs)
##         .transform(
##             box(
##                 c.area_of_use.bounds[0],
##                 c.area_of_use.bounds[1],
##                 c.area_of_use.bounds[2],
##                 c.area_of_use.bounds[3],
##             ),
##         )
##         .area,
##     )
##
##     pprint(all_crs_info)
##
##     areas = [
##         pyproj.transform(
##             pyproj.CRS.from_authority(auth_name=c.auth_name, code=c.code),
##             sort_crs,
##             box(
##                 c.area_of_use.bounds[0],
##                 c.area_of_use.bounds[1],
##                 c.area_of_use.bounds[2],
##                 c.area_of_use.bounds[3],
##             ),
##         ).area
##         for c in all_crs_info
##     ]
##     print(areas)
##
##     if not all_crs_info:
##         raise RuntimeError("No suitable projected CRS found.")
##
##     smallest_area_crs = all_crs_info[0]
##
##     units = pyproj.CRS.from_epsg(smallest_area_crs.code).axis_info[0].unit_name
##
##     if "foot" in units:
##         conv_factor = FEET_PER_MILE  # conversion factor: miles to feet
##     elif "metre" in units:
##         conv_factor = 1609.344
##     else:
##         raise RuntimeError(
##             f"Unsupported unit of measure: {units}. Expected 'foot' or 'metre'."
##         )
##
##     print(
##         f"Using CRS: {smallest_area_crs.name} ({smallest_area_crs.code}) with units: {units}"
##     )
##     raise Exception("DEBUG: Stop here.")
##
##     # Reproject to the chosen CRS.
##     buffered_region_gdf.to_crs(f"EPSG:{smallest_area_crs.code}", inplace=True)
##
##     # Buffer the geometry using the appropriate conversion factor.
##     buffered_region_gdf["geometry"] = buffered_region_gdf["geometry"].buffer(
##         buffer_dist_mi * conv_factor
##     )
##
##     # Reproject back to the original CRS.
##     buffered_region_gdf.to_crs(region_gdf.crs, inplace=True)
##
##     return buffered_region_gdf
##
##
