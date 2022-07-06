import gzip
import h3

import requests

import geopandas as gpd
import pandas as pd
from pandas_ods_reader import read_ods
from loguru import logger

from utils import boto_client, DS_CHALLENGE_BUCKET, set_loguru_log_level

# Set logger to info to ignore debugging
set_loguru_log_level(logger, "INFO")

def get_bellville_south_centroid():
    """GeoJSON data for Bellville South Suburb is retrieved from coct opendata arcgis platform
    then the centroid latitude and longitude is accessed from the geopandas.GeoDataFrame of that GeoJSON file.
    """
    # Data retrieved from
    # https://odp-cctegis.opendata.arcgis.com/datasets/cctegis::official-planning-suburbs/about
    # Access city of cape town arcgis
    arcgis_url = "https://citymaps.capetown.gov.za/agsext1/rest/services/Theme_Based/Open_Data_Service/MapServer/75/query?where=OFC_SBRB_NAME='BELLVILLE SOUTH'&f=GeoJSON"

    res = requests.get(arcgis_url)

    gdf = gpd.GeoDataFrame.from_features(res.json()["features"])
    longitude = gdf.centroid.x.iloc[0]
    latitude = gdf.centroid.y.iloc[0]

    return latitude, longitude


def calculate_point_dist(x, latitude, longitude):
    if not x[-3] or not x[-2]:
        return None
    val = h3.point_dist((latitude, longitude), (x[-3], x[-2]), unit="km")

    # The length of one minute of latitude is 1.853km
    # see https://en.wikipedia.org/wiki/Latitude#Length_of_a_degree_of_latitude
    # set it to
    if val >= 1.854:
        val = None
    return val


def create_subset_bellvile_south_1min(latitude, longitude):
    s3_client = boto_client()
    # Dowload files from aws
    s3_client.download_file(DS_CHALLENGE_BUCKET, "sr.csv.gz", "sr.csv.gz")
    s3_client.download_file(DS_CHALLENGE_BUCKET, "sr_hex.csv.gz", "sr_hex.csv.gz")

    with gzip.open("sr_hex.csv.gz") as f_in:
        sr_hex = pd.read_csv(f_in)

    distance = sr_hex.apply(
        calculate_point_dist, axis=1, args=(latitude, longitude)
    )
    sr_hex["distance"] = distance
    sr_hex = sr_hex[~sr_hex["distance"].isna()]
    return sr_hex


def clean_column_name(x):
    if x:
        x = str(x).replace(" ", "_").replace("/", "").lower()
    else:
        x = ""
    return x


def clean_ods(x):
    if x in ["<Samp", "NoData"]:
        return None
    return x


def get_bellville_ods():

    ods_url = "https://www.capetown.gov.za/_layouts/OpenDataPortalHandler/DownloadHandler.ashx?DocumentName=Wind_direction_and_speed_2020.ods&DatasetDocument=https%3A%2F%2Fcityapps.capetown.gov.za%2Fsites%2Fopendatacatalog%2FDocuments%2FWind%2FWind_direction_and_speed_2020.ods"

    res = requests.get(ods_url, allow_redirects=True)

    with open("Wind_direction_and_speed_2020.ods", "wb") as dl_file:
        dl_file.write(res.content)

    wind_speed_df = read_ods("Wind_direction_and_speed_2020.ods")
    new_header = (
            wind_speed_df.iloc[1].apply(clean_column_name)
            + wind_speed_df.iloc[2].apply(clean_column_name)
            + wind_speed_df.iloc[3].apply(clean_column_name)
    )
    # remove rows that are not used.
    wind_speed_df = wind_speed_df[4:-8]

    wind_speed_df.columns = new_header
    # clean ods file
    wind_speed_df = wind_speed_df.applymap(clean_ods)
    wind_speed_df.to_csv("wind_direction_and_speed_2020.csv")

    bellville_wind_speed_df = pd.DataFrame()
    bellville_wind_speed_df["date_time"] = wind_speed_df["date_&_time"]
    bellville_wind_speed_df["bellville_south_aqm_site_wind_dir"] = wind_speed_df[
        "bellville_south_aqm_sitewind_dir_vdeg"
    ]
    bellville_wind_speed_df["bellville_south_aqm_sitewind_speed"] = wind_speed_df[
        "bellville_south_aqm_sitewind_speed_vms"
    ]
    return bellville_wind_speed_df


def main():
    bellville_south_latitude, bellville_south_longitude = get_bellville_south_centroid()
    logger.info(f"bellville south centroid (lat, long) {bellville_south_latitude, bellville_south_longitude} ")
    df = create_subset_bellvile_south_1min(bellville_south_latitude, bellville_south_longitude)
    df2 = get_bellville_ods()
    df2 = df2.iloc[:, 1:]

    df["creation_timestamp"] = pd.to_datetime(df["creation_timestamp"])
    # df2["date_time"] = pd.to_datetime(df2["date_time"])

    # data = pd.merge_asof(
    #     df,
    #     df2,
    #     left_on="creation_timestamp",
    #     right_on="date_time",
    #     by="reference_number",
    # ).drop(columns=["date_time"], axis=1)
    #
    # data.to_csv("final.csv")


if __name__ == '__main__':
    main()