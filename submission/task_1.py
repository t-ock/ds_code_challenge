"""This script does the following.
- retrieves data from AWS using S3 SELECT command to read in the H3 resolution 8 data from city-hex-polygons-8-10.geojson.
- Downloads the city-hex-polygons-8.geojson from aws.
- Validates the result set against the city-hex-polygons-8.geojson file to validate your work.
- logs time taken for operations
"""
import timeit

import botocore
import geopandas
from loguru import logger

from utils import (CITY_HEX_8_10_SOURCE, CITY_HEX_8_VALIDATE,
                   DS_CHALLENGE_BUCKET, boto_client, s3select_to_file,
                   set_loguru_log_level)

# Set logger to info to ignore debugging
set_loguru_log_level(logger, "INFO")


def main():
    # Create s3_client for accessing data stored on S3
    s3_client = boto_client()

    if s3_client:
        # Create query to select all resolution 8 data from city-hex-polygons-8-10.geojson data file
        res_8_query = "SELECT * from S3Object[*].features[*] features  where features.properties.resolution = 8"

        # retrieve data from s3 then continue

        if s3select_to_file(
            s3_client,
            DS_CHALLENGE_BUCKET,
            res_8_query,
            CITY_HEX_8_10_SOURCE,
            "city-hex-polygons-8-select-result.data",
        ):
            try:
                # Fetch Validated data
                start_time = timeit.default_timer()
                s3_client.download_file(
                    DS_CHALLENGE_BUCKET, CITY_HEX_8_VALIDATE, CITY_HEX_8_VALIDATE
                )
                total_diff = timeit.default_timer() - start_time
                logger.info(f"Time taken to download files from AWS S3 {total_diff}")
            except botocore.exceptions.ClientError:
                logger.exception(f"Failed to download {CITY_HEX_8_VALIDATE}")

            else:
                start_time = timeit.default_timer()

                # Create geopandas DataFrames from results and validated data
                s3select_hex_8_gdf = geopandas.GeoDataFrame.from_file(
                    "city-hex-polygons-8-select-result.data",
                    geometry="geometry",
                    crs="EPSG:4326",
                )
                validate_hex_8_gdf = geopandas.GeoDataFrame.from_file(
                    CITY_HEX_8_VALIDATE, geometry="geometry", crs="EPSG:4326"
                )

                # clean dataframe for comparison by removing the resolution column
                clean_s3select_hex_8_gdf = s3select_hex_8_gdf.drop(
                    columns=["resolution"], axis=1
                )

                diff_gdf = clean_s3select_hex_8_gdf.compare(validate_hex_8_gdf)

                if diff_gdf.empty:
                    logger.info(
                        f"Data extracted via s3select from {CITY_HEX_8_10_SOURCE} is validated against {CITY_HEX_8_VALIDATE}"
                    )
                    clean_s3select_hex_8_gdf.to_file("validated_data.geojson", driver="GeoJSON")
                    logger.info("File saved to validated_data.geojson")

                else:
                    logger.error("Dataframes are not equal, difference below:")
                    logger.error(diff_gdf)
                total_diff = timeit.default_timer() - start_time
                logger.info(f"Time taken to validate and clean data {total_diff}")

if __name__ == "__main__":
    start_time = timeit.default_timer()
    main()
    total_diff = timeit.default_timer() - start_time
    logger.info(f"Total time taken {total_diff}")
