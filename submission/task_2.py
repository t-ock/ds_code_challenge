"""This script does the following.
- Retrieves sr.csv.gz and sr_hex.csv from AWS
- calculates the error rate based on missing latitude and longitude values in sr.csv
- calculates the h3_index for each service request in sr.csv
- validates the sr.csv with h3_index information against sr_hex.csv
"""
import gzip
import timeit

import h3
import pandas as pd
from loguru import logger

from utils import DS_CHALLENGE_BUCKET, boto_client, set_loguru_log_level

# Set error threshold to 50% for this example.
# I chose 50% as it is based on my naive error calculation that the max errors would be around 45% of the samples
# in the sr.csv

ERROR_THRESHOLD = 0.5

# Set logger to info to ignore debugging
set_loguru_log_level(logger, "INFO")


def calculate_error(sr_data_frame):
    """Naive method to calculate potential
    join error rate based on missing
    latitude and longitude values in sr.csv file
    """
    # TODO improve accuracy of calculating errors.

    lat_error_mean = sr_data_frame["latitude"].isna().mean()
    lon_error_mean = sr_data_frame["longitude"].isna().mean()

    min_error = max(lat_error_mean, lon_error_mean)
    max_error = lat_error_mean + lon_error_mean
    return min_error, max_error


def calculate_h3_index(x):
    """Function that uses the h3 library to calculate the h3 index given the
    latitude and longitude supposed.
    This function is used in .apply().
    This is a bit of a hack.
    """
    if not x[-2] or not x[-1]:
        return 0
    return h3.geo_to_h3(x[-2], x[-1], 8)


def main():
    start_time = timeit.default_timer()
    s3_client = boto_client()
    s3_client.download_file(DS_CHALLENGE_BUCKET, "sr.csv.gz", "sr.csv.gz")
    s3_client.download_file(DS_CHALLENGE_BUCKET, "sr_hex.csv.gz", "sr_hex.csv.gz")

    with gzip.open("sr.csv.gz") as f_in:
        result = pd.read_csv(f_in)
    time_diff = timeit.default_timer() - start_time
    logger.info(f"Time taken to download files from AWS S3 {time_diff}")

    before_exe_time = timeit.default_timer()
    min_error, max_error = calculate_error(result)
    time_diff = timeit.default_timer() - before_exe_time
    logger.info(f"Time taken calculate errors {time_diff}")

    if max_error < ERROR_THRESHOLD:

        before_exe_time = timeit.default_timer()
        h3_index = result.apply(calculate_h3_index, axis=1)
        time_diff = timeit.default_timer() - before_exe_time
        logger.info(f"Time taken set h3index {time_diff}")

        result["h3_level8_index"] = h3_index
        # remove indexes added
        result = result.iloc[:, 1:]

        with gzip.open("sr_hex.csv.gz") as v_in:
            validate = pd.read_csv(v_in)

        before_exe_time = timeit.default_timer()

        # compare Dataframes
        diff_df = result.compare(validate)

        if diff_df.empty:
            logger.info(
                "Data sr.csv transformed with h3_level8_index was validated against sr_hex.csv"
            )
            result.to_csv("sr_transformed_hex.csv", index=False)
            logger.info("File saved to sr_transformed_hex.csv")

        else:
            logger.error("Dataframes are not equal, difference below:")
            logger.error(diff_df)

        time_diff = timeit.default_timer() - before_exe_time
        logger.info(f"Time taken to validate data {time_diff}")

if __name__ == "__main__":
    start_time = timeit.default_timer()
    main()
    time_diff = timeit.default_timer() - start_time
    logger.info(f"Total time taken {time_diff}")
