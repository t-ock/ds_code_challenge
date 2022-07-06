"""
Utility functions that are used by other files and in scripts
as the tasks all require a boto_client, retrieval of credentials for scripts or other tasks.
- s3select_to_file was added as it was used in manual testing
"""
import logging
import timeit
import sys

from dataclasses import dataclass

import botocore.exceptions
import requests
import json
import boto3
import botocore

from loguru import logger



# constants that are used in multiple tasks
CREDS_URL = "https://cct-ds-code-challenge-input-data.s3.af-south-1.amazonaws.com/ds_code_challenge_creds.json"
DS_CHALLENGE_BUCKET = "cct-ds-code-challenge-input-data"
SECRETS_OBJECT = "ds_code_challenge_creds.json"
REGION = "af-south-1"
CITY_HEX_8_VALIDATE = "city-hex-polygons-8.geojson"
CITY_HEX_8_10_SOURCE = "city-hex-polygons-8-10.geojson"


def retrieve_credentials():
    """Retrieve credentials from s3 and saves it as secrets.json"""
    try:
        res = requests.get(CREDS_URL)
        with open("secrets.json", "w") as secrets_file:
            secrets_file.write(res.text)

    except Exception:
        logger.exception("Failed")


def boto_client(access_key=None, secret_key=None):
    """Configures a boto3 client with the credentials
    specified in the secrets.json file
    :type access_key: str
        Override access_key specified in secrets.json
    :type secret_key: str
        Override secret_key specified in secrets.json"""

    with open("secrets.json") as secrets_file:
        secrets_json = json.loads(secrets_file.read())
        access_key = access_key or secrets_json["s3"]["access_key"]
        secret_key = secret_key or secrets_json["s3"]["secret_key"]

    client = boto3.client(
        "s3",
        region_name=REGION,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    return client


def s3select_to_file(s3client, bucket, query, key, filename):
    """Helper function that saves s3 select data to a file"""

    try:
        starttime = timeit.default_timer()
        result = s3client.select_object_content(
            ExpressionType="SQL",
            Expression=query,
            Bucket=bucket,
            Key=key,
            InputSerialization={"JSON": {"Type": "DOCUMENT"}},
            OutputSerialization={"JSON": {}},
        )
        diff_time = timeit.default_timer() - starttime
        logger.info(f"Time taken to retrieve data from aws  : {diff_time}")
    except botocore.exceptions.EndpointConnectionError:
        logger.exception("failed to issue s3 select ")

    except botocore.exceptions.ClientError:
        logger.exception(f"Failed to issue s3 select on bucket: {bucket} ")

    else:
        records = []
        for event in result["Payload"]:
            if "Records" in event:
                records.append(event["Records"]["Payload"])
            elif "Stats" in event:
                stats = event["Stats"]["Details"]
                logger.debug(f"AWS S3 Stats {stats}")

        file_str = "".join(r.decode("utf-8") for r in records)

        starttime = timeit.default_timer()
        with open(filename, "w") as file_:
            file_.write(file_str.strip("\n"))
        diff_time = timeit.default_timer() - starttime
        logger.info(f"Time taken to write data to file  : {diff_time}")

        return True


def set_loguru_log_level(_logger, level="INFO"):
    _logger.remove()
    _logger.add(sys.stderr, level=level)


