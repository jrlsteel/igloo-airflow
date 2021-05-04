import sys

sys.path.append("..")

import datetime
from connections.connect_db import get_boto_S3_Connections, get_redshift_connection
import common.directories
import time
import traceback
import logging
import common.utils
from common.utils import IglooLogger
import boto3
import os
import smart_open
from conf import config
import sentry_sdk
import boto
import pandas_redshift as pr
import subprocess


# Logging Configuration

iglog = IglooLogger(source="AirflowVerification")


# Verification Functions

def verify_values_within_given_percent(input_var, output_var, percent):
    try:
        print(percent)
        is_verified = False
        min_int = input_var * (1 - (percent * 0.01))
        max_int = input_var * (1 + (percent * 0.01))
        if output_var >= min_int and output_var <= max_int:
            is_verified = True
            iglog.in_prod_env("Values are within given percent: {}%".format(percent))
        else:
            iglog.in_prod_env(
                "Values: {} ; {} ,  are not within given percent: {}%".format(
                    input_var, output_var, percent
                )
            )
        return is_verified
    except Exception as e:
        iglog.in_prod_env(traceback.format_exc())
        raise e


def fetch_number_of_files_in_s3(s3_prefix):
    """
    Returns an integer representing the number of files in a s3 directory.
    """
    try:
        count = 0
        directory = common.utils.get_dir()

        s3_bucket = directory["s3_bucket"]

        k = get_boto_S3_Connections(s3_bucket)

        aws_access_key_id = config.s3_config["access_key"]
        aws_secret_access_key = config.s3_config["secret_key"]

        s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

        s3_response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)
        count = s3_response["KeyCount"]
        iglog.in_prod_env(s3_response["KeyCount"])
        # when working when will list all items in a directory, filter variiable should be an input as well

        iglog.in_prod_env("and the count is " + str(count))
        return count
    except Exception as e:
        iglog.in_prod_env(traceback.format_exc())
        raise e


def verify_seventeen_new_files_in_s3(s3_prefix):
    try:
        directory = common.utils.get_dir()
        s3_bucket = directory["s3_bucket"]

        aws_access_key_id = config.s3_config["access_key"]
        aws_secret_access_key = config.s3_config["secret_key"]

        s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

        s3_response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)
        count = s3_response["KeyCount"]

        verified = False

        if not count == 17:
            iglog.in_prod_env(
                "Count failed, not equal to 17, count was: {}.".format(count)
            )
            return verified
        else:
            number_of_files_processed = 0
            if s3_response["KeyCount"] > 0:
                keys = [x["Key"] for x in s3_response["Contents"]]
                for key in keys:
                    bucket_obj = s3_client.get_object(Bucket=s3_bucket, Key=key)
                    last_modified = bucket_obj["ResponseMetadata"]["HTTPHeaders"][
                        "last-modified"
                    ]
                    parsed_last_modified = datetime.datetime.strptime(
                        last_modified, "%a, %d %b %Y %H:%M:%S %Z"
                    )

                    if parsed_last_modified.date() == datetime.date.today():
                        number_of_files_processed += 1
                if number_of_files_processed == 17:
                    verified = True
                else:
                    verified = False
            else:
                iglog.in_prod_env("No files in directory")
                verified = False
                # if not isToday(last_modified):
                #     iglog.in_prod_env(key, "was not modified today. Validation failed.")
                #     return False
        return verified
    except Exception as e:
        iglog.in_prod_env(traceback.format_exc())
        raise e


# For Ref Verification

def get_table_count(table_name):
    """
    Counts the number of rows for a given redshgift table name and returns an integer.
    """
    try:
        get_redshift_connection()
        sql = """select count(1) as Count from {}""".format(table_name)
        dataframe = pr.redshift_to_pandas(sql)
        count = dataframe.iloc[0]["count"]
        iglog.in_prod_env('Table count for: ' + str(table_name) + "is: " + str(count))
        return count
    except Exception as e:
        iglog.in_prod_env(traceback.format_exc())
        raise e


def ref_verification_step(**kwargs):
    """
    Verifcation step, which will create and format a SQL Expression to select a count from a given table
    This count willl be used to validate the previous previous ETL Step.

    Expected Args = tablename=expected_count_as_int
    """
    try:
        verified = False
        for arg in kwargs:
            count = get_table_count(arg)
            iglog.in_prod_env("table: {} : count: {}".format(arg, count))
            if not count > 0:
                iglog.in_prod_env(
                    "Failed to verify count for {} - count was {}".format(arg, str(count))
                )
                verified = False
            else:
                iglog.in_prod_env("Successfully verified: {}".format(arg))
                verified = True
        return verified
    except Exception as e:
        iglog.in_prod_env(traceback.format_exc())
        raise e

# Unused but potentially useful for directories with a few pages of data
def iterate_through_multiple_pages_of_s3(s3_prefix):
    try:
        directory = common.utils.get_dir()
        s3_bucket = directory["s3_bucket"]

        aws_access_key_id = config.s3_config["access_key"]
        aws_secret_access_key = config.s3_config["secret_key"]

        s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )
        iglog.in_prod_env(
            "Iterating through directory: {}{}".format(s3_bucket, s3_prefix)
        )
        paginator = s3_client.get_paginator("list_objects_v2")
        result = paginator.paginate(Bucket=s3_bucket, Prefix=s3_prefix)
        counter = 0
        number_of_successful_files = 0
        for page in result:
            if "Contents" in page:
                for key in page["Contents"]:
                    keyString = key["Key"]
                    bucket_obj = s3_client.get_object(Bucket=s3_bucket, Key=keyString)
                    last_modified = bucket_obj["ResponseMetadata"]["HTTPHeaders"][
                        "last-modified"
                    ]
                    parsed_last_modified = datetime.datetime.strptime(
                        last_modified, "%a, %d %b %Y %H:%M:%S %Z"
                    )

                    if parsed_last_modified.date() == datetime.date.today():
                        number_of_successful_files += 1
                        iglog.in_prod_env(
                            "Number of criteria-met files: ", number_of_successful_files
                        )
                    if counter % 100 == 0:
                        iglog.in_prod_env(keyString)
                        iglog.in_prod_env("Number of files processed: ", counter)
                    counter += 1

    except Exception as e:
        iglog.in_prod_env(traceback.format_exc())
        raise e


def verify_new_api_response_files_in_s3_directory(
    search_filter, expected_value, s3_prefix, percent
):
    """
    This verification step relies on the AWS command line. 
    It counts the number of files that match a 'grep' search in a given directory and compares it with an expected value. 
    The percent should that each the values should be in range of should also be specified.
    """
    try:
        directory = common.utils.get_dir()
        s3_bucket = directory["s3_bucket"]
        query_path = s3_bucket + "/" +  s3_prefix
        iglog.in_prod_env("Filtering for files containting: {}, in directoy: {}".format(search_filter, query_path))
        command = "aws s3 ls s3://{} | grep -1 '{}' | wc -l".format(
            query_path, search_filter
        )
        result = subprocess.check_output(command, shell=True)
        num_matching_files = int(result.decode().strip())
        iglog.in_prod_env("Shell output: " + str(num_matching_files))
        return verify_values_within_given_percent(int(expected_value), num_matching_files, int(percent))
    except Exception as e:
        iglog.in_prod_env(traceback.format_exc())
        raise e
