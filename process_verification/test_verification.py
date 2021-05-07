import sys

sys.path.append("..")

import pandas as pd
from freezegun import freeze_time
import common.utils
import smart_open
from .verification_template import (
    fetch_number_of_files_in_s3,
    verify_values_within_given_percent
)
from moto import mock_s3_deprecated, mock_s3
import boto3
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import pytest
import datetime
import os
import unittest.mock
import paramiko
import sentry_sdk


def test_verify_percent_false():
    assert(verify_values_within_given_percent(15,45,1) == False)

def test_verify_percent_true():
    assert(verify_values_within_given_percent(100,100, 1) == True)

@mock_s3
def file_count_from_s3(mocker):
    conn = boto3.resource("s3", region_name="eu-west-1")
    conn.create_bucket(
        Bucket="igloo-data-warehouse-dev-555393537168",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
    )
    s3_client = boto3.client("s3")


    count = fetch_number_of_files_in_s3("flows/outbound/D0379-elective-hh-trial/D0379_20210219_0123456789.txt")
    print(count)
    assert(count == 1)