import sys

sys.path.append("..")

import pandas as pd
from freezegun import freeze_time
from process_verification.verification_template import (
    fetch_number_of_files_in_s3,
    verify_values_within_given_percent,
    verify_number_of_rows_in_table,
    verify_table_column_value_greater_than,
)
from moto import mock_s3_deprecated, mock_s3
import boto3
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from unittest.mock import patch
import unittest


class MyTestCase(unittest.TestCase):
    def test_verify_percent_false(self):
        assert verify_values_within_given_percent(15, 45, 1) == False

    def test_verify_percent_true(self):
        assert verify_values_within_given_percent(100, 100, 1) == True

    @mock_s3
    def file_count_from_s3(self, mocker):
        conn = boto3.resource("s3", region_name="eu-west-1")
        conn.create_bucket(
            Bucket="igloo-data-warehouse-dev-555393537168",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
        )
        s3_client = boto3.client("s3")

        count = fetch_number_of_files_in_s3("flows/outbound/D0379-elective-hh-trial/D0379_20210219_0123456789.txt")
        print(count)
        assert count == 1

    @patch("process_verification.verification_template.get_table_count", return_value=10)
    def test_verify_number_of_rows_in_table_throws_exception_when_incorrect(self, mock_table_count):
        self.assertRaises(RuntimeError, verify_number_of_rows_in_table, "my_table", 0)

    @patch("process_verification.verification_template.get_table_count", return_value=0)
    def test_verify_number_of_rows_in_table_returns_true_when_correct(self, mock_table_count):
        assert verify_number_of_rows_in_table("my_table", 0) is True

    @patch("process_verification.verification_template.get_table_column_value", return_value="2021-06-04T02:00:29.962Z")
    def test_table_column_greater_than_function_throws_exception_when_greater_than(self, mock_table_value):
        self.assertRaises(
            RuntimeError, verify_table_column_value_greater_than, "my_table", "my_column", "2021-06-05T02:00:29.962Z"
        )

    @patch("process_verification.verification_template.get_table_column_value", return_value="2021-06-04T02:00:29.962Z")
    def test_table_column_greater_than_function_returns_true_when_less_than(self, mock_table_value):
        assert verify_table_column_value_greater_than("my_table", "my_column", "2021-06-03T02:00:29.962Z") is True

    @patch("process_verification.verification_template.get_table_column_value", return_value="2021-06-04T02:00:29.962Z")
    def test_table_column_greater_than_function_throws_exception_when_equal_to(self, mock_table_value):
        self.assertRaises(
            RuntimeError, verify_table_column_value_greater_than, "my_table", "my_column", "2021-06-04T02:00:29.962Z"
        )
