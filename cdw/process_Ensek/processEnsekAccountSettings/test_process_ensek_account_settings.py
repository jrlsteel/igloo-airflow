from datetime import datetime, timedelta
from lxml import etree
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from moto import mock_s3_deprecated
import csv
import json
import os
import sys
import pandas

from cdw.process_Ensek.processEnsekAccountSettings.process_ensek_account_settings import AccountSettings

from cdw.connections.connect_db import get_boto_S3_Connections as s3_con


@mock_s3_deprecated
def test_extract_data():
    s3_bucket_name = "simulated-bucket"

    s3_connection = S3Connection()
    s3_bucket = s3_connection.create_bucket(s3_bucket_name)

    account_settings = AccountSettings()

    data = {
        "AccountID": 1831,
        "BillDayOfMonth": 17,
        "BillFrequencyMonths": 1,
        "TopupWithEstimates": True,
        "SendEmail": True,
        "SendPost": False,
        "NextBillDate": "2019-10-17T00:00:00",
        "NextBillDay": 17,
        "NextBillMonth": 10,
        "NextBillYear": 2019,
    }

    k = s3_con(s3_bucket_name)
    dir_s3 = {
        "s3_key": {
            "AccountSettings": "stage1/AccountSettings/",
        },
    }

    account_settings.extract_data(data, 1831, k, dir_s3)

    # Verify that the files created in S3 have the correct contents
    k = Key(s3_bucket)
    k.key = "stage1/AccountSettings/account_settings_1831.csv"
    csv_lines = k.get_contents_as_string(encoding="utf-8")

    # Construct a CSV reader to parse the file data for us, and verify that the
    # column headers are correct.
    reader = csv.reader(csv_lines.split("\n"), delimiter=",")

    column_headers = next(reader)

    assert column_headers == [
        "AccountID",
        "BillDayOfMonth",
        "BillFrequencyMonths",
        "NextBillDate",
        "NextBillDay",
        "NextBillMonth",
        "NextBillYear",
        "SendEmail",
        "SendPost",
        "TopupWithEstimates",
        "account_id",
    ]
    assert list(reader) == [
        ["1831", "17", "1", "2019-10-17T00:00:00", "17", "10", "2019", "True", "False", "True", "1831"],
        [],
    ]
