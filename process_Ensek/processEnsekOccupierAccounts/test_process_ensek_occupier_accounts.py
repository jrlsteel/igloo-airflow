from datetime import datetime, timedelta
from lxml import etree
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from moto import mock_s3_deprecated
import csv

from process_ensek_occupier_accounts import OccupierAccounts

from connections.connect_db import get_boto_S3_Connections as s3_con
from freezegun import freeze_time

@mock_s3_deprecated
@freeze_time("2018-04-03T12:30:00.123456+01:00")
def test_extract_internal_data_response():
    s3_bucket_name = 'simulated-bucket'

    s3_connection = S3Connection()
    s3_bucket = s3_connection.create_bucket(s3_bucket_name)

    account_transactions = OccupierAccounts()

    data = {
        '$type': 'System.Collections.ObjectModel.Collection`1[[System.Object, mscorlib]], mscorlib',
        '$values': [
            {
                '$type': 'System.Collections.Generic.Dictionary`2[[System.String, mscorlib],[System.Object, mscorlib]], mscorlib',
                'AccountID': 5531,
                'COT Date': '2017-12-28T00:00:00',
                'Days Since COT Date': 1043,
                'Current Balance': 0.0
            },
            {
                '$type': 'System.Collections.Generic.Dictionary`2[[System.String, mscorlib],[System.Object, mscorlib]], mscorlib',
                'AccountID': 6580,
                'COT Date': '2018-02-13T00:00:00',
                'Days Since COT Date': 996,
                'Current Balance': 124.54
            },
            {
                '$type': 'System.Collections.Generic.Dictionary`2[[System.String, mscorlib],[System.Object, mscorlib]], mscorlib',
                'AccountID': 6850,
                'COT Date': '2018-02-23T00:00:00',
                'Days Since COT Date': 986,
                'Current Balance': 0.0
            },
        ]
    }

    k = s3_con(s3_bucket_name)
    dir_s3 = {
        "s3_key": {
            "OccupierAccounts": "stage2/stage2_OccupierAccounts/"
        },
    }

    account_transactions.extract_internal_data_response(
        data, k, dir_s3)

    expected_s3_key = 'stage2/stage2_OccupierAccounts/occupier_accounts.csv'

    # Verify that the file created in S3 has the correct contents
    k = Key(s3_bucket)
    k.key = expected_s3_key
    csv_lines = k.get_contents_as_string(encoding='utf-8')

    # Construct a CSV reader to parse the file data for us, and verify that the
    # column headers are correct.
    reader = csv.reader(csv_lines.split('\n'), delimiter=',')

    column_headers = next(reader)
    
    assert(column_headers == ['AccountID','COT Date','Current Balance','Days Since COT Date','etl_change'])
    assert(list(reader) == [['5531', '2017-12-28T00:00:00', '0.0', '1043', '2018-04-03 11:30:00.123456'], ['6580', '2018-02-13T00:00:00', '124.54', '996', '2018-04-03 11:30:00.123456'], ['6850', '2018-02-23T00:00:00', '0.0', '986', '2018-04-03 11:30:00.123456'], []])
