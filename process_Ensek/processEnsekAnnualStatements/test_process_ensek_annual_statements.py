from datetime import datetime, timedelta
from lxml import etree
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from moto import mock_s3_deprecated
import csv

from process_ensek_annual_statements import AnnualStatements

from connections.connect_db import get_boto_S3_Connections as s3_con


@mock_s3_deprecated
def test_extract_epc_data():
    s3_bucket_name = 'simulated-bucket'

    s3_connection = S3Connection()
    s3_bucket = s3_connection.create_bucket(s3_bucket_name)

    annual_statements = AnnualStatements()

    # AccountId,Amount,CreatedDate,EnergyType,From,StatementID,To,account_id
    # 10815,597.0,2019-06-30T00:00:29.22,G,2018-06-25T00:00:00,407134,2019-06-25T00:00:00,10815
    # 10815,661.0,2019-06-30T00:00:31.757,E,2018-06-25T00:00:00,407139,2019-06-25T00:00:00,10815
    data = [
        {
            "StatementID": 123456,
            "AccountId": 10815,
            "Amount": 597.0,
            "CreatedDate": "2020-11-04T15:46:26.034Z",
            "From": "2020-11-03T15:46:26.034Z",
            "To": "2020-11-04T15:46:26.034Z",
            "EnergyType": "G"
        },
        {
            "StatementID": 123457,
            "AccountId": 10815,
            "Amount": 123.0,
            "CreatedDate": "2020-11-04T15:46:26.034Z",
            "From": "2020-11-03T15:46:26.034Z",
            "To": "2020-11-04T15:46:26.034Z",
            "EnergyType": "E"
        }
    ]
    account_id = 10815

    k = s3_con(s3_bucket_name)
    dir_s3 = {
        "s3_key": {
            "AnnualStatements": "stage1/AnnualStatements/",
        },
    }

    annual_statements.extract_data(data, account_id, k, dir_s3)

    # extract_tariff_history_json(data, account_id, k, dir_s3)

    expected_s3_key = 'stage1/AnnualStatements/annual_statements_{}.csv'.format(
        account_id)

    # Verify that the file created in S3 has the correct contents
    k = Key(s3_bucket)
    k.key = expected_s3_key
    csv_lines = k.get_contents_as_string(encoding='utf-8')

    # Construct a CSV reader to parse the file data for us, and verify that the
    # column headers are correct.
    reader = csv.reader(csv_lines.split('\n'), delimiter=',')

    column_headers = next(reader)
    assert(column_headers == ['AccountId', 'Amount', 'CreatedDate',
                              'EnergyType', 'From', 'StatementID', 'To', 'account_id'])
    line1 = next(reader)
    line2 = next(reader)
    assert(line1 == ['10815', '597.0', '2020-11-04T15:46:26.034Z', 'G', '2020-11-03T15:46:26.034Z', '123456', '2020-11-04T15:46:26.034Z', '10815'])
    assert(line2 == ['10815', '123.0', '2020-11-04T15:46:26.034Z', 'E', '2020-11-03T15:46:26.034Z', '123457', '2020-11-04T15:46:26.034Z', '10815'])
