from datetime import datetime, timedelta
from lxml import etree
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from moto import mock_s3, mock_s3_deprecated
import unittest
from multiprocessing import Manager, Value
import csv

from process_ensek_internal_estimates import InternalEstimates

from connections.connect_db import get_boto_S3_Connections as s3_con


@mock_s3_deprecated
def test_extract_internal_data_response_elec():
    s3_bucket_name = "simulated-bucket"

    s3_connection = S3Connection()
    s3_bucket = s3_connection.create_bucket(s3_bucket_name)

    internal_estimates = InternalEstimates()

    # Expected CSV output:
    # EffectiveFrom,EffectiveTo,EstimationValue,IsLive,MPAN,RegisterId,SerialNumber,account_id
    # 2020-10-21T00:00:00,9999-12-31T23:59:59.9999999,7570.0,True,2700000115580,N,D10W619612,100001
    # 2020-10-15T00:00:00,9999-12-31T23:59:59.9999999,7733.3,True,2700000115580,N,D10W619612,100001
    # 2020-09-21T00:00:00,9999-12-31T23:59:59.9999999,8324.0,True,2700000115580,N,D10W619612,100001
    # 2019-12-19T00:00:00,9999-12-31T23:59:59.9999999,3450.9,True,2700000115580,N,D10W619612,100001
    # 2019-10-22T00:00:00,9999-12-31T23:59:59.9999999,3450.9,True,2700000115580,N,D10W619612,100001
    #
    # As this is an internal API, the response is not documented, it seems.
    # This is a best guess at what the source data might look like.
    data = [
        {
            "EffectiveTo": "9999-12-31T23:59:59.9999999",
            "EffectiveFrom": "2020-10-21T00:00:00",
            "EstimationValue": 7570.0,
            "IsLive": True,
            "MPAN": "2700000115580",
            "RegisterId": "N",
            "SerialNumber": "D10W619612",
        },
        {
            "EffectiveTo": "9999-12-31T23:59:59.9999999",
            "EffectiveFrom": "2020-10-15T00:00:00",
            "EstimationValue": 7733.3,
            "IsLive": True,
            "MPAN": "2700000115580",
            "RegisterId": "N",
            "SerialNumber": "D10W619612",
        },
        {
            "EffectiveTo": "9999-12-31T23:59:59.9999999",
            "EffectiveFrom": "2020-09-21T00:00:00",
            "EstimationValue": 8324.0,
            "IsLive": True,
            "MPAN": "2700000115580",
            "RegisterId": "N",
            "SerialNumber": "D10W619612",
        },
        {
            "EffectiveTo": "9999-12-31T23:59:59.9999999",
            "EffectiveFrom": "2019-12-19T00:00:00",
            "EstimationValue": 3450.9,
            "IsLive": True,
            "MPAN": "2700000115580",
            "RegisterId": "N",
            "SerialNumber": "D10W619612",
        },
        {
            "EffectiveTo": "9999-12-31T23:59:59.9999999",
            "EffectiveFrom": "2019-10-22T00:00:00",
            "EstimationValue": 3450.9,
            "IsLive": True,
            "MPAN": "2700000115580",
            "RegisterId": "N",
            "SerialNumber": "D10W619612",
        },
    ]
    account_id = 100001

    k = s3_con(s3_bucket_name)
    dir_s3 = {
        "s3_key": {
            "EstimatesElecInternal": "stage1/EstimatesElecInternal/",
            "EstimatesGasInternal": "stage1/EstimatesGasInternal/",
        },
    }
    metrics = {}
    internal_estimates.extract_internal_data_response_elec(data, account_id, k, dir_s3, metrics)

    expected_s3_key = "stage1/EstimatesElecInternal/internal_estimates_elec_{}.csv".format(account_id)

    # Verify that the file created in S3 has the correct contents
    k = Key(s3_bucket)
    k.key = expected_s3_key
    csv_lines = k.get_contents_as_string(encoding="utf-8")

    # Construct a CSV reader to parse the file data for us, and verify that the
    # column headers are correct.
    reader = csv.reader(csv_lines.split("\n"), delimiter=",")

    column_headers = next(reader)
    assert column_headers == [
        "EffectiveFrom",
        "EffectiveTo",
        "EstimationValue",
        "IsLive",
        "MPAN",
        "RegisterId",
        "SerialNumber",
        "account_id",
    ]
    # print(list(reader))
    assert list(reader) == [
        [
            "2020-10-21T00:00:00",
            "9999-12-31T23:59:59.9999999",
            "7570.0",
            "True",
            "2700000115580",
            "N",
            "D10W619612",
            "100001",
        ],
        [
            "2020-10-15T00:00:00",
            "9999-12-31T23:59:59.9999999",
            "7733.3",
            "True",
            "2700000115580",
            "N",
            "D10W619612",
            "100001",
        ],
        [
            "2020-09-21T00:00:00",
            "9999-12-31T23:59:59.9999999",
            "8324.0",
            "True",
            "2700000115580",
            "N",
            "D10W619612",
            "100001",
        ],
        [
            "2019-12-19T00:00:00",
            "9999-12-31T23:59:59.9999999",
            "3450.9",
            "True",
            "2700000115580",
            "N",
            "D10W619612",
            "100001",
        ],
        [
            "2019-10-22T00:00:00",
            "9999-12-31T23:59:59.9999999",
            "3450.9",
            "True",
            "2700000115580",
            "N",
            "D10W619612",
            "100001",
        ],
        [],  # FIXME: no idea why there's an empty list at the end.
    ]


@mock_s3_deprecated
def test_extract_internal_data_response_gas():
    s3_bucket_name = "simulated-bucket"

    s3_connection = S3Connection()
    s3_bucket = s3_connection.create_bucket(s3_bucket_name)

    internal_estimates = InternalEstimates()

    # Expected CSV output:
    # EffectiveFrom,EffectiveTo,EstimationValue,IsLive,MPRN,RegisterId,SerialNumber,account_id
    # 2020-11-01T00:00:00,9999-12-31T23:59:59.9999999,7972.0,True,7613419305,G,078247,100001
    # 2020-10-01T00:00:00,9999-12-31T23:59:59.9999999,7966.0,True,7613419305,G,078247,100001
    # 2020-07-01T00:00:00,9999-12-31T23:59:59.9999999,8046.0,True,7613419305,G,078247,100001
    # 2020-03-01T00:00:00,9999-12-31T23:59:59.9999999,10843.0,True,7613419305,G,078247,100001
    # 2019-12-17T15:25:27.957,9999-12-31T23:59:59.9999999,17088.0,True,7613419305,G,078247,100001

    #
    # As this is an internal API, the response is not documented, it seems.
    # This is a best guess at what the source data might look like.
    data = [
        {
            "EffectiveTo": "9999-12-31T23:59:59.9999999",
            "EffectiveFrom": "2020-11-01T00:00:00",
            "EstimationValue": 7972.0,
            "IsLive": True,
            "MPRN": "7613419305",
            "RegisterId": "G",
            "SerialNumber": "078247",
        },
        {
            "EffectiveTo": "9999-12-31T23:59:59.9999999",
            "EffectiveFrom": "2020-10-15T00:00:00",
            "EstimationValue": 7966.0,
            "IsLive": True,
            "MPRN": "7613419305",
            "RegisterId": "G",
            "SerialNumber": "078247",
        },
        {
            "EffectiveTo": "9999-12-31T23:59:59.9999999",
            "EffectiveFrom": "2020-09-21T00:00:00",
            "EstimationValue": 8046.0,
            "IsLive": True,
            "MPRN": "7613419305",
            "RegisterId": "G",
            "SerialNumber": "078247",
        },
        {
            "EffectiveTo": "9999-12-31T23:59:59.9999999",
            "EffectiveFrom": "2019-12-19T00:00:00",
            "EstimationValue": 10843.0,
            "IsLive": True,
            "MPRN": "7613419305",
            "RegisterId": "G",
            "SerialNumber": "078247",
        },
        {
            "EffectiveTo": "9999-12-31T23:59:59.9999999",
            "EffectiveFrom": "2019-10-22T00:00:00",
            "EstimationValue": 17088.0,
            "IsLive": True,
            "MPRN": "7613419305",
            "RegisterId": "G",
            "SerialNumber": "078247",
        },
    ]
    account_id = 100001

    k = s3_con(s3_bucket_name)
    dir_s3 = {
        "s3_key": {
            "EstimatesElecInternal": "stage1/EstimatesElecInternal/",
            "EstimatesGasInternal": "stage1/EstimatesGasInternal/",
        },
    }
    metrics = {}
    internal_estimates.extract_internal_data_response_gas(data, account_id, k, dir_s3, metrics)

    expected_s3_key = "stage1/EstimatesGasInternal/internal_estimates_gas_{}.csv".format(account_id)

    # Verify that the file created in S3 has the correct contents
    k = Key(s3_bucket)
    k.key = expected_s3_key
    csv_lines = k.get_contents_as_string(encoding="utf-8")

    # Construct a CSV reader to parse the file data for us, and verify that the
    # column headers are correct.
    reader = csv.reader(csv_lines.split("\n"), delimiter=",")

    column_headers = next(reader)
    assert column_headers == [
        "EffectiveFrom",
        "EffectiveTo",
        "EstimationValue",
        "IsLive",
        "MPRN",
        "RegisterId",
        "SerialNumber",
        "account_id",
    ]
    # print(list(reader))
    assert list(reader) == [
        ["2020-11-01T00:00:00", "9999-12-31T23:59:59.9999999", "7972.0", "True", "7613419305", "G", "078247", "100001"],
        ["2020-10-15T00:00:00", "9999-12-31T23:59:59.9999999", "7966.0", "True", "7613419305", "G", "078247", "100001"],
        ["2020-09-21T00:00:00", "9999-12-31T23:59:59.9999999", "8046.0", "True", "7613419305", "G", "078247", "100001"],
        [
            "2019-12-19T00:00:00",
            "9999-12-31T23:59:59.9999999",
            "10843.0",
            "True",
            "7613419305",
            "G",
            "078247",
            "100001",
        ],
        [
            "2019-10-22T00:00:00",
            "9999-12-31T23:59:59.9999999",
            "17088.0",
            "True",
            "7613419305",
            "G",
            "078247",
            "100001",
        ],
        [],  # FIXME: no idea why there's an empty list at the end.
    ]


@mock_s3
@unittest.mock.patch("common.utils.get_accountID_fromDB")
# @unittest.mock.patch("multiprocessing.Process")
def test_mp(mock_get_accountID_from_DB):
    with Manager() as manager:
        metrics = {
            "api_error_codes": manager.list(),
        }
