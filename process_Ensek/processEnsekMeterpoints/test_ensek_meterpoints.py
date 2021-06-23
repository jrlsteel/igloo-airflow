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
from unittest.mock import Mock, patch
from process_ensek_meterpoints_no_history import MeterPoints
import statistics
from collections import defaultdict
from pandas.io.json import json_normalize

sys.path.append("../../")
from connections.connect_db import get_boto_S3_Connections as s3_con
from common import utils as util


@mock_s3_deprecated
def test_extract_data():
    s3_bucket_name = "simulated-bucket"

    s3_connection = S3Connection()
    s3_bucket = s3_connection.create_bucket(s3_bucket_name)

    api_error_codes = []
    connection_error_counter = 0
    number_of_retries_total = 0
    retries_per_account = defaultdict(int)
    api_method_time = []
    no_meterpoints_data = []
    no_meters_data = []
    no_meterpoints_attributes_data = []
    no_registers_data = []
    no_registers_attributes_data = []
    no_meters_attributes_data = []
    account_id_counter = []
    accounts_with_no_data = []
    each_account_meterpoints = []

    # Calculated Metrics

    if api_method_time != []:
        max_api_process_time = max(api_method_time)
        min_api_process_time = min(api_method_time)
        median_api_process_time = statistics.median(api_method_time)
        average_api_process_time = statistics.mean(api_method_time)
    else:
        max_api_process_time = "No api times processed"
        min_api_process_time = "No api times processed"
        median_api_process_time = "No api times processed"
        average_api_process_time = "No api times processed"

    metrics = {
        "retries_per_account": retries_per_account,
        "api_error_codes": api_error_codes,
        "connection_error_counter": connection_error_counter,
        "number_of_retries_total": number_of_retries_total,
        "api_method_time": api_method_time,
        "no_meterpoints_data": no_meterpoints_data,
        "no_meters_data": no_meters_data,
        "no_meterpoints_attributes_data": no_meterpoints_attributes_data,
        "no_registers_data": no_registers_data,
        "no_registers_attributes_data": no_registers_attributes_data,
        "no_meters_attributes_data": no_meters_attributes_data,
        "account_id_counter": account_id_counter,
        "each_account_meterpoints": each_account_meterpoints,
        "accounts_with_no_data": accounts_with_no_data,
        "max_api_process_time": max_api_process_time,
        "min_api_proces_time": min_api_process_time,
        "median_api_process_time": median_api_process_time,
        "average_api_process_time": average_api_process_time,
    }

    meterpoints = MeterPoints(0)

    # Get the test data
    data_file = open(os.path.join(os.path.dirname(__file__), "fixtures", "json_extract.json"))
    json_string = data_file.read().replace("\n", "")
    data_file.close()
    data_json = json.loads(json_string)
    bucket_name = s3_con(s3_bucket_name)
    dir_s3 = {
        "s3_key": {
            "MeterPoints": "stage1/MeterPoints/",
            "MeterPointsAttributes": "stage1/MeterPointsAttributes/",
            "Meters": "stage1/Meters/",
            "MetersAttributes": "stage1/MetersAttributes/",
            "Registers": "stage1/Registers/",
            "RegistersAttributes": "stage1/RegistersAttributes/",
        },
    }
    account_id = "test_id"

    meterpoints.extract_meter_point_json(data_json, account_id, bucket_name, dir_s3, metrics)

    expected_meterpoints = "stage1/MeterPoints/meter_points_{}.csv".format(account_id)
    expected_meterpointattributes = "stage1/MeterPoints/meter_points_{}.csv".format(account_id)
    expected_meters = "stage1/Meters/meters_{}.csv".format(account_id)
    expected_metersattributers = "stage1/MetersAttributes/metersAttributes_{}.csv".format(account_id)
    expected_registers = "stage1/Registers/registers_{}.csv".format(account_id)
    expected_registersattributes = "stage1/RegistersAttributes/registersAttributes_{}.csv".format(account_id)

    # Verify that the files created in S3 have the correct contents
    k = Key(s3_bucket)
    k.key = expected_meterpoints
    csv_lines_meterpoints = k.get_contents_as_string(encoding="utf-8")
    k.key = expected_meterpointattributes
    csv_lines_meterpointsattributes = k.get_contents_as_string(encoding="utf-8")
    k.key = expected_meters
    csv_lines_meters = k.get_contents_as_string(encoding="utf-8")
    k.key = expected_metersattributers
    csv_lines_metersattributes = k.get_contents_as_string(encoding="utf-8")
    k.key = expected_registers
    csv_lines_registers = k.get_contents_as_string(encoding="utf-8")
    k.key = expected_registersattributes
    csv_lines_registerattributes = k.get_contents_as_string(encoding="utf-8")

    # Construct a CSV reader to parse the file data for us, and verify that the
    # column headers are correct.

    meterpoints_reader = csv.reader(csv_lines_meterpoints.split("\n"), delimiter=",")
    meterpoint_attributes_reader = csv.reader(csv_lines_meterpointsattributes.split("\n"), delimiter=",")
    meters_reader = csv.reader(csv_lines_meters.split("\n"), delimiter=",")
    meters_attributes_reader = csv.reader(csv_lines_metersattributes.split("\n"), delimiter=",")
    registers_reader = csv.reader(csv_lines_registers.split("\n"), delimiter=",")
    registers_attributes_reader = csv.reader(csv_lines_registerattributes.split("\n"), delimiter=",")

    meterpoints_column_headers = [
        "associationStartDate",
        "associationEndDate",
        "supplyStartDate",
        "supplyEndDate",
        "isSmart",
        "isSmartCommunicating",
        "meter_point_id",
        "meterPointNumber",
        "meterPointType",
        "account_id",
    ]
    meterpoint_attributes_column_headers = [
        "associationStartDate",
        "associationEndDate",
        "supplyStartDate",
        "supplyEndDate",
        "isSmart",
        "isSmartCommunicating",
        "meter_point_id",
        "meterPointNumber",
        "meterPointType",
        "account_id",
    ]
    meters_column_headers = [
        "meterSerialNumber",
        "installedDate",
        "removedDate",
        "meterId",
        "meter_point_id",
        "account_id",
    ]
    meters_attributes_column_headers = [
        "metersAttributes_attributeDescription",
        "metersAttributes_attributeName",
        "metersAttributes_attributeValue",
        "meter_id",
        "meter_point_id",
        "account_id",
    ]
    registers_column_headers = [
        "registers_eacAq",
        "registers_registerReference",
        "registers_sourceIdType",
        "registers_tariffComponent",
        "registers_tpr",
        "registers_tprPeriodDescription",
        "meter_id",
        "register_id",
        "meter_point_id",
        "account_id",
    ]
    registers_attributes_column_headers = [
        "registersAttributes_attributeDescription",
        "registersAttributes_attributeName",
        "registersAttributes_attributeValue",
        "meter_id",
        "register_id",
        "meter_point_id",
        "account_id",
    ]

    meterpoints_column_headers_s3 = next(meterpoints_reader)
    meterpoint_attributes_column_headers_s3 = next(meterpoint_attributes_reader)
    meters_column_headers_s3 = next(meters_reader)
    meters_attributes_column_headers_s3 = next(meters_attributes_reader)
    registers_column_headers_s3 = next(registers_reader)
    registers_attributes_column_headers_s3 = next(registers_attributes_reader)

    assert meterpoints_column_headers == meterpoints_column_headers_s3, "failed meterpoints"
    assert (
        meterpoint_attributes_column_headers == meterpoint_attributes_column_headers_s3
    ), "failed meterpoints attributes"
    assert meters_column_headers == meters_column_headers_s3, "failed meters"
    assert meters_attributes_column_headers == meters_attributes_column_headers_s3, "failed meters attributes"
    assert registers_column_headers == registers_column_headers_s3, "failed registers"
    assert registers_attributes_column_headers == registers_attributes_column_headers_s3, "not good"

    assert list(meterpoints_reader) == [
        [
            "2020-12-09T00:00:00",
            "",
            "2020-12-30T00:00:00",
            "",
            "False",
            "False",
            "326218",
            "2000022892662",
            "E",
            "test_id",
        ],
        [
            "2020-12-09T00:00:00",
            "",
            "2020-12-30T00:00:00",
            "",
            "False",
            "False",
            "326219",
            "3930574909",
            "G",
            "test_id",
        ],
        [],
    ], "failed meterspoints data"
    assert list(meterpoint_attributes_reader) == [
        [
            "2020-12-09T00:00:00",
            "",
            "2020-12-30T00:00:00",
            "",
            "False",
            "False",
            "326218",
            "2000022892662",
            "E",
            "test_id",
        ],
        [
            "2020-12-09T00:00:00",
            "",
            "2020-12-30T00:00:00",
            "",
            "False",
            "False",
            "326219",
            "3930574909",
            "G",
            "test_id",
        ],
        [],
    ], "failed meterpoiints attributes data"
    assert list(meters_reader) == [
        ["19L3578888", "2019-09-12T00:00:00", "", "309928", "326218", "test_id"],
        ["E6S15524731961", "2019-09-12T00:00:00", "", "312908", "326219", "test_id"],
        [],
    ], "failed meters data"
    assert list(meters_attributes_reader) == [
        [
            "Manufacturers  Make  Type",
            "Manufacturers_Make_Type",
            "Landis+ Gyr E470",
            "309928",
            "326218",
            "test_id",
        ],
        ["Meter Location", "METER_LOCATION", "E", "309928", "326218", "test_id"],
        ["Meter Type", "MeterType", "S2ADE", "309928", "326218", "test_id"],
        ["Amr  Indicator", "Amr_Indicator", "N", "312908", "326219", "test_id"],
        [
            "Bypass  Fitted  Indicator",
            "Bypass_Fitted_Indicator",
            "N",
            "312908",
            "326219",
            "test_id",
        ],
        [
            "Collar  Fitted  Indicator",
            "Collar_Fitted_Indicator",
            "N",
            "312908",
            "326219",
            "test_id",
        ],
        [
            "Conversion  Factor",
            "Conversion_Factor",
            "1.02264",
            "312908",
            "326219",
            "test_id",
        ],
        ["Gas  Act  Owner", "Gas_Act_Owner", "S", "312908", "326219", "test_id"],
        [
            "Imperial  Indicator",
            "Imperial_Indicator",
            "N",
            "312908",
            "326219",
            "test_id",
        ],
        [
            "Inspection  Date",
            "Inspection_Date",
            "7/29/2016 12:00:00 AM",
            "312908",
            "326219",
            "test_id",
        ],
        ["Manufacture  Code", "Manufacture_Code", "LPG", "312908", "326219", "test_id"],
        ["Meter  Link  Code", "Meter_Link_Code", "F", "312908", "326219", "test_id"],
        ["Meter Location", "METER_LOCATION", "0", "312908", "326219", "test_id"],
        [
            "Meter  Location  Description",
            "Meter_Location_Description",
            "GARAGE",
            "312908",
            "326219",
            "test_id",
        ],
        [
            "Meter  Manufacturer  Code",
            "Meter_Manufacturer_Code",
            "LPG",
            "312908",
            "326219",
            "test_id",
        ],
        [
            "Meter  Mechanism  Code",
            "Meter_Mechanism_Code",
            "S2",
            "312908",
            "326219",
            "test_id",
        ],
        ["Meter  Status", "Meter_Status", "LI", "312908", "326219", "test_id"],
        ["Meter Type", "MeterType", "U", "312908", "326219", "test_id"],
        ["Model  Code", "Model_Code", "E6VG470", "312908", "326219", "test_id"],
        ["Payment  Type", "Payment_Type", "Credit", "312908", "326219", "test_id"],
        [
            "Year  Of  Manufacture",
            "Year_Of_Manufacture",
            "2019",
            "312908",
            "326219",
            "test_id",
        ],
        [],
    ], "failed meters attributes data"
    assert list(registers_reader) == [
        [
            "10497.1",
            "1",
            "D0019",
            "",
            "",
            "HHDAY",
            "309928",
            "331651",
            "326218",
            "test_id",
        ],
        ["13029.0", "G", "", "", "", "GAS", "312908", "334873", "326219", "test_id"],
        [],
    ], "failed registers data"
    assert list(registers_attributes_reader) == [
        [
            "Measurement  Quantity ID",
            "Measurement_Quantity_ID",
            "AI",
            "309928",
            "331651",
            "326218",
            "test_id",
        ],
        ["Multiplier", "Multiplier", "1", "309928", "331651", "326218", "test_id"],
        [
            "No  Of  Digits",
            "No_Of_Digits",
            "5",
            "309928",
            "331651",
            "326218",
            "test_id",
        ],
        ["Type", "Type", "C", "309928", "331651", "326218", "test_id"],
        ["Multiplier", "Multiplier", "1", "312908", "334873", "326219", "test_id"],
        [
            "No  Of  Digits",
            "No_Of_Digits",
            "5",
            "312908",
            "334873",
            "326219",
            "test_id",
        ],
        [
            "Units  Of  Measure",
            "Units_Of_Measure",
            "SCFH",
            "312908",
            "334873",
            "326219",
            "test_id",
        ],
        [],
    ], "failed registers attributes data"


@patch("process_ensek_meterpoints_no_history.requests.get")
def test_api_request(mock_get):
    test_dictionary = {"test_json_object": "test_output"}
    mock_get.return_value.ok = True
    mock_get.return_value.status_code = 200
    mock_get.return_value.content = json.dumps(test_dictionary).encode()

    print("mock - ", mock_get)
    api_error_codes = []
    connection_error_counter = 0
    number_of_retries_total = 0
    retries_per_account = defaultdict(int)
    api_method_time = []
    no_meterpoints_data = []
    no_meters_data = []
    no_meterpoints_attributes_data = []
    no_registers_data = []
    no_registers_attributes_data = []
    no_meters_attributes_data = []
    account_id_counter = []
    accounts_with_no_data = []
    each_account_meterpoints = []

    # Calculated Metrics

    if api_method_time != []:
        max_api_process_time = max(api_method_time)
        min_api_process_time = min(api_method_time)
        median_api_process_time = statistics.median(api_method_time)
        average_api_process_time = statistics.mean(api_method_time)
    else:
        max_api_process_time = "No api times processed"
        min_api_process_time = "No api times processed"
        median_api_process_time = "No api times processed"
        average_api_process_time = "No api times processed"

    metrics = {
        "retries_per_account": retries_per_account,
        "api_error_codes": api_error_codes,
        "connection_error_counter": connection_error_counter,
        "number_of_retries_total": number_of_retries_total,
        "api_method_time": api_method_time,
        "no_meterpoints_data": no_meterpoints_data,
        "no_meters_data": no_meters_data,
        "no_meterpoints_attributes_data": no_meterpoints_attributes_data,
        "no_registers_data": no_registers_data,
        "no_registers_attributes_data": no_registers_attributes_data,
        "no_meters_attributes_data": no_meters_attributes_data,
        "account_id_counter": account_id_counter,
        "each_account_meterpoints": each_account_meterpoints,
        "accounts_with_no_data": accounts_with_no_data,
        "max_api_process_time": max_api_process_time,
        "min_api_proces_time": min_api_process_time,
        "median_api_process_time": median_api_process_time,
        "average_api_process_time": average_api_process_time,
    }
    metrics = [metrics]
    # What is being tested
    meterpoints = MeterPoints(0)
    url = "https://test.com"
    headers = {"test_head": "test_value"}
    account_id = "1831"
    response = meterpoints.get_api_response(url, headers, account_id, metrics)

    (passed_url,), passed_headers = mock_get.call_args

    assert response == test_dictionary, "Incorrect response returned"

    assert url == passed_url, "Incorrect URL passed to request"
    assert {"headers": headers} == passed_headers, "Incorrect Headers passed to request"
