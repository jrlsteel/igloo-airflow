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
import unittest
import responses
from multiprocessing import Manager, Value

from cdw.process_Ensek.processEnsekStatus.process_ensek_registration_meterpoint_status import (
    RegistrationsMeterpointsStatus,
    main,
)

from cdw.connections.connect_db import get_boto_S3_Connections as s3_con


@mock_s3_deprecated
def test_extract_reg_elec_json():
    s3_bucket_name = "simulated-bucket"

    s3_connection = S3Connection()
    s3_bucket = s3_connection.create_bucket(s3_bucket_name)

    registrations_meterpoints_status = RegistrationsMeterpointsStatus()

    data = "Tracker.Registration.Live"
    series = pandas.Series(
        {"account_id": 1831, "meter_point_id": 2102, "meterpointnumber": 2000007794886, "meterpointtype": "E"}
    )

    k = s3_con(s3_bucket_name)
    dir_s3 = {
        "s3_key": {
            "RegistrationsElecMeterpoint": "stage1/RegistrationsElecMeterpoint/",
            "RegistrationsGasMeterpoint": "stage1/RegistrationsGasMeterpoint/",
        },
    }

    registrations_meterpoints_status.extract_reg_elec_json(data, series, k, dir_s3)

    # Verify that the files created in S3 have the correct contents
    k = Key(s3_bucket)
    k.key = "stage1/RegistrationsElecMeterpoint/reg_elec_mp_status_1831_2000007794886.csv"
    csv_lines = k.get_contents_as_string(encoding="utf-8")

    # Construct a CSV reader to parse the file data for us, and verify that the
    # column headers are correct.
    reader = csv.reader(csv_lines.split("\n"), delimiter=",")

    column_headers = next(reader)

    assert column_headers == ["account_id", "meter_point_id", "meterpointnumber", "status"]
    assert list(reader) == [["1831", "2102", "2000007794886", "Tracker.Registration.Live"], []]


@mock_s3_deprecated
def test_extract_reg_gas_json():
    s3_bucket_name = "simulated-bucket"

    s3_connection = S3Connection()
    s3_bucket = s3_connection.create_bucket(s3_bucket_name)

    registrations_meterpoints_status = RegistrationsMeterpointsStatus()

    data = "Tracker.Registration.Gas.Cancelled.in.Cooling.Off"

    series = pandas.Series(
        {"account_id": 100000, "meter_point_id": 159413, "meterpointnumber": 2459924700, "meterpointtype": "G"}
    )

    k = s3_con(s3_bucket_name)
    dir_s3 = {
        "s3_key": {
            "RegistrationsElecMeterpoint": "stage1/RegistrationsElecMeterpoint/",
            "RegistrationsGasMeterpoint": "stage1/RegistrationsGasMeterpoint/",
        },
    }

    registrations_meterpoints_status.extract_reg_gas_json(data, series, k, dir_s3)

    # Verify that the files created in S3 have the correct contents
    k = Key(s3_bucket)
    k.key = "stage1/RegistrationsGasMeterpoint/reg_gas_mp_status_100000_2459924700.csv"
    csv_lines = k.get_contents_as_string(encoding="utf-8")

    # Construct a CSV reader to parse the file data for us, and verify that the
    # column headers are correct.
    reader = csv.reader(csv_lines.split("\n"), delimiter=",")

    column_headers = next(reader)

    assert column_headers == ["account_id", "meter_point_id", "meterpointnumber", "status"]
    assert list(reader) == [["100000", "159413", "2459924700", "Tracker.Registration.Gas.Cancelled.in.Cooling.Off"], []]


def test_process_accounts_0_accounts():
    # Arrange
    df = pandas.DataFrame.from_records(
        [], columns=["account_id", "meter_point_id", "meterpointnumber", "meterpointtype"]
    )

    # Act
    with Manager() as manager:
        metrics = {
            "api_error_codes": manager.list(),
            "connection_error_counter": Value("i", 0),
            "number_of_retries_total": Value("i", 0),
            "retries_per_account": manager.dict(),
            "api_method_time": manager.list(),
            "meterpointnumber_counter": Value("i", 0),
            "meterpointnumbers_with_no_data": manager.list(),
        }
        registrations_meterpoints_status = RegistrationsMeterpointsStatus()
        registrations_meterpoints_status.process_accounts(df, None, None, metrics)

        assert list(metrics["api_error_codes"]) == []
        assert metrics["connection_error_counter"].value == 0
        assert metrics["number_of_retries_total"].value == 0
        assert dict(metrics["retries_per_account"]) == {}
        assert len(metrics["api_method_time"]) == 0
        assert metrics["meterpointnumber_counter"].value == 0
        assert list(metrics["meterpointnumbers_with_no_data"]) == []


@responses.activate
@unittest.mock.patch(
    "cdw.process_Ensek.processEnsekStatus.process_ensek_registration_meterpoint_status.RegistrationsMeterpointsStatus.extract_reg_elec_json"
)
def test_process_accounts_1_elec_account(mock_extract_reg_elec_json):
    # Arrange
    df = pandas.DataFrame.from_records(
        [(1831, 2102, 2000007794886, "E")],
        columns=["account_id", "meter_point_id", "meterpointnumber", "meterpointtype"],
    )
    ensek_response_body = '"Tracker.Registration.Live"'
    responses.add(
        responses.GET,
        "https://api.igloo.ignition.ensek.co.uk/Accounts/1831/Processes/Registrations/Elec?meterpointnumber=2000007794886",
        body=ensek_response_body,
    )

    # Act
    with Manager() as manager:
        metrics = {
            "api_error_codes": manager.list(),
            "connection_error_counter": Value("i", 0),
            "number_of_retries_total": Value("i", 0),
            "retries_per_account": manager.dict(),
            "api_method_time": manager.list(),
            "meterpointnumber_counter": Value("i", 0),
            "meterpointnumbers_with_no_data": manager.list(),
        }
        registrations_meterpoints_status = RegistrationsMeterpointsStatus()
        registrations_meterpoints_status.process_accounts(df, None, None, metrics)

        # Assert
        assert mock_extract_reg_elec_json.call_args[0][0] == "Tracker.Registration.Live"

        assert list(metrics["api_error_codes"]) == []
        assert metrics["connection_error_counter"].value == 0
        assert metrics["number_of_retries_total"].value == 0
        assert dict(metrics["retries_per_account"]) == {}
        assert len(metrics["api_method_time"]) == 1
        assert metrics["meterpointnumber_counter"].value == 1
        assert list(metrics["meterpointnumbers_with_no_data"]) == []


@responses.activate
@unittest.mock.patch(
    "cdw.process_Ensek.processEnsekStatus.process_ensek_registration_meterpoint_status.RegistrationsMeterpointsStatus.extract_reg_gas_json"
)
def test_process_accounts_1_gas_account(mock_extract_reg_gas_json):
    # Arrange
    df = pandas.DataFrame.from_records(
        [(1831, 2102, 2000007794886, "G")],
        columns=["account_id", "meter_point_id", "meterpointnumber", "meterpointtype"],
    )
    ensek_response_body = '"Tracker.Registration.Live"'
    responses.add(
        responses.GET,
        "https://api.igloo.ignition.ensek.co.uk/Accounts/1831/Processes/Registrations/Gas?meterpointnumber=2000007794886",
        body=ensek_response_body,
    )

    # Act
    with Manager() as manager:
        metrics = {
            "api_error_codes": manager.list(),
            "connection_error_counter": Value("i", 0),
            "number_of_retries_total": Value("i", 0),
            "retries_per_account": manager.dict(),
            "api_method_time": manager.list(),
            "meterpointnumber_counter": Value("i", 0),
            "meterpointnumbers_with_no_data": manager.list(),
        }
        registrations_meterpoints_status = RegistrationsMeterpointsStatus()
        registrations_meterpoints_status.process_accounts(df, None, None, metrics)

        # Assert
        assert mock_extract_reg_gas_json.call_args[0][0] == "Tracker.Registration.Live"

        assert list(metrics["api_error_codes"]) == []
        assert metrics["connection_error_counter"].value == 0
        assert metrics["number_of_retries_total"].value == 0
        assert dict(metrics["retries_per_account"]) == {}
        assert len(metrics["api_method_time"]) == 1
        assert metrics["meterpointnumber_counter"].value == 1
        assert list(metrics["meterpointnumbers_with_no_data"]) == []


@responses.activate
@unittest.mock.patch(
    "cdw.process_Ensek.processEnsekStatus.process_ensek_registration_meterpoint_status.RegistrationsMeterpointsStatus.extract_reg_elec_json"
)
@unittest.mock.patch(
    "cdw.process_Ensek.processEnsekStatus.process_ensek_registration_meterpoint_status.RegistrationsMeterpointsStatus.extract_reg_gas_json"
)
def test_process_accounts_10_accounts(mock_extract_reg_gas_json, mock_extract_reg_elec_json):
    # Arrange
    df = pandas.DataFrame.from_records(
        [
            (1, 11, 111, "G"),
            (2, 22, 222, "E"),
            (3, 33, 333, "G"),
            (4, 44, 444, "E"),
            (5, 55, 555, "G"),
            (6, 66, 666, "E"),
            (7, 77, 777, "G"),
            (8, 88, 888, "E"),
            (9, 99, 999, "G"),
            (10, 1010, 101010, "E"),
        ],
        columns=["account_id", "meter_point_id", "meterpointnumber", "meterpointtype"],
    )
    ensek_response_body = '"Tracker.Registration.Live-{}"'
    responses.add(
        responses.GET,
        "https://api.igloo.ignition.ensek.co.uk/Accounts/1/Processes/Registrations/Gas?meterpointnumber=111",
        body=ensek_response_body.format(1),
    )
    responses.add(
        responses.GET,
        "https://api.igloo.ignition.ensek.co.uk/Accounts/2/Processes/Registrations/Elec?meterpointnumber=222",
        body=ensek_response_body.format(2),
    )
    responses.add(
        responses.GET,
        "https://api.igloo.ignition.ensek.co.uk/Accounts/3/Processes/Registrations/Gas?meterpointnumber=333",
        body=ensek_response_body.format(3),
    )
    responses.add(
        responses.GET,
        "https://api.igloo.ignition.ensek.co.uk/Accounts/4/Processes/Registrations/Elec?meterpointnumber=444",
        body=ensek_response_body.format(4),
    )
    responses.add(
        responses.GET,
        "https://api.igloo.ignition.ensek.co.uk/Accounts/5/Processes/Registrations/Gas?meterpointnumber=555",
        body=ensek_response_body.format(5),
    )
    responses.add(
        responses.GET,
        "https://api.igloo.ignition.ensek.co.uk/Accounts/6/Processes/Registrations/Elec?meterpointnumber=666",
        body=ensek_response_body.format(6),
    )
    responses.add(
        responses.GET,
        "https://api.igloo.ignition.ensek.co.uk/Accounts/7/Processes/Registrations/Gas?meterpointnumber=777",
        body=ensek_response_body.format(7),
    )
    responses.add(
        responses.GET,
        "https://api.igloo.ignition.ensek.co.uk/Accounts/8/Processes/Registrations/Elec?meterpointnumber=888",
        body=ensek_response_body.format(8),
    )
    responses.add(
        responses.GET,
        "https://api.igloo.ignition.ensek.co.uk/Accounts/9/Processes/Registrations/Gas?meterpointnumber=999",
        body=ensek_response_body.format(9),
    )
    responses.add(
        responses.GET,
        "https://api.igloo.ignition.ensek.co.uk/Accounts/10/Processes/Registrations/Elec?meterpointnumber=101010",
        body=ensek_response_body.format(10),
    )

    # Act
    with Manager() as manager:
        metrics = {
            "api_error_codes": manager.list(),
            "connection_error_counter": Value("i", 0),
            "number_of_retries_total": Value("i", 0),
            "retries_per_account": manager.dict(),
            "api_method_time": manager.list(),
            "meterpointnumber_counter": Value("i", 0),
            "meterpointnumbers_with_no_data": manager.list(),
        }
        registrations_meterpoints_status = RegistrationsMeterpointsStatus()
        registrations_meterpoints_status.process_accounts(df, None, None, metrics)

        # Assert
        assert mock_extract_reg_gas_json.call_args_list[0][0][0] == "Tracker.Registration.Live-1"
        assert mock_extract_reg_gas_json.call_args_list[1][0][0] == "Tracker.Registration.Live-3"
        assert mock_extract_reg_gas_json.call_args_list[2][0][0] == "Tracker.Registration.Live-5"
        assert mock_extract_reg_gas_json.call_args_list[3][0][0] == "Tracker.Registration.Live-7"
        assert mock_extract_reg_gas_json.call_args_list[4][0][0] == "Tracker.Registration.Live-9"

        assert mock_extract_reg_elec_json.call_args_list[0][0][0] == "Tracker.Registration.Live-2"
        assert mock_extract_reg_elec_json.call_args_list[1][0][0] == "Tracker.Registration.Live-4"
        assert mock_extract_reg_elec_json.call_args_list[2][0][0] == "Tracker.Registration.Live-6"
        assert mock_extract_reg_elec_json.call_args_list[3][0][0] == "Tracker.Registration.Live-8"
        assert mock_extract_reg_elec_json.call_args_list[4][0][0] == "Tracker.Registration.Live-10"

        assert list(metrics["api_error_codes"]) == []
        assert metrics["connection_error_counter"].value == 0
        assert metrics["number_of_retries_total"].value == 0
        assert dict(metrics["retries_per_account"]) == {}
        assert len(metrics["api_method_time"]) == 10
        assert metrics["meterpointnumber_counter"].value == 10
        assert list(metrics["meterpointnumbers_with_no_data"]) == []


@unittest.mock.patch("cdw.common.utils.execute_query")
@unittest.mock.patch("multiprocessing.Process")
@unittest.mock.patch("cdw.connections.connect_db.get_boto_S3_Connections")
def test_main(mock_get_boto_S3_Connections, mock_multiprocessing_process, mock_execute_query):
    # Arrange
    mock_execute_query.return_value = pandas.DataFrame(
        [
            [8046, 408903, 9309500700, "G"],
            [8046, 13809, 1900034071970, "E"],
            [21112, 36271, 1300009258842, "E"],
            [32486, 55129, 2405611302, "G"],
            [46577, 411919, 1637435702, "G"],
            [63666, 101436, 3972193101, "G"],
            [73125, 357751, 2229158607, "G"],
            [73941, 384896, 8882382705, "G"],
            [79243, 437688, 2000050831720, "E"],
            [86073, 137615, 1610030856299, "E"],
            [106332, 171225, 1610004694967, "E"],
            [106332, 171226, 1554692802, "G"],
            [107047, 416124, 7656943003, "G"],
            [120445, 400785, 4256548707, "G"],
            [124796, 198680, 1012568781947, "E"],
            [132801, 208819, 2000014982034, "E"],
            [132801, 208820, 3965995708, "G"],
            [136458, 215369, 1141606003, "G"],
            [141117, 223681, 1610011357349, "E"],
            [147181, 390187, 2000054945218, "E"],
            [152207, 243409, 522554809, "G"],
            [169975, 271890, 1012438580093, "E"],
            [175838, 269256, 3387138003, "G"],
            [178816, 285903, 1900000072230, "E"],
        ],
        columns=["account_id", "meter_point_id", "meterpointnumber", "meterpointttype"],
    )

    # Act
    main()

    # Assert
    assert mock_execute_query.call_count == 1
    assert mock_multiprocessing_process.call_count == 6

    assert mock_multiprocessing_process.call_args_list[0][1]["args"][0].equals(
        pandas.DataFrame(
            [
                [8046, 408903, 9309500700, "G"],
                [8046, 13809, 1900034071970, "E"],
                [21112, 36271, 1300009258842, "E"],
                [32486, 55129, 2405611302, "G"],
            ],
            columns=["account_id", "meter_point_id", "meterpointnumber", "meterpointttype"],
            index=[0, 1, 2, 3],
        )
    )

    assert mock_multiprocessing_process.call_args_list[1][1]["args"][0].equals(
        pandas.DataFrame(
            [
                [46577, 411919, 1637435702, "G"],
                [63666, 101436, 3972193101, "G"],
                [73125, 357751, 2229158607, "G"],
                [73941, 384896, 8882382705, "G"],
            ],
            columns=["account_id", "meter_point_id", "meterpointnumber", "meterpointttype"],
            index=[4, 5, 6, 7],
        )
    )

    assert mock_multiprocessing_process.call_args_list[2][1]["args"][0].equals(
        pandas.DataFrame(
            [
                [79243, 437688, 2000050831720, "E"],
                [86073, 137615, 1610030856299, "E"],
                [106332, 171225, 1610004694967, "E"],
                [106332, 171226, 1554692802, "G"],
            ],
            columns=["account_id", "meter_point_id", "meterpointnumber", "meterpointttype"],
            index=[8, 9, 10, 11],
        )
    )

    assert mock_multiprocessing_process.call_args_list[3][1]["args"][0].equals(
        pandas.DataFrame(
            [
                [107047, 416124, 7656943003, "G"],
                [120445, 400785, 4256548707, "G"],
                [124796, 198680, 1012568781947, "E"],
                [132801, 208819, 2000014982034, "E"],
            ],
            columns=["account_id", "meter_point_id", "meterpointnumber", "meterpointttype"],
            index=[12, 13, 14, 15],
        )
    )

    assert mock_multiprocessing_process.call_args_list[4][1]["args"][0].equals(
        pandas.DataFrame(
            [
                [132801, 208820, 3965995708, "G"],
                [136458, 215369, 1141606003, "G"],
                [141117, 223681, 1610011357349, "E"],
                [147181, 390187, 2000054945218, "E"],
            ],
            columns=["account_id", "meter_point_id", "meterpointnumber", "meterpointttype"],
            index=[16, 17, 18, 19],
        )
    )

    assert mock_multiprocessing_process.call_args_list[5][1]["args"][0].equals(
        pandas.DataFrame(
            [
                [152207, 243409, 522554809, "G"],
                [169975, 271890, 1012438580093, "E"],
                [175838, 269256, 3387138003, "G"],
                [178816, 285903, 1900000072230, "E"],
            ],
            columns=["account_id", "meter_point_id", "meterpointnumber", "meterpointttype"],
            index=[20, 21, 22, 23],
        )
    )
