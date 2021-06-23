from datetime import datetime, timedelta
from lxml import etree
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from moto import mock_s3, mock_s3_deprecated
import csv
import json
import os
import responses
import requests
import pytest
import unittest
from multiprocessing import Manager, Value

from process_ensek_tariffs_history import TariffHistory, process_ensek_tariffs_history

from connections.connect_db import get_boto_S3_Connections as s3_con

fixtures = {
    "tariff-history-elec-only": json.load(
        open(os.path.join(os.path.dirname(__file__), "fixtures", "tariff-history-elec-only.json"))
    ),
    "tariff-history-elec-only-formatted-for-stage1": json.load(
        open(
            os.path.join(
                os.path.dirname(__file__),
                "fixtures",
                "tariff-history-elec-only-formatted-for-stage1.json",
            )
        )
    ),
    "tariff-history-dual-fuel": json.load(
        open(os.path.join(os.path.dirname(__file__), "fixtures", "tariff-history-dual-fuel.json"))
    ),
    "tariff-history-dual-fuel-formatted-for-stage1": json.load(
        open(
            os.path.join(
                os.path.dirname(__file__),
                "fixtures",
                "tariff-history-dual-fuel-formatted-for-stage1.json",
            )
        )
    ),
}


@mock_s3_deprecated
def test_extract_tariff_history_json_elec_only():
    # Arrange

    s3_bucket_name = "simulated-bucket"

    s3_connection = S3Connection()
    s3_bucket = s3_connection.create_bucket(s3_bucket_name)

    tariff_history = TariffHistory()

    data = fixtures["tariff-history-elec-only"]

    k = s3_con(s3_bucket_name)
    dir_s3 = {
        "s3_key": {
            "TariffHistory": "stage1/TariffHistory/",
            "TariffHistoryElecStandCharge": "stage1/TariffHistoryElecStandCharge/",
            "TariffHistoryElecUnitRates": "stage1/TariffHistoryElecUnitRates/",
            "TariffHistoryGasStandCharge": "stage1/TariffHistoryGasStandCharge/",
            "TariffHistoryGasUnitRates": "stage1/TariffHistoryGasUnitRates/",
        },
    }

    account_id = 1831

    # Act

    tariff_history.extract_tariff_history_json(data, account_id, k, dir_s3)

    # Assert

    # Verify that the files created in S3 have the correct contents
    k = Key(s3_bucket)
    k.key = "stage1/TariffHistory/df_tariff_history_{}.csv".format(account_id)
    csv_lines = k.get_contents_as_string(encoding="utf-8")

    # Construct a CSV reader to parse the file data for us, and verify that the
    # column headers are correct.
    reader = csv.reader(csv_lines.split("\n"), delimiter=",")

    column_headers = next(reader)

    assert column_headers == [
        "tariffName",
        "startDate",
        "endDate",
        "discounts",
        "tariffType",
        "exitFees",
        "account_id",
    ]
    assert list(reader) == [
        [
            "Igloo Pioneer",
            "2019-05-02T00:00:00",
            "2019-06-09T00:00:00",
            "[]",
            "Variable",
            "",
            "1831",
        ],
        ["Igloo Pioneer", "2019-06-10T00:00:00", "", "[]", "Variable", "", "1831"],
        [],
    ]

    k = Key(s3_bucket)
    k.key = "stage1/TariffHistoryElecStandCharge/th_elec_standingcharge_{}.csv".format(account_id)
    csv_lines = k.get_contents_as_string(encoding="utf-8")

    # Construct a CSV reader to parse the file data for us, and verify that the
    # column headers are correct.
    reader = csv.reader(csv_lines.split("\n"), delimiter=",")

    column_headers = next(reader)

    assert column_headers == [
        "chargeableComponentUID",
        "name",
        "rate",
        "registers",
        "tariffName",
        "startDate",
        "endDate",
        "account_id",
    ]
    assert list(reader) == [
        [
            "ELECTRICITYSTANDINGCHARGE",
            "Standing Charge",
            "19.841",
            "",
            "Igloo Pioneer",
            "2019-05-02T00:00:00",
            "2019-06-09T00:00:00",
            "1831",
        ],
        [
            "ELECTRICITYSTANDINGCHARGE",
            "Standing Charge",
            "19.841",
            "",
            "Igloo Pioneer",
            "2019-06-10T00:00:00",
            "",
            "1831",
        ],
        [],
    ]

    k = Key(s3_bucket)
    k.key = "stage1/TariffHistoryElecUnitRates/th_elec_unitrates_{}.csv".format(account_id)
    csv_lines = k.get_contents_as_string(encoding="utf-8")

    # Construct a CSV reader to parse the file data for us, and verify that the
    # column headers are correct.
    reader = csv.reader(csv_lines.split("\n"), delimiter=",")

    column_headers = next(reader)

    assert column_headers == [
        "chargeableComponentUID",
        "name",
        "rate",
        "registers",
        "tariffName",
        "startDate",
        "endDate",
        "account_id",
    ]
    assert list(reader) == [
        [
            "ANYTIME",
            "Any Time",
            "13.019",
            "[1935, 14649, 91578, 125544]",
            "Igloo Pioneer",
            "2019-05-02T00:00:00",
            "2019-06-09T00:00:00",
            "1831",
        ],
        [
            "ANYTIME",
            "Any Time",
            "12.564",
            "[1935, 14649, 91578, 125544]",
            "Igloo Pioneer",
            "2019-06-10T00:00:00",
            "",
            "1831",
        ],
        [],
    ]


@mock_s3_deprecated
def test_extract_tariff_history_json_dual_fuel():
    # Arrange

    s3_bucket_name = "simulated-bucket"

    s3_connection = S3Connection()
    s3_bucket = s3_connection.create_bucket(s3_bucket_name)

    tariff_history = TariffHistory()

    data = fixtures["tariff-history-dual-fuel"]

    k = s3_con(s3_bucket_name)
    dir_s3 = {
        "s3_key": {
            "TariffHistory": "stage1/TariffHistory/",
            "TariffHistoryElecStandCharge": "stage1/TariffHistoryElecStandCharge/",
            "TariffHistoryElecUnitRates": "stage1/TariffHistoryElecUnitRates/",
            "TariffHistoryGasStandCharge": "stage1/TariffHistoryGasStandCharge/",
            "TariffHistoryGasUnitRates": "stage1/TariffHistoryGasUnitRates/",
        },
    }

    account_id = 3453

    # Act

    tariff_history.extract_tariff_history_json(data, account_id, k, dir_s3)

    # Assert

    # Verify that the files created in S3 have the correct contents
    k = Key(s3_bucket)
    k.key = "stage1/TariffHistory/df_tariff_history_{}.csv".format(account_id)
    csv_lines = k.get_contents_as_string(encoding="utf-8")

    # Construct a CSV reader to parse the file data for us, and verify that the
    # column headers are correct.
    reader = csv.reader(csv_lines.split("\n"), delimiter=",")

    column_headers = next(reader)

    assert column_headers == [
        "tariffName",
        "startDate",
        "endDate",
        "discounts",
        "tariffType",
        "exitFees",
        "account_id",
    ]
    assert list(reader) == [
        [
            "Igloo Pioneer",
            "2017-10-23T00:00:00",
            "2018-04-15T00:00:00",
            "[]",
            "Variable",
            "",
            "3453",
        ],
        [
            "Igloo Pioneer",
            "2018-04-16T00:00:00",
            "2018-08-31T00:00:00",
            "[]",
            "Variable",
            "",
            "3453",
        ],
        [
            "Igloo Pioneer",
            "2018-09-01T00:00:00",
            "2018-10-14T00:00:00",
            "[]",
            "Variable",
            "",
            "3453",
        ],
        [],
    ]

    k = Key(s3_bucket)
    k.key = "stage1/TariffHistoryElecStandCharge/th_elec_standingcharge_{}.csv".format(account_id)
    csv_lines = k.get_contents_as_string(encoding="utf-8")

    # Construct a CSV reader to parse the file data for us, and verify that the
    # column headers are correct.
    reader = csv.reader(csv_lines.split("\n"), delimiter=",")

    column_headers = next(reader)

    assert column_headers == [
        "chargeableComponentUID",
        "name",
        "rate",
        "registers",
        "tariffName",
        "startDate",
        "endDate",
        "account_id",
    ]
    assert list(reader) == [
        [
            "ELECTRICITYSTANDINGCHARGE",
            "Standing Charge",
            "20.0",
            "",
            "Igloo Pioneer",
            "2017-10-23T00:00:00",
            "2018-04-15T00:00:00",
            "3453",
        ],
        [
            "ELECTRICITYSTANDINGCHARGE",
            "Standing Charge",
            "20.0",
            "",
            "Igloo Pioneer",
            "2018-04-16T00:00:00",
            "2018-08-31T00:00:00",
            "3453",
        ],
        [
            "ELECTRICITYSTANDINGCHARGE",
            "Standing Charge",
            "23.33333",
            "",
            "Igloo Pioneer",
            "2018-09-01T00:00:00",
            "2018-10-14T00:00:00",
            "3453",
        ],
        [],
    ]

    k = Key(s3_bucket)
    k.key = "stage1/TariffHistoryElecUnitRates/th_elec_unitrates_{}.csv".format(account_id)
    csv_lines = k.get_contents_as_string(encoding="utf-8")

    # Construct a CSV reader to parse the file data for us, and verify that the
    # column headers are correct.
    reader = csv.reader(csv_lines.split("\n"), delimiter=",")

    column_headers = next(reader)

    assert column_headers == [
        "chargeableComponentUID",
        "name",
        "rate",
        "registers",
        "tariffName",
        "startDate",
        "endDate",
        "account_id",
    ]
    assert list(reader) == [
        [
            "ANYTIME",
            "Any Time",
            "11.344",
            "[4578]",
            "Igloo Pioneer",
            "2017-10-23T00:00:00",
            "2018-04-15T00:00:00",
            "3453",
        ],
        [
            "ANYTIME",
            "Any Time",
            "11.70667",
            "[4578]",
            "Igloo Pioneer",
            "2018-04-16T00:00:00",
            "2018-08-31T00:00:00",
            "3453",
        ],
        [
            "DAY",
            "Day Consumption",
            "11.70667",
            "[]",
            "Igloo Pioneer",
            "2018-04-16T00:00:00",
            "2018-08-31T00:00:00",
            "3453",
        ],
        [
            "NIGHT",
            "Night Consumption",
            "11.70667",
            "[]",
            "Igloo Pioneer",
            "2018-04-16T00:00:00",
            "2018-08-31T00:00:00",
            "3453",
        ],
        [
            "ANYTIME",
            "Any Time",
            "12.7019",
            "[4578]",
            "Igloo Pioneer",
            "2018-09-01T00:00:00",
            "2018-10-14T00:00:00",
            "3453",
        ],
        [],
    ]

    k = Key(s3_bucket)
    k.key = "stage1/TariffHistoryGasStandCharge/th_gas_standingcharge_{}.csv".format(account_id)
    csv_lines = k.get_contents_as_string(encoding="utf-8")

    # Construct a CSV reader to parse the file data for us, and verify that the
    # column headers are correct.
    reader = csv.reader(csv_lines.split("\n"), delimiter=",")

    column_headers = next(reader)

    assert column_headers == [
        "chargeableComponentUID",
        "name",
        "rate",
        "registers",
        "tariffName",
        "startDate",
        "endDate",
        "account_id",
    ]
    assert list(reader) == [
        [
            "GASSTANDINGCHARGE",
            "Standing Charge",
            "20.0",
            "",
            "Igloo Pioneer",
            "2017-10-23T00:00:00",
            "2018-04-15T00:00:00",
            "3453",
        ],
        [
            "GASSTANDINGCHARGE",
            "Standing Charge",
            "20.0",
            "",
            "Igloo Pioneer",
            "2018-04-16T00:00:00",
            "2018-08-31T00:00:00",
            "3453",
        ],
        [
            "GASSTANDINGCHARGE",
            "Standing Charge",
            "23.33333",
            "",
            "Igloo Pioneer",
            "2018-09-01T00:00:00",
            "2018-10-14T00:00:00",
            "3453",
        ],
        [],
    ]

    k = Key(s3_bucket)
    k.key = "stage1/TariffHistoryGasUnitRates/th_gas_unitrates_{}.csv".format(account_id)
    csv_lines = k.get_contents_as_string(encoding="utf-8")

    # Construct a CSV reader to parse the file data for us, and verify that the
    # column headers are correct.
    reader = csv.reader(csv_lines.split("\n"), delimiter=",")

    column_headers = next(reader)

    assert column_headers == [
        "chargeableComponentUID",
        "name",
        "rate",
        "registers",
        "tariffName",
        "startDate",
        "endDate",
        "account_id",
    ]
    assert list(reader) == [
        [
            "GAS",
            "Gas Consumption",
            "2.476",
            "[5027]",
            "Igloo Pioneer",
            "2017-10-23T00:00:00",
            "2018-04-15T00:00:00",
            "3453",
        ],
        [
            "GAS",
            "Gas Consumption",
            "2.60952",
            "[5027]",
            "Igloo Pioneer",
            "2018-04-16T00:00:00",
            "2018-08-31T00:00:00",
            "3453",
        ],
        [
            "GAS",
            "Gas Consumption",
            "2.60952",
            "[5027]",
            "Igloo Pioneer",
            "2018-09-01T00:00:00",
            "2018-10-14T00:00:00",
            "3453",
        ],
        [],
    ]


@responses.activate
@pytest.mark.parametrize(
    "ensek_response, expected",
    [
        (fixtures["tariff-history-elec-only"], fixtures["tariff-history-elec-only"]),
        (fixtures["tariff-history-dual-fuel"], fixtures["tariff-history-dual-fuel"]),
    ],
)
def test_get_api_response_successful(ensek_response, expected):
    # Arrange

    url = "https://api.igloo.ignition.ensek.co.uk/Accounts/123456/TariffsWithHistory"
    headers = {}
    account_id = 123456

    responses.add(responses.GET, url, json=ensek_response)

    # Act

    with Manager() as manager:
        metrics = {
            "api_error_codes": manager.list(),
            "connection_error_counter": Value("i", 0),
            "number_of_retries_total": Value("i", 0),
            "retries_per_account": manager.dict(),
            "api_method_time": manager.list(),
            "no_tariffs_history_data": manager.list(),
            "account_id_counter": Value("i", 0),
            "accounts_with_no_data": manager.list(),
        }

        tariff_history = TariffHistory()
        response = tariff_history.get_api_response(url, headers, account_id, metrics)

    # Assert

    assert response == expected


@responses.activate
def test_get_api_response_bad_request():
    # Arrange

    url = "https://api.igloo.ignition.ensek.co.uk/Accounts/123456/TariffsWithHistory"
    headers = {}
    account_id = 123456

    responses.add(responses.GET, url, status=400)

    # Act

    with Manager() as manager:
        metrics = {
            "api_error_codes": manager.list(),
            "connection_error_counter": Value("i", 0),
            "number_of_retries_total": Value("i", 0),
            "retries_per_account": manager.dict(),
            "api_method_time": manager.list(),
            "no_tariffs_history_data": manager.list(),
            "account_id_counter": Value("i", 0),
            "accounts_with_no_data": manager.list(),
        }

        tariff_history = TariffHistory()
        response = tariff_history.get_api_response(url, headers, account_id, metrics)

    # Assert

    assert response is None


@responses.activate
@pytest.mark.parametrize(
    "ensek_response, expected",
    [
        (fixtures["tariff-history-elec-only"], fixtures["tariff-history-elec-only"]),
    ],
)
def test_get_api_response_one_connection_failure(ensek_response, expected):
    # Arrange

    url = "https://api.igloo.ignition.ensek.co.uk/Accounts/123456/TariffsWithHistory"
    headers = {}
    account_id = 123456

    responses.add(responses.GET, url, body=requests.ConnectionError())
    responses.add(responses.GET, url, json=ensek_response)

    # Act

    with Manager() as manager:
        metrics = {
            "api_error_codes": manager.list(),
            "connection_error_counter": Value("i", 0),
            "number_of_retries_total": Value("i", 0),
            "retries_per_account": manager.dict(),
            "api_method_time": manager.list(),
            "no_tariffs_history_data": manager.list(),
            "account_id_counter": Value("i", 0),
            "accounts_with_no_data": manager.list(),
        }

        tariff_history = TariffHistory()
        response = tariff_history.get_api_response(url, headers, account_id, metrics)

    # Assert

    assert response == expected


@responses.activate
def test_get_api_response_max_connection_failures():
    # Arrange

    url = "https://api.igloo.ignition.ensek.co.uk/Accounts/123456/TariffsWithHistory"
    headers = {}
    account_id = 123456

    # retry_in_secs is set to 2, and connection_timeout is set to 5, so three
    # connection errors will get us over the timeout and cause the code to give up.
    responses.add(responses.GET, url, body=requests.ConnectionError())
    responses.add(responses.GET, url, body=requests.ConnectionError())
    responses.add(responses.GET, url, body=requests.ConnectionError())

    # Act

    with Manager() as manager:
        metrics = {
            "api_error_codes": manager.list(),
            "connection_error_counter": Value("i", 0),
            "number_of_retries_total": Value("i", 0),
            "retries_per_account": manager.dict(),
            "api_method_time": manager.list(),
            "no_tariffs_history_data": manager.list(),
            "account_id_counter": Value("i", 0),
            "accounts_with_no_data": manager.list(),
        }

        tariff_history = TariffHistory()
        response = tariff_history.get_api_response(url, headers, account_id, metrics)

    # Assert

    assert response is None


def test_process_accounts_0_accounts():
    # Arrange

    # Act

    with Manager() as manager:
        metrics = {
            "api_error_codes": manager.list(),
            "connection_error_counter": Value("i", 0),
            "number_of_retries_total": Value("i", 0),
            "retries_per_account": manager.dict(),
            "api_method_time": manager.list(),
            "no_tariffs_history_data": manager.list(),
            "account_id_counter": Value("i", 0),
            "accounts_with_no_data": manager.list(),
        }
        tariff_history = TariffHistory()
        tariff_history.process_accounts([], None, None, metrics)

        # Assert

        assert metrics["account_id_counter"].value == 0


@unittest.mock.patch("process_ensek_tariffs_history.TariffHistory.get_api_response")
@unittest.mock.patch("process_ensek_tariffs_history.TariffHistory.extract_tariff_history_json")
def test_process_accounts_1_account(mock_extract_tariff_history_json, mock_get_api_response):
    # Arrange

    mock_get_api_response.side_effect = [fixtures["tariff-history-elec-only"]]

    # Act

    with Manager() as manager:
        metrics = {
            "api_error_codes": manager.list(),
            "connection_error_counter": Value("i", 0),
            "number_of_retries_total": Value("i", 0),
            "retries_per_account": manager.dict(),
            "api_method_time": manager.list(),
            "no_tariffs_history_data": manager.list(),
            "account_id_counter": Value("i", 0),
            "accounts_with_no_data": manager.list(),
        }
        tariff_history = TariffHistory()
        tariff_history.process_accounts([1831], None, None, metrics)

    # Assert

    mock_extract_tariff_history_json.assert_called_once_with(
        fixtures["tariff-history-elec-only-formatted-for-stage1"], 1831, None, None
    )


@unittest.mock.patch("process_ensek_tariffs_history.TariffHistory.get_api_response")
@unittest.mock.patch("process_ensek_tariffs_history.TariffHistory.extract_tariff_history_json")
def test_process_accounts_10_accounts(mock_extract_tariff_history_json, mock_get_api_response):
    # Arrange

    mock_get_api_response.side_effect = [
        fixtures["tariff-history-elec-only"],
        fixtures["tariff-history-dual-fuel"],
        fixtures["tariff-history-elec-only"],
        fixtures["tariff-history-dual-fuel"],
        fixtures["tariff-history-elec-only"],
        fixtures["tariff-history-dual-fuel"],
        fixtures["tariff-history-elec-only"],
        fixtures["tariff-history-dual-fuel"],
        fixtures["tariff-history-elec-only"],
        fixtures["tariff-history-dual-fuel"],
    ]

    expected_extract_tariff_history_json_calls = [
        unittest.mock.call(fixtures["tariff-history-elec-only-formatted-for-stage1"], 1831, None, None),
        unittest.mock.call(fixtures["tariff-history-dual-fuel-formatted-for-stage1"], 1832, None, None),
        unittest.mock.call(fixtures["tariff-history-elec-only-formatted-for-stage1"], 1833, None, None),
        unittest.mock.call(fixtures["tariff-history-dual-fuel-formatted-for-stage1"], 1834, None, None),
        unittest.mock.call(fixtures["tariff-history-elec-only-formatted-for-stage1"], 1835, None, None),
        unittest.mock.call(fixtures["tariff-history-dual-fuel-formatted-for-stage1"], 1836, None, None),
        unittest.mock.call(fixtures["tariff-history-elec-only-formatted-for-stage1"], 1837, None, None),
        unittest.mock.call(fixtures["tariff-history-dual-fuel-formatted-for-stage1"], 1838, None, None),
        unittest.mock.call(fixtures["tariff-history-elec-only-formatted-for-stage1"], 1839, None, None),
        unittest.mock.call(fixtures["tariff-history-dual-fuel-formatted-for-stage1"], 1840, None, None),
    ]

    # Act

    with Manager() as manager:
        metrics = {
            "api_error_codes": manager.list(),
            "connection_error_counter": Value("i", 0),
            "number_of_retries_total": Value("i", 0),
            "retries_per_account": manager.dict(),
            "api_method_time": manager.list(),
            "no_tariffs_history_data": manager.list(),
            "account_id_counter": Value("i", 0),
            "accounts_with_no_data": manager.list(),
        }

        tariff_history = TariffHistory()
        tariff_history.process_accounts(
            [1831, 1832, 1833, 1834, 1835, 1836, 1837, 1838, 1839, 1840], None, None, metrics
        )

    # Assert

    mock_extract_tariff_history_json.assert_has_calls(expected_extract_tariff_history_json_calls)


# Skipping this test for now as mock_s3_depracted seems to break when combined
# with multiprocessing :(
@mock_s3_deprecated
@unittest.mock.patch("common.utils.get_accountID_fromDB")
@unittest.mock.patch("multiprocessing.Process")
def skip_test_process_ensek_tariffs_history_30_accounts(mock_multiprocessing_process, mock_get_accountID_fromDB):
    # Arrange

    s3_connection = S3Connection()
    s3_connection.create_bucket("igloo-data-warehouse-dev-555393537168")

    mock_get_accountID_fromDB.return_value = list(range(1, 31))

    # Act

    process_ensek_tariffs_history()

    # Assert

    assert mock_get_accountID_fromDB.call_count == 1
    assert mock_multiprocessing_process.call_count == 6

    # multiprocessing.Process is passed two kwargs, 'target' and 'args'.
    # 'args' is itself a tuple of args that will be passed to 'target'.
    # The first argument passed to each invocation of 'target' will be
    # list of account ids for that thread to process - we assert that each
    # of those is as expected.
    #
    #                  first element of 'args' tuple  -----------\
    #             kwarg called 'args' that is a tuple ------\     \
    #                   kwargs passed in----------------\    \     \
    #                   call number ------------------\  \    \     \
    #                                                  \  \    \     \
    assert mock_multiprocessing_process.call_args_list[0][1]["args"][0] == [
        1,
        2,
        3,
        4,
        5,
    ]
    assert mock_multiprocessing_process.call_args_list[1][1]["args"][0] == [
        6,
        7,
        8,
        9,
        10,
    ]
    assert mock_multiprocessing_process.call_args_list[2][1]["args"][0] == [
        11,
        12,
        13,
        14,
        15,
    ]
    assert mock_multiprocessing_process.call_args_list[3][1]["args"][0] == [
        16,
        17,
        18,
        19,
        20,
    ]
    assert mock_multiprocessing_process.call_args_list[4][1]["args"][0] == [
        21,
        22,
        23,
        24,
        25,
    ]
    assert mock_multiprocessing_process.call_args_list[5][1]["args"][0] == [
        26,
        27,
        28,
        29,
        30,
    ]
