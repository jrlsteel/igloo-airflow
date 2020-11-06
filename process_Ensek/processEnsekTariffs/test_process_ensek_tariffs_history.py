from datetime import datetime, timedelta
from lxml import etree
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from moto import mock_s3_deprecated
import csv
import json
import os

from process_ensek_tariffs_history import TariffHistory

from connections.connect_db import get_boto_S3_Connections as s3_con

fixtures = {
    "tariff-history-elec-only": json.load(open(os.path.join(os.path.dirname(__file__), 'fixtures', 'tariff-history-elec-only.json'))),
    "tariff-history-dual-fuel": json.load(open(os.path.join(os.path.dirname(__file__), 'fixtures', 'tariff-history-dual-fuel.json'))),
}

@mock_s3_deprecated
def test_extract_tariff_history_json_elec_only():
    s3_bucket_name = 'simulated-bucket'

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

    tariff_history.extract_tariff_history_json(
        data, account_id, k, dir_s3)

    # Verify that the files created in S3 have the correct contents
    k = Key(s3_bucket)
    k.key = 'stage1/TariffHistory/df_tariff_history_{}.csv'.format(account_id)
    csv_lines = k.get_contents_as_string(encoding='utf-8')

    # Construct a CSV reader to parse the file data for us, and verify that the
    # column headers are correct.
    reader = csv.reader(csv_lines.split('\n'), delimiter=',')

    column_headers = next(reader)

    assert(column_headers == ['tariffName', 'startDate', 'endDate',
                              'discounts', 'tariffType', 'exitFees', 'account_id'])
    assert(list(reader) == [
        ['Igloo Pioneer', '2019-05-02T00:00:00',
            '2019-06-09T00:00:00', '[]', 'Variable', '', '1831'],
        ['Igloo Pioneer', '2019-06-10T00:00:00', '', '[]', 'Variable', '', '1831'],
        []
    ])

    k = Key(s3_bucket)
    k.key = 'stage1/TariffHistoryElecStandCharge/th_elec_standingcharge_{}.csv'.format(account_id)
    csv_lines = k.get_contents_as_string(encoding='utf-8')

    # Construct a CSV reader to parse the file data for us, and verify that the
    # column headers are correct.
    reader = csv.reader(csv_lines.split('\n'), delimiter=',')

    column_headers = next(reader)

    assert(column_headers == ['chargeableComponentUID', 'name', 'rate',
                              'registers', 'tariffName', 'startDate', 'endDate', 'account_id'])
    assert(list(reader) == [
        ['ELECTRICITYSTANDINGCHARGE', 'Standing Charge', '19.841', '',
            'Igloo Pioneer', '2019-05-02T00:00:00', '2019-06-09T00:00:00', '1831'],
        ['ELECTRICITYSTANDINGCHARGE', 'Standing Charge', '19.841',
            '', 'Igloo Pioneer', '2019-06-10T00:00:00', '', '1831'],
        []
    ])

    k = Key(s3_bucket)
    k.key = 'stage1/TariffHistoryElecUnitRates/th_elec_unitrates_{}.csv'.format(account_id)
    csv_lines = k.get_contents_as_string(encoding='utf-8')

    # Construct a CSV reader to parse the file data for us, and verify that the
    # column headers are correct.
    reader = csv.reader(csv_lines.split('\n'), delimiter=',')

    column_headers = next(reader)

    assert(column_headers == ['chargeableComponentUID', 'name', 'rate',
                              'registers', 'tariffName', 'startDate', 'endDate', 'account_id'])
    assert(list(reader) == [
        ['ANYTIME', 'Any Time', '13.019', '[1935, 14649, 91578, 125544]',
            'Igloo Pioneer', '2019-05-02T00:00:00', '2019-06-09T00:00:00', '1831'],
        ['ANYTIME', 'Any Time', '12.564', '[1935, 14649, 91578, 125544]',
            'Igloo Pioneer', '2019-06-10T00:00:00', '', '1831'],
        []
    ])

@mock_s3_deprecated
def test_extract_tariff_history_json_dual_fuel():
    s3_bucket_name = 'simulated-bucket'

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

    tariff_history.extract_tariff_history_json(
        data, account_id, k, dir_s3)

    # Verify that the files created in S3 have the correct contents
    k = Key(s3_bucket)
    k.key = 'stage1/TariffHistory/df_tariff_history_{}.csv'.format(account_id)
    csv_lines = k.get_contents_as_string(encoding='utf-8')

    # Construct a CSV reader to parse the file data for us, and verify that the
    # column headers are correct.
    reader = csv.reader(csv_lines.split('\n'), delimiter=',')

    column_headers = next(reader)

    assert(column_headers == ['tariffName', 'startDate', 'endDate', 'discounts', 'tariffType', 'exitFees', 'account_id'])
    assert(list(reader) == [
        ['Igloo Pioneer', '2017-10-23T00:00:00', '2018-04-15T00:00:00', '[]', 'Variable', '', '3453'],
        ['Igloo Pioneer', '2018-04-16T00:00:00', '2018-08-31T00:00:00', '[]', 'Variable', '', '3453'],
        ['Igloo Pioneer', '2018-09-01T00:00:00', '2018-10-14T00:00:00', '[]', 'Variable', '', '3453'],
        []
    ])

    k = Key(s3_bucket)
    k.key = 'stage1/TariffHistoryElecStandCharge/th_elec_standingcharge_{}.csv'.format(account_id)
    csv_lines = k.get_contents_as_string(encoding='utf-8')

    # Construct a CSV reader to parse the file data for us, and verify that the
    # column headers are correct.
    reader = csv.reader(csv_lines.split('\n'), delimiter=',')

    column_headers = next(reader)

    assert(column_headers == ['chargeableComponentUID', 'name', 'rate',
                              'registers', 'tariffName', 'startDate', 'endDate', 'account_id'])
    assert(list(reader) == [
        ['ELECTRICITYSTANDINGCHARGE', 'Standing Charge', '20.0', '', 'Igloo Pioneer', '2017-10-23T00:00:00', '2018-04-15T00:00:00', '3453'],
        ['ELECTRICITYSTANDINGCHARGE', 'Standing Charge', '20.0', '', 'Igloo Pioneer', '2018-04-16T00:00:00', '2018-08-31T00:00:00', '3453'],
        ['ELECTRICITYSTANDINGCHARGE', 'Standing Charge', '23.33333', '', 'Igloo Pioneer', '2018-09-01T00:00:00', '2018-10-14T00:00:00', '3453'],
        []
    ])

    k = Key(s3_bucket)
    k.key = 'stage1/TariffHistoryElecUnitRates/th_elec_unitrates_{}.csv'.format(account_id)
    csv_lines = k.get_contents_as_string(encoding='utf-8')

    # Construct a CSV reader to parse the file data for us, and verify that the
    # column headers are correct.
    reader = csv.reader(csv_lines.split('\n'), delimiter=',')

    column_headers = next(reader)

    assert(column_headers == ['chargeableComponentUID', 'name', 'rate',
                              'registers', 'tariffName', 'startDate', 'endDate', 'account_id'])
    assert(list(reader) == [
        ['ANYTIME', 'Any Time', '11.344', '[4578]', 'Igloo Pioneer', '2017-10-23T00:00:00', '2018-04-15T00:00:00', '3453'],
        ['ANYTIME', 'Any Time', '11.70667', '[4578]', 'Igloo Pioneer', '2018-04-16T00:00:00', '2018-08-31T00:00:00', '3453'],
        ['DAY', 'Day Consumption', '11.70667', '[]', 'Igloo Pioneer', '2018-04-16T00:00:00', '2018-08-31T00:00:00', '3453'],
        ['NIGHT', 'Night Consumption', '11.70667', '[]', 'Igloo Pioneer', '2018-04-16T00:00:00', '2018-08-31T00:00:00', '3453'],
        ['ANYTIME', 'Any Time', '12.7019', '[4578]', 'Igloo Pioneer', '2018-09-01T00:00:00', '2018-10-14T00:00:00', '3453'],
        []
    ])

    k = Key(s3_bucket)
    k.key = 'stage1/TariffHistoryGasStandCharge/th_gas_standingcharge_{}.csv'.format(account_id)
    csv_lines = k.get_contents_as_string(encoding='utf-8')

    # Construct a CSV reader to parse the file data for us, and verify that the
    # column headers are correct.
    reader = csv.reader(csv_lines.split('\n'), delimiter=',')

    column_headers = next(reader)

    assert(column_headers == ['chargeableComponentUID', 'name', 'rate',
                              'registers', 'tariffName', 'startDate', 'endDate', 'account_id'])
    assert(list(reader) == [
        ['GASSTANDINGCHARGE', 'Standing Charge', '20.0', '', 'Igloo Pioneer', '2017-10-23T00:00:00', '2018-04-15T00:00:00', '3453'],
        ['GASSTANDINGCHARGE', 'Standing Charge', '20.0', '', 'Igloo Pioneer', '2018-04-16T00:00:00', '2018-08-31T00:00:00', '3453'],
        ['GASSTANDINGCHARGE', 'Standing Charge', '23.33333', '', 'Igloo Pioneer', '2018-09-01T00:00:00', '2018-10-14T00:00:00', '3453'],
        []
    ])

    k = Key(s3_bucket)
    k.key = 'stage1/TariffHistoryGasUnitRates/th_gas_unitrates_{}.csv'.format(account_id)
    csv_lines = k.get_contents_as_string(encoding='utf-8')

    # Construct a CSV reader to parse the file data for us, and verify that the
    # column headers are correct.
    reader = csv.reader(csv_lines.split('\n'), delimiter=',')

    column_headers = next(reader)

    assert(column_headers == ['chargeableComponentUID', 'name', 'rate',
                              'registers', 'tariffName', 'startDate', 'endDate', 'account_id'])
    assert(list(reader) == [
        ['GAS', 'Gas Consumption', '2.476', '[5027]', 'Igloo Pioneer', '2017-10-23T00:00:00', '2018-04-15T00:00:00', '3453'],
        ['GAS', 'Gas Consumption', '2.60952', '[5027]', 'Igloo Pioneer', '2018-04-16T00:00:00', '2018-08-31T00:00:00', '3453'],
        ['GAS', 'Gas Consumption', '2.60952', '[5027]', 'Igloo Pioneer', '2018-09-01T00:00:00', '2018-10-14T00:00:00', '3453'],
        []
    ])
