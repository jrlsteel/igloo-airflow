import sys

sys.path.append("..")

import pandas as pd
from freezegun import freeze_time
import common.utils
import smart_open
from .d0379 import (
    dataframe_to_d0379,
    fetch_d0379_data,
    write_to_s3,
    copy_d0379_to_sftp,
    NoD0379FileFound,
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


def test_fetch_d0379_data(mocker):
    mocker.patch("common.utils.execute_query_return_df")
    fetch_d0379_data([123], datetime.date.fromisoformat("2018-04-03"))
    common.utils.execute_query_return_df.assert_called_once_with(
        """
select
    account_id,
    mpxn,
    measurement_class,
    primaryvalue,
    cast(hhdate as timestamp) as hhdate
from vw_etl_d0379_hh_elec_settlement
where
    account_id in (123) and
    cast(hhdate as timestamp) >= '2018-04-03 00:30:00' and
    cast(hhdate as timestamp) <= '2018-04-04 00:00:00'
order by account_id, mpxn, hhdate;
"""
    )


@freeze_time("2020-11-28T12:30:00.123456+01:00")
def test_dataframe_to_d0379_skips_mpxns_with_fewer_than_48_hh_readings():
    d0379_data_columns = [
        "account_id",
        "mpxn",
        "measurement_class",
        "primaryvalue",
        "hhdate",
    ]
    d0379_data = [
        ["1831", "1234567891234", "AI", 0.240, "2020-10-20T00:30:00.00Z"],
    ]

    d0379_date = datetime.date.fromisoformat("2020-10-20")

    df = pd.DataFrame(d0379_data, columns=d0379_data_columns)

    d0379_text = dataframe_to_d0379(d0379_date, df, "0123456789")

    assert (
        d0379_text.split("\n")
        == """ZHV|0123456789|D0379001|X|PION|C|UDMS|20201128113000||||OPER|
ZPT|0123456789|49||1|20201128113000|""".split(
            "\n"
        )
    )


@freeze_time("2020-11-28T12:30:00.123456+01:00")
def test_dataframe_to_d0379_handles_multiple_mpxns():
    d0379_data_columns = [
        "mpxn",
        "measurement_quantity_id",
        "suplier_id",
        "measurement_class",
        "primaryvalue",
        "hhdate",
    ]
    d0379_data = [
        ["1234567891234", "AI", "PION", "A", 240, "2020-10-20T00:30:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 204, "2020-10-20T01:00:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 218, "2020-10-20T01:30:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 201, "2020-10-20T02:00:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 238, "2020-10-20T02:30:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 187, "2020-10-20T03:00:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 206, "2020-10-20T03:30:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 195, "2020-10-20T04:00:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 212, "2020-10-20T04:30:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 166, "2020-10-20T05:00:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 223, "2020-10-20T05:30:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 187, "2020-10-20T06:00:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 213, "2020-10-20T06:30:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 201, "2020-10-20T07:00:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 232, "2020-10-20T07:30:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 223, "2020-10-20T08:00:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 297, "2020-10-20T08:30:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 262, "2020-10-20T09:00:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 341, "2020-10-20T09:30:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 307, "2020-10-20T10:00:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 276, "2020-10-20T10:30:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 295, "2020-10-20T11:00:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 309, "2020-10-20T11:30:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 276, "2020-10-20T12:00:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 400, "2020-10-20T12:30:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 401, "2020-10-20T13:00:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 328, "2020-10-20T13:30:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 396, "2020-10-20T14:00:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 552, "2020-10-20T14:30:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 358, "2020-10-20T15:00:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 369, "2020-10-20T15:30:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 386, "2020-10-20T16:00:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 350, "2020-10-20T16:30:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 325, "2020-10-20T17:00:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 335, "2020-10-20T17:30:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 386, "2020-10-20T18:00:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 348, "2020-10-20T18:30:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 355, "2020-10-20T19:00:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 285, "2020-10-20T19:30:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 206, "2020-10-20T20:00:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 245, "2020-10-20T20:30:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 221, "2020-10-20T21:00:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 204, "2020-10-20T21:30:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 432, "2020-10-20T22:00:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 471, "2020-10-20T22:30:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 485, "2020-10-20T23:00:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 93, "2020-10-20T23:30:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 419, "2020-10-21T00:00:00.00Z"],
        ["2234567891235", "AI", "PION", "A", 240, "2020-10-20T00:30:00.00Z"],
        ["2234567891235", "AI", "PION", "A", 204, "2020-10-20T01:00:00.00Z"],
        ["2234567891235", "AI", "PION", "A", 218, "2020-10-20T01:30:00.00Z"],
        ["2234567891235", "AI", "PION", "A", 201, "2020-10-20T02:00:00.00Z"],
        ["2234567891235", "AI", "PION", "A", 238, "2020-10-20T02:30:00.00Z"],
        ["2234567891235", "AI", "PION", "A", 187, "2020-10-20T03:00:00.00Z"],
        ["2234567891235", "AI", "PION", "A", 206, "2020-10-20T03:30:00.00Z"],
        ["2234567891235", "AI", "PION", "A", 195, "2020-10-20T04:00:00.00Z"],
        ["2234567891235", "AI", "PION", "A", 212, "2020-10-20T04:30:00.00Z"],
        ["2234567891235", "AI", "PION", "A", 166, "2020-10-20T05:00:00.00Z"],
        ["2234567891235", "AI", "PION", "A", 223, "2020-10-20T05:30:00.00Z"],
        ["2234567891235", "AI", "PION", "A", 187, "2020-10-20T06:00:00.00Z"],
        ["2234567891235", "AI", "PION", "A", 213, "2020-10-20T06:30:00.00Z"],
        ["2234567891235", "AI", "PION", "A", 201, "2020-10-20T07:00:00.00Z"],
        ["2234567891235", "AI", "PION", "A", 232, "2020-10-20T07:30:00.00Z"],
        ["2234567891235", "AI", "PION", "A", 223, "2020-10-20T08:00:00.00Z"],
        ["2234567891235", "AI", "PION", "A", 297, "2020-10-20T08:30:00.00Z"],
        ["2234567891235", "AI", "PION", "A", 262, "2020-10-20T09:00:00.00Z"],
        ["2234567891235", "AI", "PION", "A", 341, "2020-10-20T09:30:00.00Z"],
        ["2234567891235", "AI", "PION", "A", 307, "2020-10-20T10:00:00.00Z"],
        ["2234567891235", "AI", "PION", "A", 276, "2020-10-20T10:30:00.00Z"],
        ["2234567891235", "AI", "PION", "A", 295, "2020-10-20T11:00:00.00Z"],
        ["2234567891235", "AI", "PION", "A", 309, "2020-10-20T11:30:00.00Z"],
        ["2234567891235", "AI", "PION", "A", 276, "2020-10-20T12:00:00.00Z"],
        ["2234567891235", "AI", "PION", "A", 400, "2020-10-20T12:30:00.00Z"],
        ["2234567891235", "AI", "PION", "A", 401, "2020-10-20T13:00:00.00Z"],
        ["2234567891235", "AI", "PION", "A", 328, "2020-10-20T13:30:00.00Z"],
        ["2234567891235", "AI", "PION", "A", 396, "2020-10-20T14:00:00.00Z"],
        ["2234567891235", "AI", "PION", "A", 552, "2020-10-20T14:30:00.00Z"],
        ["2234567891235", "AI", "PION", "A", 358, "2020-10-20T15:00:00.00Z"],
        ["2234567891235", "AI", "PION", "A", 369, "2020-10-20T15:30:00.00Z"],
        ["2234567891235", "AI", "PION", "A", 386, "2020-10-20T16:00:00.00Z"],
        ["2234567891235", "AI", "PION", "A", 350, "2020-10-20T16:30:00.00Z"],
        ["2234567891235", "AI", "PION", "A", 325, "2020-10-20T17:00:00.00Z"],
        ["2234567891235", "AI", "PION", "A", 335, "2020-10-20T17:30:00.00Z"],
        ["2234567891235", "AI", "PION", "A", 386, "2020-10-20T18:00:00.00Z"],
        ["2234567891235", "AI", "PION", "A", 348, "2020-10-20T18:30:00.00Z"],
        ["2234567891235", "AI", "PION", "A", 355, "2020-10-20T19:00:00.00Z"],
        ["2234567891235", "AI", "PION", "A", 285, "2020-10-20T19:30:00.00Z"],
        ["2234567891235", "AI", "PION", "A", 206, "2020-10-20T20:00:00.00Z"],
        ["2234567891235", "AI", "PION", "A", 245, "2020-10-20T20:30:00.00Z"],
        ["2234567891235", "AI", "PION", "A", 221, "2020-10-20T21:00:00.00Z"],
        ["2234567891235", "AI", "PION", "A", 204, "2020-10-20T21:30:00.00Z"],
        ["2234567891235", "AI", "PION", "A", 432, "2020-10-20T22:00:00.00Z"],
        ["2234567891235", "AI", "PION", "A", 471, "2020-10-20T22:30:00.00Z"],
        ["2234567891235", "AI", "PION", "A", 485, "2020-10-20T23:00:00.00Z"],
        ["2234567891235", "AI", "PION", "A", 93, "2020-10-20T23:30:00.00Z"],
        ["2234567891235", "AI", "PION", "A", 20419, "2020-10-21T00:00:00.00Z"],
    ]

    d0379_date = datetime.date.fromisoformat("2020-10-20")

    df = pd.DataFrame(d0379_data, columns=d0379_data_columns)

    d0379_text = dataframe_to_d0379(d0379_date, df, "0123456789")

    assert (
        d0379_text.split("\n")
        == """ZHV|0123456789|D0379001|X|PION|C|UDMS|20201128113000||||OPER|
25B|1234567891234|AI|PION|
26B|20201020|
66L|A|0.240|
66L|A|0.204|
66L|A|0.218|
66L|A|0.201|
66L|A|0.238|
66L|A|0.187|
66L|A|0.206|
66L|A|0.195|
66L|A|0.212|
66L|A|0.166|
66L|A|0.223|
66L|A|0.187|
66L|A|0.213|
66L|A|0.201|
66L|A|0.232|
66L|A|0.223|
66L|A|0.297|
66L|A|0.262|
66L|A|0.341|
66L|A|0.307|
66L|A|0.276|
66L|A|0.295|
66L|A|0.309|
66L|A|0.276|
66L|A|0.400|
66L|A|0.401|
66L|A|0.328|
66L|A|0.396|
66L|A|0.552|
66L|A|0.358|
66L|A|0.369|
66L|A|0.386|
66L|A|0.350|
66L|A|0.325|
66L|A|0.335|
66L|A|0.386|
66L|A|0.348|
66L|A|0.355|
66L|A|0.285|
66L|A|0.206|
66L|A|0.245|
66L|A|0.221|
66L|A|0.204|
66L|A|0.432|
66L|A|0.471|
66L|A|0.485|
66L|A|0.093|
66L|A|0.419|
25B|2234567891235|AI|PION|
26B|20201020|
66L|A|0.240|
66L|A|0.204|
66L|A|0.218|
66L|A|0.201|
66L|A|0.238|
66L|A|0.187|
66L|A|0.206|
66L|A|0.195|
66L|A|0.212|
66L|A|0.166|
66L|A|0.223|
66L|A|0.187|
66L|A|0.213|
66L|A|0.201|
66L|A|0.232|
66L|A|0.223|
66L|A|0.297|
66L|A|0.262|
66L|A|0.341|
66L|A|0.307|
66L|A|0.276|
66L|A|0.295|
66L|A|0.309|
66L|A|0.276|
66L|A|0.400|
66L|A|0.401|
66L|A|0.328|
66L|A|0.396|
66L|A|0.552|
66L|A|0.358|
66L|A|0.369|
66L|A|0.386|
66L|A|0.350|
66L|A|0.325|
66L|A|0.335|
66L|A|0.386|
66L|A|0.348|
66L|A|0.355|
66L|A|0.285|
66L|A|0.206|
66L|A|0.245|
66L|A|0.221|
66L|A|0.204|
66L|A|0.432|
66L|A|0.471|
66L|A|0.485|
66L|A|0.093|
66L|A|20.419|
ZPT|0123456789|49||1|20201128113000|""".split(
            "\n"
        )
    )


@freeze_time("2020-11-28T12:30:00.123456+01:00")
def test_dataframe_to_d0379_sorts_hh_periods():
    d0379_data_columns = [
        "mpxn",
        "measurement_quantity_id",
        "suplier_id",
        "measurement_class",
        "primaryvalue",
        "hhdate",
    ]
    d0379_data = [
        ["1234567891234", "AI", "PION", "A", 240, "2020-10-20T00:30:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 204, "2020-10-20T01:00:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 218, "2020-10-20T01:30:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 201, "2020-10-20T02:00:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 238, "2020-10-20T02:30:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 187, "2020-10-20T03:00:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 206, "2020-10-20T03:30:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 195, "2020-10-20T04:00:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 212, "2020-10-20T04:30:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 166, "2020-10-20T05:00:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 223, "2020-10-20T05:30:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 187, "2020-10-20T06:00:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 213, "2020-10-20T06:30:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 201, "2020-10-20T07:00:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 232, "2020-10-20T07:30:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 223, "2020-10-20T08:00:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 297, "2020-10-20T08:30:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 262, "2020-10-20T09:00:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 341, "2020-10-20T09:30:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 307, "2020-10-20T10:00:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 276, "2020-10-20T10:30:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 295, "2020-10-20T11:00:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 309, "2020-10-20T11:30:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 276, "2020-10-20T12:00:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 400, "2020-10-20T12:30:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 401, "2020-10-20T13:00:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 328, "2020-10-20T13:30:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 396, "2020-10-20T14:00:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 552, "2020-10-20T14:30:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 358, "2020-10-20T15:00:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 369, "2020-10-20T15:30:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 386, "2020-10-20T16:00:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 350, "2020-10-20T16:30:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 325, "2020-10-20T17:00:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 335, "2020-10-20T17:30:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 386, "2020-10-20T18:00:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 348, "2020-10-20T18:30:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 355, "2020-10-20T19:00:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 285, "2020-10-20T19:30:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 206, "2020-10-20T20:00:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 245, "2020-10-20T20:30:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 221, "2020-10-20T21:00:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 204, "2020-10-20T21:30:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 432, "2020-10-20T22:00:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 471, "2020-10-20T22:30:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 419, "2020-10-21T00:00:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 485, "2020-10-20T23:00:00.00Z"],
        ["1234567891234", "AI", "PION", "A", 93, "2020-10-20T23:30:00.00Z"],
    ]

    d0379_date = datetime.date.fromisoformat("2020-10-20")

    df = pd.DataFrame(d0379_data, columns=d0379_data_columns)

    d0379_text = dataframe_to_d0379(d0379_date, df, "0123456789")

    assert (
        d0379_text.split("\n")
        == """ZHV|0123456789|D0379001|X|PION|C|UDMS|20201128113000||||OPER|
25B|1234567891234|AI|PION|
26B|20201020|
66L|A|0.240|
66L|A|0.204|
66L|A|0.218|
66L|A|0.201|
66L|A|0.238|
66L|A|0.187|
66L|A|0.206|
66L|A|0.195|
66L|A|0.212|
66L|A|0.166|
66L|A|0.223|
66L|A|0.187|
66L|A|0.213|
66L|A|0.201|
66L|A|0.232|
66L|A|0.223|
66L|A|0.297|
66L|A|0.262|
66L|A|0.341|
66L|A|0.307|
66L|A|0.276|
66L|A|0.295|
66L|A|0.309|
66L|A|0.276|
66L|A|0.400|
66L|A|0.401|
66L|A|0.328|
66L|A|0.396|
66L|A|0.552|
66L|A|0.358|
66L|A|0.369|
66L|A|0.386|
66L|A|0.350|
66L|A|0.325|
66L|A|0.335|
66L|A|0.386|
66L|A|0.348|
66L|A|0.355|
66L|A|0.285|
66L|A|0.206|
66L|A|0.245|
66L|A|0.221|
66L|A|0.204|
66L|A|0.432|
66L|A|0.471|
66L|A|0.485|
66L|A|0.093|
66L|A|0.419|
ZPT|0123456789|49||1|20201128113000|""".split(
            "\n"
        )
    )


@mock_s3_deprecated
def test_write_to_s3():
    """
    Verify that data is written to S3 successfully.
    """
    s3_connection = S3Connection()
    s3_bucket = s3_connection.create_bucket("igloo-data-warehouse-prod-630944350233")

    d0379_content = "test-content"
    write_to_s3(d0379_content, "igloo-data-warehouse-prod-630944350233", "d0379")

    # assertions
    k = Key(s3_bucket)
    k.key = "d0379"
    d0379_s3_content = k.get_contents_as_string(encoding="utf-8")

    assert d0379_s3_content == d0379_content


@mock_s3_deprecated
def test_write_to_s3_error_invalid_bucket():
    """
    Verify that an exception is raised if the destination S3 bucket does not
    exist.
    """
    s3_connection = S3Connection()
    s3_connection.create_bucket("igloo-data-warehouse-prod-630944350233")

    d0379_content = "test-content"
    with pytest.raises(Exception):
        write_to_s3(d0379_content, "invalid-s3-bucket", "d0379")


@mock_s3
def test_copy_d0379_to_sftp(mocker):
    conn = boto3.resource("s3", region_name="eu-west-1")
    conn.create_bucket(
        Bucket="igloo-data-warehouse-dev-555393537168",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
    )

    s3_client = boto3.client("s3")
    d0379_file_path = os.path.join(
        os.path.dirname(__file__), "fixtures", "D0379_20210219_0123456789.txt"
    )
    s3_client.upload_file(
        d0379_file_path,
        "igloo-data-warehouse-dev-555393537168",
        "flows/outbound/D0379-elective-hh-trial/D0379_20210219_0123456789.txt",
    )

    with unittest.mock.patch("smart_open.open", unittest.mock.mock_open()):
        copy_d0379_to_sftp(datetime.date.fromisoformat("2021-02-19"))
        smart_open.open.assert_called_once_with(
            "sftp://elective_hh:password@127.0.0.1/home/elective_hh/HH/D0379/D0379_20210219_0123456789.txt",
            "w",
        )


@mock_s3
def test_copy_d0379_to_sftp_raises_exception_if_no_files_present(mocker):
    conn = boto3.resource("s3", region_name="eu-west-1")
    conn.create_bucket(
        Bucket="igloo-data-warehouse-dev-555393537168",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
    )

    with unittest.mock.patch("smart_open.open", unittest.mock.mock_open()):
        with pytest.raises(NoD0379FileFound):
            copy_d0379_to_sftp(datetime.date.fromisoformat("2021-02-19"))


@mock_s3
def test_copy_d0379_to_sftp_raises_exception_if_open_fails(mocker):
    conn = boto3.resource("s3", region_name="eu-west-1")
    conn.create_bucket(
        Bucket="igloo-data-warehouse-dev-555393537168",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
    )

    s3_client = boto3.client("s3")
    d0379_file_path = os.path.join(
        os.path.dirname(__file__), "fixtures", "D0379_20210219_0123456789.txt"
    )
    s3_client.upload_file(
        d0379_file_path,
        "igloo-data-warehouse-dev-555393537168",
        "flows/outbound/D0379-elective-hh-trial/D0379_20210219_0123456789.txt",
    )

    def mock_open_raises_authentication_failed_exception(file, mode):
        raise paramiko.ssh_exception.AuthenticationException()

    with unittest.mock.patch(
        "smart_open.open", mock_open_raises_authentication_failed_exception
    ):
        with pytest.raises(paramiko.ssh_exception.AuthenticationException):
            copy_d0379_to_sftp(datetime.date.fromisoformat("2021-02-19"))
