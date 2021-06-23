from datetime import datetime, timedelta
from lxml import etree
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from moto import mock_s3_deprecated
import csv

from process_ensek_direct_debits import DirectDebit

from connections.connect_db import get_boto_S3_Connections as s3_con


@mock_s3_deprecated
def test_extract_direct_debit_json():
    s3_bucket_name = "simulated-bucket"

    s3_connection = S3Connection()
    s3_bucket = s3_connection.create_bucket(s3_bucket_name)

    direct_debit = DirectDebit()

    # Expected CSV output:
    # Amount,BankAccount,BankName,PaymentDate,Reference,SortCode,account_id
    # 98.00,******46,BARCLAYS BANK PLC,17,,,1831

    # Contrary to Ensek's Swagger documentation, the DirectDebit API actually returns
    # a structure like this:

    data = {
        "Amount": 98.000000,
        "BankName": "BARCLAYS BANK PLC",
        "BankAccount": "******55",
        "PaymentDate": 17,
        "Reference": None,
        "SortCode": None,
    }

    account_id = 1831

    k = s3_con(s3_bucket_name)
    dir_s3 = {
        "s3_key": {
            "DirectDebit": "stage1/DirectDebit/",
            "DirectDebitHealthCheck": "stage1/DirectDebitHealthCheck/",
        },
    }

    direct_debit.extract_direct_debit_json(data, account_id, k, dir_s3)

    expected_s3_key = "stage1/DirectDebit/direct_debit{}.csv".format(account_id)

    # Verify that the file created in S3 has the correct contents
    k = Key(s3_bucket)
    k.key = expected_s3_key
    csv_lines = k.get_contents_as_string(encoding="utf-8")

    # Construct a CSV reader to parse the file data for us, and verify that the
    # column headers are correct.
    reader = csv.reader(csv_lines.split("\n"), delimiter=",")

    column_headers = next(reader)
    assert column_headers == ["Amount", "BankAccount", "BankName", "PaymentDate", "Reference", "SortCode", "account_id"]
    assert list(reader) == [
        ["98.0", "******55", "BARCLAYS BANK PLC", "17", "", "", "1831"],
        [],  # FIXME: no idea why there's an empty list at the end.
    ]


@mock_s3_deprecated
def test_extract_direct_debit_health_check_json():
    s3_bucket_name = "simulated-bucket"

    s3_connection = S3Connection()
    s3_bucket = s3_connection.create_bucket(s3_bucket_name)

    direct_debit = DirectDebit()

    # Expected CSV output:
    # directDebitIsActive,isTopup,paymentAmount,paymentDay,DirectDebitType,BankAccountIsActive,DirectDebitStatus,NextAvailablePaymentDate,AccountName,Amount,BankName,BankAccount,PaymentDate,Reference,SortCode,account_id
    # True,False,82.98,17,Fixed,True,Authorised,2019-04-05T00:00:00,,82.98,BARCLAYS BANK PLC,******46,17,,,1831

    data = {
        "DirectDebitType": "Fixed",
        "BankAccountIsActive": True,
        "DirectDebitStatus": "Authorised",
        "SubscriptionDetails": [
            {"paymentDay": 17, "paymentAmount": 98.0, "isTopup": False, "directDebitIsActive": True}
        ],
        "ScheduledOneTimePayments": [
            {
                "paymentDate": "2020-11-17T00:00:00",
                "amount": 98.0,
                "Type": "Scheduled",
                "id": None,
                "isCancellable": False,
            },
            {
                "paymentDate": "2020-12-17T00:00:00",
                "amount": 98.0,
                "Type": "Scheduled",
                "id": None,
                "isCancellable": False,
            },
            {
                "paymentDate": "2021-01-18T00:00:00",
                "amount": 98.0,
                "Type": "Scheduled",
                "id": None,
                "isCancellable": False,
            },
            {
                "paymentDate": "2021-02-17T00:00:00",
                "amount": 98.0,
                "Type": "Scheduled",
                "id": None,
                "isCancellable": False,
            },
            {
                "paymentDate": "2021-03-17T00:00:00",
                "amount": 98.0,
                "Type": "Scheduled",
                "id": None,
                "isCancellable": False,
            },
            {
                "paymentDate": "2021-04-19T00:00:00",
                "amount": 98.0,
                "Type": "Scheduled",
                "id": None,
                "isCancellable": False,
            },
            {
                "paymentDate": "2021-05-17T00:00:00",
                "amount": 98.0,
                "Type": "Scheduled",
                "id": None,
                "isCancellable": False,
            },
            {
                "paymentDate": "2021-06-17T00:00:00",
                "amount": 98.0,
                "Type": "Scheduled",
                "id": None,
                "isCancellable": False,
            },
            {
                "paymentDate": "2021-07-19T00:00:00",
                "amount": 98.0,
                "Type": "Scheduled",
                "id": None,
                "isCancellable": False,
            },
            {
                "paymentDate": "2021-08-17T00:00:00",
                "amount": 98.0,
                "Type": "Scheduled",
                "id": None,
                "isCancellable": False,
            },
        ],
        "NextAvailablePaymentDate": "2020-11-10T00:00:00",
        "AccountName": None,
        "Amount": 98.00,
        "BankName": "BARCLAYS BANK PLC",
        "BankAccount": "******55",
        "PaymentDate": 17,
        "Reference": None,
        "SortCode": None,
    }

    account_id = 1831

    k = s3_con(s3_bucket_name)
    dir_s3 = {
        "s3_key": {
            "DirectDebit": "stage1/DirectDebit/",
            "DirectDebitHealthCheck": "stage1/DirectDebitHealthCheck/",
        },
    }

    direct_debit.extract_direct_debit_health_check_json(data, account_id, k, dir_s3)

    expected_s3_key = "stage1/DirectDebitHealthCheck/dd_heath_check_{}.csv".format(account_id)

    # Verify that the file created in S3 has the correct contents
    k = Key(s3_bucket)
    k.key = expected_s3_key
    csv_lines = k.get_contents_as_string(encoding="utf-8")

    # Construct a CSV reader to parse the file data for us, and verify that the
    # column headers are correct.
    reader = csv.reader(csv_lines.split("\n"), delimiter=",")

    column_headers = next(reader)
    assert column_headers == [
        "directDebitIsActive",
        "isTopup",
        "paymentAmount",
        "paymentDay",
        "DirectDebitType",
        "BankAccountIsActive",
        "DirectDebitStatus",
        "NextAvailablePaymentDate",
        "AccountName",
        "Amount",
        "BankName",
        "BankAccount",
        "PaymentDate",
        "Reference",
        "SortCode",
        "account_id",
    ]
    assert list(reader) == [
        [
            "True",
            "False",
            "98.0",
            "17",
            "Fixed",
            "True",
            "Authorised",
            "2020-11-10T00:00:00",
            "",
            "98.0",
            "BARCLAYS BANK PLC",
            "******55",
            "17",
            "",
            "",
            "1831",
        ],
        [],  # FIXME: no idea why there's an empty list at the end.
    ]
