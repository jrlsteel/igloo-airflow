from datetime import datetime, timedelta
from lxml import etree
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from moto import mock_s3_deprecated
import csv
import responses
import unittest
import os
import json

from cdw.process_Ensek.processEnsekTransactions.process_ensek_transactions import AccountTransactions

from cdw.connections.connect_db import get_boto_S3_Connections as s3_con

from multiprocessing import Manager, Value

fixtures = {
    "ensek-response-1831": json.load(
        open(os.path.join(os.path.dirname(__file__), "fixtures", "ensek-response-1831.json"))
    ),
    "ensek-response-210251": json.load(
        open(os.path.join(os.path.dirname(__file__), "fixtures", "ensek-response-210251.json"))
    ),
    "ensek-response-1831-formatted": json.load(
        open(os.path.join(os.path.dirname(__file__), "fixtures", "ensek-response-1831-formatted.json"))
    ),
    "ensek-response-210251-formatted": json.load(
        open(os.path.join(os.path.dirname(__file__), "fixtures", "ensek-response-210251-formatted.json"))
    ),
}


@mock_s3_deprecated
def test_extract_account_transactions_json():
    s3_bucket_name = "simulated-bucket"

    s3_connection = S3Connection()
    s3_bucket = s3_connection.create_bucket(s3_bucket_name)

    account_transactions = AccountTransactions(0)

    # Expected CSV output:
    # Amount,BankAccount,BankName,PaymentDate,Reference,SortCode,account_id
    # 98.00,******46,BARCLAYS BANK PLC,17,,,1831

    # Contrary to Ensek's Swagger documentation, the DirectDebit API actually returns
    # a structure like this:

    data = [
        {
            "id": 10648,
            "amount": -77.35,
            "currentBalance": -77.35,
            "creationDetail": {"createdDate": "2017-05-09T16:05:56.49", "createdBy": None},
            "transactionType": "PAYMENT",
            "transactionTypeFriendlyName": "Payment",
            "statementId": None,
            "method": "Direct Debit",
            "sourceDate": "2017-04-24T00:00:00",
            "actions": [],
            "isCancelled": False,
        },
        {
            "id": 10658,
            "amount": -77.35,
            "currentBalance": -154.70,
            "creationDetail": {"createdDate": "2017-05-25T10:12:36.043", "createdBy": None},
            "transactionType": "PAYMENT",
            "transactionTypeFriendlyName": "Payment",
            "statementId": None,
            "method": "Direct Debit",
            "sourceDate": "2017-05-24T00:00:00",
            "actions": [],
            "isCancelled": False,
        },
    ]

    account_id = 1831

    k = s3_con(s3_bucket_name)
    dir_s3 = {
        "s3_key": {
            "AccountTransactions": "stage1/AccountTransactions/",
        },
    }
    metrics = {}
    account_transactions.extract_account_transactions_json(data, account_id, k, dir_s3, [metrics])

    expected_s3_key = "stage1/AccountTransactions/account_transactions_{}.csv".format(account_id)

    # Verify that the file created in S3 has the correct contents
    k = Key(s3_bucket)
    k.key = expected_s3_key
    csv_lines = k.get_contents_as_string(encoding="utf-8")

    # Construct a CSV reader to parse the file data for us, and verify that the
    # column headers are correct.
    reader = csv.reader(csv_lines.split("\n"), delimiter=",")

    column_headers = next(reader)

    assert column_headers == [
        "actions",
        "amount",
        "creationDetail_createdBy",
        "creationDetail_createdDate",
        "currentBalance",
        "id",
        "isCancelled",
        "method",
        "sourceDate",
        "statementId",
        "transactionType",
        "transactionTypeFriendlyName",
        "account_id",
    ]
    assert list(reader) == [
        [
            "[]",
            "-77.35",
            "",
            "2017-05-09T16:05:56.49",
            "-77.35",
            "10648",
            "False",
            "Direct Debit",
            "2017-04-24T00:00:00",
            "",
            "PAYMENT",
            "Payment",
            "1831",
        ],
        [
            "[]",
            "-77.35",
            "",
            "2017-05-25T10:12:36.043",
            "-154.7",
            "10658",
            "False",
            "Direct Debit",
            "2017-05-24T00:00:00",
            "",
            "PAYMENT",
            "Payment",
            "1831",
        ],
        [],  # FIXME: no idea why there's an empty list at the end.
    ]


@responses.activate
@unittest.mock.patch(
    "cdw.process_Ensek.processEnsekTransactions.process_ensek_transactions.AccountTransactions.get_api_response"
)
@unittest.mock.patch(
    "cdw.process_Ensek.processEnsekTransactions.process_ensek_transactions.AccountTransactions.extract_account_transactions_json"
)
def test_process_accounts_2_accounts(mock_extract_account_transactions_json, mock_get_api_response):
    # Arrange
    mock_get_api_response.side_effect = [
        # Alternate the two sample responses we have - it doesn't really
        # matter what's in them.
        fixtures["ensek-response-1831"],
        fixtures["ensek-response-210251"],
    ]

    # Act
    with Manager() as manager:
        metrics = {
            "api_error_codes": manager.list(),
            "connection_error_counter": Value("i", 0),
            "number_of_retries_total": Value("i", 0),
            "retries_per_account": manager.dict(),
            "api_method_time": manager.list(),
            "no_account_transactions_data": manager.list(),
            "account_id_counter": Value("i", 0),
            "accounts_with_no_data": manager.list(),
        }
        account_transactions = AccountTransactions(0)
        account_transactions.processAccounts([1831, 210251], None, None, [metrics])

    # Assert
    assert mock_get_api_response.call_count == 2
    assert (
        mock_get_api_response.call_args_list[0][0][0]
        == "https://api.igloo.ignition.ensek.co.uk/Accounts/1831/Transactions"
    )
    assert (
        mock_get_api_response.call_args_list[1][0][0]
        == "https://api.igloo.ignition.ensek.co.uk/Accounts/210251/Transactions"
    )

    assert mock_extract_account_transactions_json.call_count == 2
    assert mock_extract_account_transactions_json.call_args_list[0][0][0] == fixtures["ensek-response-1831-formatted"]
    assert mock_extract_account_transactions_json.call_args_list[0][0][1] == 1831
    assert mock_extract_account_transactions_json.call_args_list[1][0][0] == fixtures["ensek-response-210251-formatted"]
    assert mock_extract_account_transactions_json.call_args_list[1][0][1] == 210251
