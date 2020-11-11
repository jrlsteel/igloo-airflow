from datetime import datetime, timedelta
from lxml import etree
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from moto import mock_s3_deprecated
import csv

from process_ensek_transactions import AccountTransactions

from connections.connect_db import get_boto_S3_Connections as s3_con


@mock_s3_deprecated
def test_extract_account_transactions_json():
    s3_bucket_name = 'simulated-bucket'

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
            "creationDetail": {
                "createdDate": "2017-05-09T16:05:56.49",
                "createdBy": None
            },
            "transactionType": "PAYMENT",
            "transactionTypeFriendlyName": "Payment",
            "statementId": None,
            "method": "Direct Debit",
            "sourceDate": "2017-04-24T00:00:00",
            "actions": [],
            "isCancelled": False
        },
        {
            "id": 10658,
            "amount": -77.35,
            "currentBalance": -154.70,
            "creationDetail": {
                "createdDate": "2017-05-25T10:12:36.043",
                "createdBy": None
            },
            "transactionType": "PAYMENT",
            "transactionTypeFriendlyName": "Payment",
            "statementId": None,
            "method": "Direct Debit",
            "sourceDate": "2017-05-24T00:00:00",
            "actions": [],
            "isCancelled": False
        }
    ]

    account_id = 1831

    k = s3_con(s3_bucket_name)
    dir_s3 = {
        "s3_key": {
            "AccountTransactions": "stage1/AccountTransactions/",
        },
    }

    account_transactions.extract_account_transactions_json(
        data, account_id, k, dir_s3)

    expected_s3_key = 'stage1/AccountTransactions/account_transactions_{}.csv'.format(
        account_id)

    # Verify that the file created in S3 has the correct contents
    k = Key(s3_bucket)
    k.key = expected_s3_key
    csv_lines = k.get_contents_as_string(encoding='utf-8')

    # Construct a CSV reader to parse the file data for us, and verify that the
    # column headers are correct.
    reader = csv.reader(csv_lines.split('\n'), delimiter=',')

    column_headers = next(reader)
    
    assert(column_headers == ['actions', 'amount', 'creationDetail_createdBy', 'creationDetail_createdDate', 'currentBalance', 'id', 'isCancelled', 'method', 'sourceDate', 'statementId', 'transactionType', 'transactionTypeFriendlyName', 'account_id'])
    assert(list(reader) == [
        ['[]', '-77.35', '', '2017-05-09T16:05:56.49', '-77.35', '10648', 'False', 'Direct Debit', '2017-04-24T00:00:00', '', 'PAYMENT', 'Payment', '1831'],
        ['[]', '-77.35', '', '2017-05-25T10:12:36.043', '-154.7', '10658', 'False', 'Direct Debit', '2017-05-24T00:00:00', '', 'PAYMENT', 'Payment', '1831'],
        []  # FIXME: no idea why there's an empty list at the end.
    ])
