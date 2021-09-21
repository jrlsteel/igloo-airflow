import io
import sys
import json
import time
import unittest
from unittest import mock
from unittest.mock import patch
import boto3
from moto import mock_s3
from freezegun import freeze_time
from cdw.process_payment_method_reconciliation import config_test_vpm
from cdw.process_payment_method_reconciliation.validate_payment_methods import PaymentMethodValidator

bucket_name = "test-bucket"

test_ensek_config = {
    "ensek_api": {
        "base_url": "https://api.example.ignition.ensek.co.uk",
        "api_key": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx==",
    }
}

DD_ACCOUNT = 1234
PORB_ACCOUNT = 5678

porb_output = {
    "DirectDebitType": "Fixed",
    "BankAccountIsActive": True,
    "DirectDebitStatus": "NotAuthorised",
    "SubscriptionDetails": [
        {
            "paymentDay": 19,
            "paymentAmount": 103.11,
            "isTopup": False,
            "directDebitIsActive": False,
        }
    ],
    "ScheduledOneTimePayments": [],
    "NextAvailablePaymentDate": None,
    "AccountName": None,
    "Amount": 103.11,
    "BankName": "METRO BANK PLC",
    "BankAccount": "******37",
    "PaymentDate": 19,
    "Reference": None,
    "SortCode": None,
}

dd_output = {
    "DirectDebitType": "Fixed",
    "BankAccountIsActive": True,
    "DirectDebitStatus": "Authorised",
    "SubscriptionDetails": [
        {
            "paymentDay": 19,
            "paymentAmount": 155.68,
            "isTopup": False,
            "directDebitIsActive": True,
        }
    ],
    "ScheduledOneTimePayments": [],
    "NextAvailablePaymentDate": "2021-05-11T00:00:00",
    "AccountName": None,
    "Amount": 155.68,
    "BankName": "HSBC UK BANK PLC",
    "BankAccount": "******96",
    "PaymentDate": 19,
    "Reference": None,
    "SortCode": None,
}


def mock_get_accounts(*args, **kwargs):
    return [
        {"contract_id": DD_ACCOUNT, "payment_method": "DD"},
        {"contract_id": PORB_ACCOUNT, "payment_method": "PORB"},
    ]


def mocked_requests_get(*args, **kwargs):
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code
            self.content = json.dumps(self.json_data).encode("utf-8")
            self.ok = self.status_code >= 200 and self.status_code <= 202

        def json(self):
            return self.json_data

    if f"/Accounts/{DD_ACCOUNT}/DirectDebits/HealthCheck" in args[0]:

        status_code = 200
        body = dd_output

        return MockResponse(body, status_code)

    if f"/Accounts/{PORB_ACCOUNT}/DirectDebits/HealthCheck" in args[0]:

        status_code = 200
        body = porb_output

        return MockResponse(body, status_code)

    return MockResponse(None, 404)


@mock_s3
@freeze_time("2021-03-04T12:18:00.123456+01:00")
class TestValidatePaymentMethods(unittest.TestCase):
    def setUp(self):

        self.s3 = boto3.resource(
            "s3",
            aws_access_key_id="xxxx",
            aws_secret_access_key="xxxx",
        )

        self.instance = PaymentMethodValidator(
            config=config_test_vpm, ensek_config=test_ensek_config, bucket_name=bucket_name
        )

        self.bucket = self.s3.Bucket(bucket_name)
        self.bucket.create(
            CreateBucketConfiguration={
                "LocationConstraint": "eu-west-1",
            }
        )

    def tearDown(self):

        self.bucket.objects.all().delete()
        self.bucket.delete()

    @patch.object(PaymentMethodValidator, "get_accounts", mock_get_accounts)
    def test_get_accounts(self):

        accounts = self.instance.get_accounts()

        print(accounts)

    @mock.patch("requests.get", side_effect=mocked_requests_get)
    def test_get_ensek_status_for_dd_account(self, mock_requests):

        status = self.instance.get_ensek_status(DD_ACCOUNT)

        print("status is", status)

        self.assertEqual(status, {"payment_method": "DD", "status_code": 200, "response": ""})

    @mock.patch("requests.get", side_effect=mocked_requests_get)
    def test_get_ensek_status_for_porb_account(self, mock_requests):

        status = self.instance.get_ensek_status(PORB_ACCOUNT)

        print("status is", status)

        self.assertEqual(status, {"payment_method": "PORB", "status_code": 200, "response": ""})

    @patch.object(PaymentMethodValidator, "get_accounts", mock_get_accounts)
    @mock.patch("requests.get", side_effect=mocked_requests_get)
    def test_process(self, mock_requests):

        self.instance.process()

        filename = "ensek-payment-method-reconciliation-{}".format(time.time())

        report_object = self.s3.Object(
            bucket_name,
            "payment-method-reconciliation/ensek-payment-method-reconciliation-1614856680.123456.csv",
        )
        report = report_object.get()
        report_contents = report["Body"].read().decode("utf-8")
        report_lines = report_contents.split("\n")

        self.assertEqual(
            report_lines[0],
            "contract_id,payment_method,ensek_method,status_code,response,change",
        )
        self.assertEqual(report_lines[1], "1234,DD,DD,200,,")
        self.assertEqual(report_lines[2], "5678,PORB,PORB,200,,")


if __name__ == "__main__":
    unittest.main()
