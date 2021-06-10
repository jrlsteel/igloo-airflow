"""
This script gets a list of all live accounts from the database
and uses the ENSEK API to check whether the account has a direct
debit against it.

When complete, a CSV report is written to S3
"""

import os
import json
import sys
import io
import time
import random
import traceback
import requests
import pandas as pd
import logging
import boto3
from datetime import datetime

sys.path.append("..")
from connections import connect_db as db

logger = logging.getLogger(__name__)
logger.setLevel("DEBUG")


class PaymentMethodValidator:
    def __init__(self, config, ensek_config, bucket_name, sample_size=None):

        self.sample_size = sample_size
        self.ensek_config = ensek_config

        self.s3_destination_bucket = bucket_name
        self.s3_key_prefix = "payment-method-reconciliation"

        self.aws_access_key_id = config.s3_config["access_key"]
        self.aws_secret_access_key = config.s3_config["secret_key"]

        self.s3 = None

    def connect_s3(self):

        logger.info("connecting to s3")

        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
        )

    def get_accounts(self):
        """
        Gets a list of account numbers from the database.
        This can be the full list of a subset
        """

        accounts_sql = """
            select debt_status.contract_id, debt_status.payment_method
            from ref_account_debt_status as debt_status
            inner join ref_calculated_daily_customer_file rcdcf
                on contract_id = rcdcf.account_id and
                rcdcf.account_status = 'Live'
            where contract_id is not null
            order by contract_id
        """

        logger.info("running sql: %s", accounts_sql)

        connection = db.get_redshift_connection()
        accounts_df = connection.redshift_to_pandas(accounts_sql)
        db.close_redshift_connection()
        account_statuses = accounts_df.to_dict("records")

        if self.sample_size:
            logger.info("running with sample size of %i", self.sample_size)
            account_statuses = random.sample(account_statuses, self.sample_size)

        return account_statuses

    def store_report(self, accounts_list):
        """
        Stores the report that is produced in S3 at the
        end of the job
        """

        report_df = pd.DataFrame(accounts_list)

        csv_string = report_df.to_csv(index=False)

        if not self.s3:
            self.connect_s3()

        filename = "ensek-payment-method-reconciliation-{}.csv".format(time.time())

        self.s3.put_object(
            Bucket=self.s3_destination_bucket,
            Key="{}/{}".format(self.s3_key_prefix, filename),
            Body=csv_string.encode(),
        )

        logger.info("object %s stored in s3", filename)

    def process(self):

        accounts = self.get_accounts()

        for count, account in enumerate(accounts):
            account_id = account["contract_id"]

            if count % 100 == 0:
                logger.info("currently processing account %i", account_id)

            ensek_status = self.get_ensek_status(account_id)

            retry = 0
            while ensek_status["status_code"] == 429 and retry < 10:
                logger.info("received a rate limit response")
                time.sleep(60)
                ensek_status = self.get_ensek_status(account_id)
                retry += 1

            if retry == 10:
                break

            change = ""
            if account["payment_method"] != ensek_status["payment_method"]:
                change = "{}>{}".format(
                    account["payment_method"],
                    ensek_status["payment_method"],
                )

            account["ensek_method"] = ensek_status["payment_method"]
            account["status_code"] = ensek_status["status_code"]
            account["response"] = ensek_status["response"]
            account["change"] = change

        logger.info("finished calling ENSEK API")

        self.store_report(accounts)

        logger.info("finished")

    def get_ensek_status(self, account_id):
        """
        Calls ENSEK API and determines from the response whether
        the payment method is DD or PORB
        """
        ensek_api_config = self.ensek_config["ensek_api"]

        api_url = "{base_url}/Accounts/{account_id}/DirectDebits/HealthCheck".format(
            account_id=account_id, base_url=ensek_api_config["base_url"]
        )

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": "Bearer {}".format(ensek_api_config["api_key"]),
        }

        try:
            response = requests.get(api_url, headers=headers, timeout=10)
        except Exception as e:
            # Log the exception and treat it as a rate limit
            traceback.print_exc()
            return {
                "payment_method": "",
                "status_code": 429,
                "response": {"error": "exception raise during API call"},
            }

        ensek_data = response.json()
        status_code = response.status_code

        try:
            direct_debit_status = ensek_data["DirectDebitStatus"]
        except KeyError:
            return {
                "payment_method": "PORB",
                "status_code": status_code,
                "response": json.dumps(ensek_data),
            }

        active_dd = direct_debit_status == "Authorised"
        subscription_statuses = [
            subscription["directDebitIsActive"] for subscription in ensek_data["SubscriptionDetails"]
        ]
        active_subscription = any(subscription_statuses)

        ensek_payment_method = "DD" if active_dd and active_subscription else "PORB"

        return {
            "payment_method": ensek_payment_method,
            "status_code": status_code,
            "response": "",
        }


if __name__ == "__main__":

    from conf import config
    from common import utils as util

    directory = util.get_dir()
    finance_bucket = directory["s3_finance_bucket"]
    api_key = directory["apis"]["token"]

    ensek_config = {"ensek_api": {"base_url": "https://api.igloo.ignition.ensek.co.uk", "api_key": api_key}}

    validator = PaymentMethodValidator(config=config, ensek_config=ensek_config, bucket_name=finance_bucket)

    validator.process()
