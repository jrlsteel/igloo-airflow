import timeit
import requests
import json
import pandas as pd
from pandas.io.json import json_normalize
from ratelimit import limits, sleep_and_retry
import time
from requests import ConnectionError
import csv
import multiprocessing
from multiprocessing import freeze_support
import datetime
from pathlib import Path

import sys
import os


from cdw.conf import config as con
from cdw.common import utils as util
from cdw.connections.connect_db import get_boto_S3_Connections as s3_con
from cdw.connections import connect_db as db
from cdw.common import api_filters as apif


class SmartReadsBillings:
    max_calls = con.api_config["max_api_calls"]
    rate = con.api_config["allowed_period_in_secs"]

    def __init__(self):
        self.start_date = datetime.datetime.strptime("2018-01-01", "%Y-%m-%d").date()
        self.end_date = datetime.datetime.today().date()
        self.api_url, self.head, self.key = util.get_api_info(api="smart_reads_billing", header_type="json")
        self.num_days_per_api_calls = 7
        self.sql_elec = apif.smart_reads_billing["elec"]  # there is no need for a weekly run here
        self.sql_gas = apif.smart_reads_billing["gas"]  # there is no need for a weekly run here
        self.sql_all = apif.smart_reads_billing["all"]  # there is no need for a weekly run here

    def format_json_response(self, data):
        """
        This function replaces the null values in the json data to empty string.
        :param data: The json response returned from api
        :return: json data
        """
        try:
            data_str = json.dumps(data, indent=4).replace("null", '""')
            data_json = json.loads(data_str)
            return data_json
        except Exception as e:
            print(e)
            return json.dumps('{message: "malformed response error"}')

    def post_api_response(self, api_url, body, head, query_string="", auth=""):
        session = requests.Session()
        status_code = 0
        response_json = json.loads("{}")

        print(body)

        try:
            print("POST {}".format(api_url))
            response = session.post(api_url, data=body, headers=head)
            status_code = response.status_code
            print("status_code: {}".format(status_code))
            if status_code != 201:
                response_json = json.loads(response.content.decode("utf-8"))
                print(response_json)
        except ConnectionError:
            response_json = json.loads('{message: "Connection Error"}')

        return response_json, status_code

    @staticmethod
    def extract_data_response(data, filename, _param):
        data["setup"] = _param
        df = json_normalize(data)
        if df.empty:
            print(" - No Data")
        else:
            csv_filename = Path("results/" + filename + datetime.datetime.today().strftime("%y%m%d") + ".csv")
            if csv_filename.exists():
                df.to_csv(csv_filename, mode="a", index=False, header=False)
            else:
                df.to_csv(csv_filename, mode="w", index=False)

            print("df_string: {0}".format(df))

    def processAccounts(self, _df):
        api_url_smart_reads, head_smart_reads = util.get_smart_read_billing_api_info("smart_reads_billing")

        for index, df in _df.iterrows():
            # Get Smart Reads Billing
            body = json.dumps(
                {
                    "meterReadingDateTime": df["meterreadingdatetime"]
                    .to_pydatetime()
                    .replace(tzinfo=datetime.timezone.utc)
                    .isoformat(),
                    "accountId": df["accountid"],
                    "uuid": df["uuid"],
                    "userId": df["user_id"],
                    "zendeskId": df["zendesk_id"],
                    "meterType": df["metertype"],
                    "meterPointNumber": df["meterpointnumber"],
                    "meter": df["meter"],
                    "register": df["register"],
                    "reading": df["reading"],
                    "source": df["source"],
                    "createdBy": df["createdby"],
                    "dateCreated": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                },
                default=str,
            )

            response_smart_reads = self.post_api_response(api_url_smart_reads, body, head_smart_reads)
            # print(account_elec_response)

            if response_smart_reads:
                formated_response_smart_reads = self.format_json_response(response_smart_reads)
                print(formated_response_smart_reads)
            else:
                print("ac:" + str(df["accountid"]) + " has no data for Elec status")
                msg_ac = "ac:" + str(df["accountid"]) + " has no data for Elec status"

    def smart_reads_billing_details(self, config_sql):
        pr = db.get_redshift_connection()
        smart_reads_get_df = pr.redshift_to_pandas(config_sql)
        db.close_redshift_connection()
        # addresses_list = addresses_df.values.tolist()

        return smart_reads_get_df


if __name__ == "__main__":
    start = timeit.default_timer()
    freeze_support()
    p = SmartReadsBillings()

    smart_reads_billing_sql = p.sql_all
    smart_reads_billing_df = p.smart_reads_billing_details(smart_reads_billing_sql)

    p.processAccounts(smart_reads_billing_df)

    print(len(smart_reads_billing_df))

    print("Process completed in " + str(timeit.default_timer() - start) + " seconds")
