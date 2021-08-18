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
import argparse
import sys
import os

sys.path.append("..")

from conf import config as con
from common import utils as util
from connections.connect_db import get_boto_S3_Connections as s3_con
from connections import connect_db as db
from common import api_filters as apif
import common

logger = common.utils.IglooLogger(source="Read Submission to Ensek")


class SmartReadsBillings:
    max_calls = con.api_config["max_api_calls"]
    rate = con.api_config["allowed_period_in_secs"]

    def __init__(self):
        self.start_date = datetime.datetime.strptime("2018-01-01", "%Y-%m-%d").date()
        self.end_date = datetime.datetime.today().date()
        self.api_url, self.head, self.key = util.get_api_info(api="smart_reads_billing", header_type="json")
        self.num_days_per_api_calls = 7
        self.sql_unified = """select * from vw_etl_smart_billing_reads_unified where vw_etl_smart_billing_reads_unified.meterreadingdatetime = '{}' and vw_etl_smart_billing_reads_unified.next_bill_date = '{}'"""

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
            logger.in_prod_env(e)
            return json.dumps('{message: "malformed response error"}')

    def post_api_response(self, api_url, body, head, query_string="", auth=""):
        session = requests.Session()
        status_code = 0
        response_json = json.loads("{}")

        try:
            response = session.post(api_url, data=body, headers=head)
            status_code = response.status_code
            if status_code != 201:
                response_json = json.loads(response.content.decode("utf-8"))
                logger.in_prod_env(f"Status code not 201 instead: {status_code}, response is:\n {str(response_json)}")
        except ConnectionError:
            self.log_error("Unable to Connect")
            response_json = json.loads('{message: "Connection Error"}')

        return response_json, status_code

    @staticmethod
    def extract_data_response(data, filename, _param):
        data["setup"] = _param
        df = json_normalize(data)
        if df.empty:
            logger.in_prod_env("No Data in {}".format(str(df)))
        else:
            csv_filename = Path("results/" + filename + datetime.datetime.today().strftime("%y%m%d") + ".csv")
            if csv_filename.exists():
                df.to_csv(csv_filename, mode="a", index=False, header=False)
            else:
                df.to_csv(csv_filename, mode="w", index=False)

            logger.in_prod_env("df_string: {0}".format(str(df)))

    def process_accounts(self, _df, dry_run):
        api_url_smart_reads, head_smart_reads = util.get_smart_read_billing_api_info("smart_reads_billing")
        for index, df in _df.iterrows():
            account_id = str(df["accountid"])
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
                    "meterPointNumber": df["meterpointid"],
                    "meter": df["meter"],
                    "register": df["register"],
                    "reading": df["reading"],
                    "source": "SMART",
                    "dateCreated": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                },
                default=str,
            )
            if dry_run is True:
                logger.in_prod_env(body)
            else:
                response_smart_reads = self.post_api_response(api_url_smart_reads, body, head_smart_reads)

                if not response_smart_reads:
                    logger.in_prod_env(f"account: {account_id} has no data for Elec status")
            if index % 100 == 0:
                logger.in_prod_env(f"Reads processed: {index+1}")

    def smart_reads_billing_details(self, config_sql):
        pr = db.get_redshift_connection()
        smart_reads_get_df = pr.redshift_to_pandas(config_sql)
        db.close_redshift_connection()

        return smart_reads_get_df


def main(billing_window_start_date, next_bill_date, dcc_adapter_filter=None, dry_run=True):
    """
    Start process for pushing readings to Ensek
    If you run the file instead of this function the date will be set to today's date
    :param str billing_window_start_date: Filter the reads based on this date. Formatted YYYY-MM-DD
    """
    logger.in_prod_env(
        f"""\nBilling_window_start_date: {billing_window_start_date}\n
        Next_bill_date: {next_bill_date}\n
        Dcc_adapter_filter: {dcc_adapter_filter}\n
        Dry_run: {dry_run}\n"""
    )

    (billing_window_start_date, next_bill_date, dcc_adapter_filter, dry_run)
    start = timeit.default_timer()
    freeze_support()
    p = SmartReadsBillings()

    where_clauses = [
        f"vw_etl_smart_billing_reads_unified.meterreadingdatetime = '{billing_window_start_date}'",
        f"vw_etl_smart_billing_reads_unified.next_bill_date = '{next_bill_date}'",
    ]

    if dcc_adapter_filter is not None:
        where_clauses.append(f"dcc_adapter in ('{dcc_adapter_filter}')")

    sql_query = f"""select * from vw_etl_smart_billing_reads_unified where {' and '.join(where_clauses)}"""

    logger.in_prod_env(f"Query to collect reads from redshift:\n{sql_query}")

    smart_reads_billing_df = p.smart_reads_billing_details(sql_query)
    logger.in_prod_env(f"Number of reads to process: {len(smart_reads_billing_df)}")

    p.process_accounts(smart_reads_billing_df, dry_run)

    logger.in_prod_env(f"Process completed in {str(timeit.default_timer() - start)} seconds")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    # defaults to todays date if no argument is passed in for billing_window_start_date
    parser.add_argument(
        "--billing-window-start-date",
        type=datetime.date.fromisoformat,
        default=str(datetime.date.today()),
    )
    parser.add_argument("--dcc-adapter-filter", default=None)
    parser.add_argument("--dry-run", default=False, action="store_true")
    args = parser.parse_args()
    logger.in_prod_env(f"Running with billing_window_start_date {args.billing_window_start_date}")
    main(
        args.billing_window_start_date,
        args.billing_window_start_date + datetime.timedelta(5),
        dcc_adapter_filter=args.dcc_adapter_filter,
        dry_run=args.dry_run,
    )
