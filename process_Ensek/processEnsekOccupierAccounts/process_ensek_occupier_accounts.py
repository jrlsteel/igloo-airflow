import timeit
import requests
import json
from pandas.io.json import json_normalize
from ratelimit import limits, sleep_and_retry
import time
from requests import ConnectionError
import csv
import sys
import os
import datetime


sys.path.append("../..")

from common import utils as util
from conf import config as con
from connections.connect_db import get_boto_S3_Connections as s3_con


class OccupierAccounts:
    max_calls = con.api_config["max_api_calls"]
    rate = con.api_config["allowed_period_in_secs"]

    def __init__(self):
        self.now = datetime.datetime.now()

    @sleep_and_retry
    @limits(calls=max_calls, period=rate)
    def get_api_response(self, api_url, head):
        """
        get the response for the respective url that is passed as part of this function
        """
        session = requests.Session()
        start_time = time.time()
        timeout = con.api_config["connection_timeout"]
        retry_in_secs = con.api_config["retry_in_secs"]
        i = 0
        payload = '[{"name":"$applicationUID","value":"PortfolioManagement"},{"name":"$workspaceUID","value":"OccupierAccounts"}]'
        while True:
            try:
                response = session.post(api_url, headers=head, data=payload)

                if response.status_code == 200:
                    response_json = json.loads(response.content.decode("utf-8"))
                    response_items = response_json["data"]
                    print(response_items)
                    return response_items
                else:
                    print("Problem Grabbing Data: ", response.status_code)
                    # self.log_error('Response Error: Problem grabbing data', response.status_code)
                    break

            except ConnectionError:
                if time.time() > start_time + timeout:
                    print("Unable to Connect after {} seconds of ConnectionErrors".format(timeout))
                    # self.log_error('Unable to Connect after {} seconds of ConnectionErrors'.format(timeout))

                    break
                else:
                    print("Retrying connection in " + str(retry_in_secs) + " seconds" + str(i))
                    # self.log_error('Retrying connection in ' + str(retry_in_secs) + ' seconds' + str(i))

                    time.sleep(retry_in_secs)
            i = i + retry_in_secs

    def extract_internal_data_response(self, data, k, dir_s3):
        """Processing meter points data"""
        df_occupier_accounts = json_normalize(data, "$values")
        df_occupier_accounts = df_occupier_accounts[["AccountID", "COT Date", "Current Balance", "Days Since COT Date"]]
        df_occupier_accounts.rename(
            columns={
                "AccountID": "account_id",
                "COT Date": "co     t_date",
                "Current Balance": "current_balance",
                "Days Since COT Date": "days_since_cot",
            }
        )
        df_occupier_accounts["etl_change"] = self.now

        print(df_occupier_accounts)
        # print(df_internal_readings)
        if df_occupier_accounts.empty:
            print(" - has no occupier accounts")
        else:
            column_list = util.get_common_info("ensek_column_order", "occupier_accounts")
            df_occupier_accounts_string = df_occupier_accounts.to_csv(None, columns=column_list, index=False)

            file_name_occupier_accounts = "occupier_accounts.csv"
            # k.key = 'ensek-meterpoints/ReadingsInternal/' + file_name_internal_readings
            k.key = dir_s3["s3_key"]["OccupierAccounts"] + file_name_occupier_accounts
            k.set_contents_from_string(df_occupier_accounts_string)

    """Format Json to handle null values"""

    def format_json_response(self, data):
        data_str = json.dumps(data, indent=4).replace("null", '""')
        data_json = json.loads(data_str)
        return data_json

    def log_error(self, error_msg, error_code=""):
        logs_dir_path = sys.path[0] + "/logs/"
        if not os.path.exists(logs_dir_path):
            os.makedirs(logs_dir_path)
        with open(
            sys.path[0] + "/logs/" + "occupier_accounts_logs_" + time.strftime("%d%m%Y") + ".csv", mode="a"
        ) as errorlog:
            employee_writer = csv.writer(errorlog, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL)
            employee_writer.writerow([error_msg, error_code])

    def processOccupierAccounts(self, k, dir_s3):
        api_url, head = util.get_ensek_api_info1("occupier_accounts")
        t = con.api_config["total_no_of_calls"]

        api_url1 = api_url
        internal_data_response = self.get_api_response(api_url1, head)
        print(json.dumps(internal_data_response, indent=4))

        formatted_internal_data = self.format_json_response(internal_data_response)
        print("formatted_internal_data:\n{}\n".format(formatted_internal_data))
        self.extract_internal_data_response(formatted_internal_data, k, dir_s3)


if __name__ == "__main__":

    start = timeit.default_timer()

    p = OccupierAccounts()

    dir_s3 = util.get_dir()
    bucket_name = dir_s3["s3_bucket"]
    s3 = s3_con(bucket_name)

    p.processOccupierAccounts(s3, dir_s3)

    print("Process completed in " + str(timeit.default_timer() - start) + " seconds")
