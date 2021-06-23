import multiprocessing
import requests
import json
from pandas.io.json import json_normalize
import pandas as pd
from ratelimit import limits, sleep_and_retry
import time
from requests import ConnectionError
import csv
from multiprocessing import freeze_support
import timeit

import sys
import os

sys.path.append("../..")

from conf import config as con
from common import utils as util
from connections.connect_db import get_boto_S3_Connections as s3_con


class DirectDebit:

    max_calls = con.api_config["max_api_calls"]
    rate = con.api_config["allowed_period_in_secs"]

    def __init__(self):
        pass

    @sleep_and_retry
    @limits(calls=max_calls, period=rate)
    def get_api_response(self, api_url, head):
        """
        get the response for the respective url that is passed as part of this function
        """
        start_time = time.time()
        timeout = con.api_config["connection_timeout"]
        retry_in_secs = con.api_config["retry_in_secs"]
        i = 0
        while True:
            try:
                response = requests.get(api_url, headers=head)
                if response.status_code == 200:
                    return json.loads(response.content.decode("utf-8"))
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

    def extract_direct_debit_json(self, data, account_id, k, dir_s3_key):
        # global k

        df_direct_debit = json_normalize(data)
        print("df_direct_debit: \n{}\n".format(df_direct_debit.to_string()))
        df_direct_debit["account_id"] = account_id
        # print(df_direct_debit.to_string)
        filename_direct_debit = "direct_debit" + str(account_id) + ".csv"

        column_list = util.get_common_info("ensek_column_order", "direct_debit")
        df_direct_debit_string = df_direct_debit.to_csv(None, columns=column_list, index=False)
        # k.key = 'ensek-meterpoints/DirectDebit/' + filename_direct_debit
        k.key = dir_s3_key["s3_key"]["DirectDebit"] + filename_direct_debit
        k.set_contents_from_string(df_direct_debit_string)

    def extract_direct_debit_health_check_json(self, data, account_id, k, dir_s3_key):
        # global k
        meta_data_subscription = [
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
        ]
        df_direct_debit = json_normalize(data, record_path=["SubscriptionDetails"], meta=meta_data_subscription)
        df_direct_debit["account_id"] = account_id
        filename_direct_debit = "dd_heath_check_" + str(account_id) + ".csv"
        column_list = util.get_common_info("ensek_column_order", "direct_debit_health_check")
        df_direct_debit_string = df_direct_debit.to_csv(None, columns=column_list, index=False)
        # print(df_direct_debit_string)
        k.key = dir_s3_key["s3_key"]["DirectDebitHealthCheck"] + filename_direct_debit
        k.set_contents_from_string(df_direct_debit_string)

    def format_json_response(self, data):
        data_str = json.dumps(data, indent=4).replace("null", '""')
        data_json = json.loads(data_str)
        return data_json

    def log_error(self, error_msg, error_code=""):
        logs_dir_path = sys.path[0] + "/logs/"
        if not os.path.exists(logs_dir_path):
            os.makedirs(logs_dir_path)
        with open(logs_dir_path + "direct_debit_logs_" + time.strftime("%d%m%Y") + ".csv", mode="a") as errorlog:
            employee_writer = csv.writer(errorlog, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL)
            employee_writer.writerow([error_msg, error_code])

    def processAccounts(self, account_ids, k, dir_s3):
        api_url, head = util.get_ensek_api_info1("direct_debits")
        api_url_ddh, head_ddh = util.get_ensek_api_info1("direct_debits_heath_check")

        for account_id in account_ids:
            t = con.api_config["total_no_of_calls"]
            # Get Direct Debit
            print("ac: " + str(account_id))
            msg_ac = "ac:" + str(account_id)
            # self.log_error(msg_ac, '')
            api_url1 = api_url.format(account_id)
            dd_response = self.get_api_response(api_url1, head)
            if dd_response:
                formatted_dd = self.format_json_response(dd_response)
                # print(json.dumps(formatted_dd))
                self.extract_direct_debit_json(formatted_dd, account_id, k, dir_s3)
            else:
                print("ac:" + str(account_id) + " has no data")
                msg_ac = "ac:" + str(account_id) + " has no data"
                # self.log_error(msg_ac, '')

            # Get Direct Debit Health Check
            print("ac: " + str(account_id))
            msg_ac = "ac:" + str(account_id)
            # self.log_error(msg_ac, '')
            api_url_ddh1 = api_url_ddh.format(account_id)
            ddh_response = self.get_api_response(api_url_ddh1, head_ddh)
            if ddh_response:
                formatted_ddh = self.format_json_response(ddh_response)
                # print(json.dumps(formatted_dd))
                self.extract_direct_debit_health_check_json(formatted_ddh, account_id, k, dir_s3)
            else:
                print("ac:" + str(account_id) + " has no data")
                msg_ac = "ac:" + str(account_id) + " has no data"
                # self.log_error(msg_ac, '')


if __name__ == "__main__":
    freeze_support()

    dir_s3 = util.get_dir()
    bucket_name = dir_s3["s3_bucket"]

    s3 = s3_con(bucket_name)

    account_ids = []
    """Enable this to test for 1 account id"""
    if con.test_config["enable_manual"] == "Y":
        account_ids = con.test_config["account_ids"]

    if con.test_config["enable_file"] == "Y":
        account_ids = util.get_Users_from_s3(s3)

    if con.test_config["enable_db"] == "Y":
        account_ids = util.get_accountID_fromDB(False)
    #
    if con.test_config["enable_db_max"] == "Y":
        account_ids = util.get_accountID_fromDB(True)

    # Enable to test without multiprocessing.
    # p = DirectDebit()
    # p.processAccounts(account_ids, s3, dir_s3)

    ###### Multiprocessing Starts #########
    env = util.get_env()
    total_processes = util.get_multiprocess("total_ensek_processes")

    if env == "uat":
        n = total_processes  # number of process to run in parallel
    else:
        n = total_processes

    k = int(len(account_ids) / n)  # get equal no of files for each process

    print(len(account_ids))
    print(k)

    processes = []
    lv = 0
    start = timeit.default_timer()

    for i in range(n + 1):
        p1 = DirectDebit()
        print(i)
        uv = i * k
        if i == n:
            t = multiprocessing.Process(target=p1.processAccounts, args=(account_ids[lv:], s3_con(bucket_name), dir_s3))
        else:
            t = multiprocessing.Process(
                target=p1.processAccounts, args=(account_ids[lv:uv], s3_con(bucket_name), dir_s3)
            )
        lv = uv

        processes.append(t)

    for p in processes:
        p.start()
        time.sleep(2)

    for process in processes:
        process.join()
    ####### Multiprocessing Ends #########

    print("Process completed in " + str(timeit.default_timer() - start) + " seconds")
