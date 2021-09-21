import timeit

import requests
import json
import pandas
from ratelimit import limits, sleep_and_retry
import time
import datetime
from requests import ConnectionError
import multiprocessing
from multiprocessing import Manager, Value, freeze_support
import statistics

import sys
import os


import cdw.common
import cdw.common.utils
import cdw.conf
from cdw.connections.connect_db import get_boto_S3_Connections as s3_con

iglog = cdw.common.utils.IglooLogger("Process_Ensek_Accounts")


class Accounts:
    max_calls = cdw.conf.config.api_config["max_api_calls"]
    rate = cdw.conf.config.api_config["allowed_period_in_secs"]

    def __init__(self):
        pass

    @sleep_and_retry
    @limits(calls=max_calls, period=rate)
    def get_api_response(self, api_url, head, account_id, metrics):
        """
        get the response for the respective url that is passed as part of this function
        """
        start_time = time.time()
        timeout = cdw.conf.config.api_config["connection_timeout"]
        retry_in_secs = cdw.conf.config.api_config["retry_in_secs"]
        i = 0
        attempt_num = 0
        total_api_time = 0.0
        api_call_start = 0.0

        while True:
            try:
                attempt_num += 1
                api_call_start = time.time()
                response = requests.get(api_url, headers=head)
                total_api_time += time.time() - api_call_start

                if response.status_code == 200:
                    method_time = time.time() - start_time
                    metrics[0]["api_method_time"].append(method_time)
                    return json.loads(response.content.decode("utf-8"))
                else:
                    metrics[0]["api_error_codes"].append(response.status_code)
                    break

            except ConnectionError:
                total_api_time += time.time() - api_call_start
                if time.time() > start_time + timeout:
                    try:
                        with metrics[0]["connection_error_counter"].get_lock():
                            metrics[0]["connection_error_counter"].value += 1
                            if metrics[0]["connection_error_counter"].value % 100 == 0:
                                iglog.in_prod_env(
                                    "Connection errors: {}".format(str(metrics[0]["connection_error_counter"]))
                                )
                        break
                    except:
                        pass
                else:
                    try:
                        with metrics[0]["number_of_retries_total"].get_lock():
                            metrics[0]["number_of_retries_total"].value += 1
                            if attempt_num > 0:
                                metrics[0]["retries_per_account"][account_id] = attempt_num
                    except:
                        pass

                    time.sleep(retry_in_secs)
            i = i + retry_in_secs
            method_time = time.time() - start_time
            metrics[0]["api_method_time"].append(method_time)

    def extract_accounts_json(self, data, account_id, k, dir_s3):

        df_accounts = pandas.json_normalize(data)
        df_accounts.columns = df_accounts.columns.str.replace(".", "_")
        df_accounts.drop(columns="primaryContact_person_contacts", inplace=True)
        df_accounts["siteAddress_displayName"].replace(to_replace="\n", value=" ", regex=True, inplace=True)
        df_accounts.replace(",", " ", regex=True)
        df_accounts["account_id"] = account_id
        filename_accounts = "accounts_" + str(account_id) + ".csv"
        df_accounts_string = df_accounts.to_csv(None, index=False)

        k.key = dir_s3["s3_key"]["Accounts"] + filename_accounts
        k.set_contents_from_string(df_accounts_string)

    def format_json_response(self, data):
        """
        This function replaces the null value with empty string as json normalize method will not accept null values in the json data
        :param data: source json data
        :return: formatted json data
        """
        data_str = json.dumps(data, indent=4).replace("null", '""')
        data_json = json.loads(data_str)
        return data_json

    def processAccounts(self, account_ids, k, dir_s3, metrics):

        api_url, head = cdw.common.utils.get_ensek_api_info1("accounts")

        for account_id in account_ids:
            with metrics[0]["account_id_counter"].get_lock():
                metrics[0]["account_id_counter"].value += 1
            api_url1 = api_url.format(account_id)
            api_response = self.get_api_response(api_url1, head, account_id, metrics)
            if api_response:
                formatted_reponse_at = self.format_json_response(api_response)
                self.extract_accounts_json(formatted_reponse_at, account_id, k, dir_s3)
            else:
                metrics[0]["accounts_with_no_data"].append(account_id)


def main():
    freeze_support()

    dir_s3 = cdw.common.utils.get_dir()
    bucket_name = dir_s3["s3_bucket"]

    s3 = s3_con(bucket_name)

    account_ids = []
    """Enable this to test for 1 account id"""
    if cdw.conf.config.test_config["enable_manual"] == "Y":
        account_ids = cdw.conf.config.test_config["account_ids"]

    if cdw.conf.config.test_config["enable_file"] == "Y":
        account_ids = cdw.common.utils.get_Users_from_s3(s3)

    if cdw.conf.config.test_config["enable_db"] == "Y":
        account_ids = cdw.common.utils.get_accountID_fromDB(False)

    if cdw.conf.config.test_config["enable_db_max"] == "Y":
        account_ids = cdw.common.utils.get_accountID_fromDB(True)

    # Enable to test without multiprocessing.
    # p = Accounts()
    # p.processAccounts(account_ids, s3, dir_s3)

    ####### Multiprocessing Starts #########
    total_processes = cdw.common.utils.get_multiprocess("total_ensek_processes")

    accounts_per_process = int(len(account_ids) / total_processes)

    iglog.in_prod_env(f"Total number of account IDs to be processed: {len(account_ids)}")
    iglog.in_prod_env(f"Accounts per process: {accounts_per_process}")
    processes = []
    last_value = 0
    start = timeit.default_timer()

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

        try:
            for i in range(total_processes + 1):
                p1 = Accounts()
                uv = i * accounts_per_process
                if i == total_processes:
                    t = multiprocessing.Process(
                        target=p1.processAccounts,
                        args=(account_ids[last_value:], s3_con(bucket_name), dir_s3, [metrics]),
                    )
                else:
                    t = multiprocessing.Process(
                        target=p1.processAccounts,
                        args=(account_ids[last_value:uv], s3_con(bucket_name), dir_s3, [metrics]),
                    )
                last_value = uv

                processes.append(t)

            for p in processes:
                p.start()
                time.sleep(2)

            for process in processes:
                process.join()
            ###### Multiprocessing Ends #########
        finally:
            start_metrics = timeit.default_timer()
            ####### Multiprocessing Ends #########
            if len(metrics["api_method_time"]) != 0:
                metrics["max_api_process_time"] = max(metrics["api_method_time"])
                metrics["min_api_process_time"] = min(metrics["api_method_time"])
                metrics["median_api_process_time"] = statistics.median(metrics["api_method_time"])
                metrics["average_api_process_time"] = statistics.mean(metrics["api_method_time"])
            else:
                metrics["max_api_process_time"] = "No api times processed"
                metrics["min_api_process_time"] = "No api times processed"
                metrics["median_api_process_time"] = "No api times processed"
                metrics["average_api_process_time"] = "No api times processed"

            metrics["connection_error_counter"] = metrics["connection_error_counter"].value
            metrics["number_of_retries_total"] = metrics["number_of_retries_total"].value
            metrics["account_id_counter"] = metrics["account_id_counter"].value

            iglog.in_prod_env("Process completed in " + str(timeit.default_timer() - start) + " seconds\n")
            iglog.in_prod_env("Metrics completed in " + str(timeit.default_timer() - start_metrics) + " seconds\n")
            iglog.in_prod_env("METRICS FROM CURRENT RUN...\n")
            for metric_name, metric_data in metrics.items():
                if metric_name in [
                    "api_method_time",
                ]:
                    continue

                if isinstance(metric_data, multiprocessing.managers.ListProxy) and len(metric_data) >= 0:
                    metric_data = len(metric_data)

                iglog.in_prod_env(str(metric_name).upper() + "\n" + str(metric_data) + "\n")


if __name__ == "__main__":
    main()
