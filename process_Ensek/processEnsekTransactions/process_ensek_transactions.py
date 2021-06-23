import timeit

import requests
import json
import pandas
from ratelimit import limits, sleep_and_retry
import time
from requests import ConnectionError
import multiprocessing
from multiprocessing import freeze_support
from multiprocessing import Manager, freeze_support, Value
import statistics

import sys
import os

sys.path.append("..")
sys.path.append("../..")

import common
import common.utils
from conf import config
from connections.connect_db import get_boto_S3_Connections as s3_con


iglog = common.utils.IglooLogger("Process_Ensek_Transactions")


class AccountTransactions:
    max_calls = config.api_config["max_api_calls"]
    rate = config.api_config["allowed_period_in_secs"]

    def __init__(self, process_num):
        logs_dir_path = sys.path[0] + "/logs/"
        if not os.path.exists(logs_dir_path):
            os.makedirs(logs_dir_path)
        self.pnum = process_num

    @sleep_and_retry
    @limits(calls=max_calls, period=rate)
    def get_api_response(self, api_url, head, account_id, metrics):
        """
        get the response for the respective url that is passed as part of this function
        """
        start_time = time.time()
        timeout = config.api_config["connection_timeout"]
        retry_in_secs = config.api_config["retry_in_secs"]
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

    def extract_account_transactions_json(self, data, account_id, k, dir_s3, metrics):
        df_account_transactions = pandas.json_normalize(data)

        if df_account_transactions.empty:
            metrics[0]["no_account_transaction_data"].append(account_id)

        df_account_transactions.columns = df_account_transactions.columns.str.replace(".", "_")
        df_account_transactions["account_id"] = account_id
        filename_account_transactions = "account_transactions_" + str(account_id) + ".csv"
        column_list = common.utils.get_common_info("ensek_column_order", "account_transactions")
        df_account_transactions_string = df_account_transactions.to_csv(None, columns=column_list, index=False)

        k.key = dir_s3["s3_key"]["AccountTransactions"] + filename_account_transactions
        k.set_contents_from_string(df_account_transactions_string)

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
        api_url_template, head = common.utils.get_ensek_api_info1("account_transactions")

        for account_id in account_ids:
            with metrics[0]["account_id_counter"].get_lock():
                metrics[0]["account_id_counter"].value += 1
                if metrics[0]["account_id_counter"].value % 1000 == 0:
                    iglog.in_prod_env("Account IDs processesed: {}".format(str(metrics[0]["account_id_counter"].value)))
            # Get Accounts Transactions
            api_url = api_url_template.format(account_id)
            api_response_at = self.get_api_response(api_url, head, account_id, metrics)

            if api_response_at:
                formatted_reponse_at = self.format_json_response(api_response_at)
                self.extract_account_transactions_json(formatted_reponse_at, account_id, k, dir_s3, metrics)
            else:
                metrics[0]["accounts_with_no_data"].append(account_id)


def main():
    freeze_support()

    dir_s3 = common.utils.get_dir()
    bucket_name = dir_s3["s3_bucket"]

    s3 = s3_con(bucket_name)

    account_ids = []
    """Enable this to test for 1 account id"""
    if config.test_config["enable_manual"] == "Y":
        account_ids = config.test_config["account_ids"]

    if config.test_config["enable_file"] == "Y":
        account_ids = common.utils.get_Users_from_s3(s3)

    if config.test_config["enable_db"] == "Y":
        account_ids = common.utils.get_accountID_fromDB(False)

    if config.test_config["enable_db_max"] == "Y":
        account_ids = common.utils.get_accountID_fromDB(True)

    # Enable to test without multiprocessing.
    # p = AccountTransactions()
    # p.processAccounts(account_ids, s3, dir_s3)

    ####### Multiprocessing Starts #########
    total_processes = common.utils.get_multiprocess("total_ensek_processes")

    accounts_per_process = int(len(account_ids) / total_processes)

    iglog.in_prod_env(f"Total number of account IDs to be processed: {len(account_ids)}")
    iglog.in_prod_env(f"Accounts per process: {accounts_per_process}")

    processes = []
    last_value = 0
    start = timeit.default_timer()

    # Raw Metrics
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
                p1 = AccountTransactions(i)
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

            for process in processes:
                process.start()
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
                iglog.in_prod_env(str(metric_name).upper() + "\n" + str(metric_data) + "\n")


if __name__ == "__main__":
    main()
