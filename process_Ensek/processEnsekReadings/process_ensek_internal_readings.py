import timeit
import statistics
import requests
import json
import pandas
from ratelimit import limits, sleep_and_retry
import time
from requests import ConnectionError
import csv
from multiprocessing import Manager, Process, freeze_support, Value
import multiprocessing
from multiprocessing import freeze_support

import sys
import os

sys.path.append("..")
sys.path.append("../..")

from common import etl_metrics
from common import utils as util
from conf import config as con
from connections.connect_db import get_boto_S3_Connections as s3_con

iglog = util.IglooLogger(source="EnsekInternalReadings")


class InternalReadings:
    max_calls = con.api_config["max_api_calls"]
    rate = con.api_config["allowed_period_in_secs"]

    def __init__(self):
        pass

    @sleep_and_retry
    @limits(calls=max_calls, period=rate)
    def get_api_response(self, api_url, head, account_id, metrics):
        """
        get the response for the respective url that is passed as part of this function
        """
        session = requests.Session()
        start_time = time.time()
        timeout = con.api_config["connection_timeout"]
        retry_in_secs = con.api_config["retry_in_secs"]
        attempt_num = 0
        i = 0
        response_items = []
        while True:
            try:
                response = session.post(api_url, headers=head, params={"page": 1})

                if response.status_code == 200:
                    response_json = json.loads(response.content.decode("utf-8"))
                    total_pages = response_json["totalPages"]
                    response_items.extend(response_json["items"])
                    for page in range(2, total_pages + 1):
                        response_next_page = session.post(api_url, headers=head, params={"page": page})
                        response_next_page_json = json.loads(response_next_page.content.decode("utf-8"))["items"]
                        response_items.extend(response_next_page_json)
                else:
                    metrics[0]["api_error_codes"].append(response.status_code)
                break

            except ConnectionError:
                with metrics[0]["connection_error_counter"].get_lock():
                    metrics[0]["connection_error_counter"].value += 1
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
                        break
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
        return response_items

    def extract_internal_data_response(self, data, account_id, k, dir_s3, metrics):
        """Processing meter points data"""
        column_list = util.get_common_info("ensek_column_order", "ref_readings_internal")
        df_internal_readings = pandas.json_normalize(data)
        if df_internal_readings.empty:
            metrics[0]["no_internal_readings_data"].append(account_id)
        else:
            df_internal_readings_string = df_internal_readings.to_csv(None, columns=column_list, index=False)
            file_name_internal_readings = "internal_readings_" + str(account_id) + ".csv"
            k.key = dir_s3["s3_key"]["ReadingsInternal"] + file_name_internal_readings
            k.set_contents_from_string(df_internal_readings_string)

    """Format Json to handle null values"""

    def format_json_response(self, data):
        data_str = json.dumps(data, indent=4).replace("null", '""')
        data_json = json.loads(data_str)
        return data_json

    def processAccounts(self, account_ids, k, dir_s3, metrics):
        current_account_id = ""
        api_url, head = util.get_ensek_api_info1("internal_readings")
        for account_id in account_ids:
            current_account_id = account_id
            with metrics[0]["account_id_counter"].get_lock():
                metrics[0]["account_id_counter"].value += 1
                if metrics[0]["account_id_counter"].value % 1000 == 0:
                    iglog.in_prod_env("Account IDs processesed: {}".format(str(metrics[0]["account_id_counter"].value)))
            api_url1 = api_url.format(account_id)
            internal_data_response = self.get_api_response(api_url1, head, account_id, metrics)
            if internal_data_response:
                formatted_internal_data = self.format_json_response(internal_data_response)
                self.extract_internal_data_response(formatted_internal_data, account_id, k, dir_s3, metrics)
            else:
                metrics[0]["accounts_with_no_data"].append(account_id)


def main():
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

    if con.test_config["enable_db_max"] == "Y":
        account_ids = util.get_accountID_fromDB(True)

    p = int(len(account_ids) / 12)

    # Enable to test without multiprocessing.
    # p.processAccounts(account_ids, s3, dir_s3)

    ####### Multiprocessing Starts #########
    env = util.get_env()

    if env == "uat":
        n = 12  # number of process to run in parallel
    else:
        n = 12

    iglog.in_prod_env("Number of account IDs to be processed -  {}".format(len(account_ids)))
    iglog.in_prod_env("Number of threads for process -  {}".format(n))
    iglog.in_prod_env(
        "Number of account IDs to be processed per thread -  {}".format("{:.2f}".format(len(account_ids) / n))
    )

    k = int(len(account_ids) / n)  # get equal no of files for each process

    processes = []
    lv = 0
    start = timeit.default_timer()

    # Raw Metrics
    with Manager() as manager:

        # metrics = manager.dict()
        metrics = {
            "api_error_codes": manager.list(),
            "connection_error_counter": Value("i", 0),
            "number_of_retries_total": Value("i", 0),
            "retries_per_account": manager.dict(),
            "api_method_time": manager.list(),
            "no_internal_readings_data": manager.list(),
            "account_id_counter": Value("i", 0),
            "accounts_with_no_data": manager.list(),
            "dict_api_error_codes": manager.dict(),
        }

        try:
            for i in range(n + 1):
                p1 = InternalReadings()
                uv = i * k
                if i == n:
                    t = multiprocessing.Process(
                        target=p1.processAccounts,
                        args=(account_ids[lv:], s3_con(bucket_name), dir_s3, [metrics]),
                    )
                else:
                    t = multiprocessing.Process(
                        target=p1.processAccounts,
                        args=(
                            account_ids[lv:uv],
                            s3_con(bucket_name),
                            dir_s3,
                            [metrics],
                        ),
                    )
                lv = uv

                processes.append(t)

            for p in processes:
                p.start()
                time.sleep(2)

            for process in processes:
                process.join()
            ####### Multiprocessing Ends #########
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
            etl_metrics.publish_metrics(metrics)


if __name__ == "__main__":
    main()
