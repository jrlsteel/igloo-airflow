import timeit
import requests
import json
import pandas
from ratelimit import limits, sleep_and_retry
import time

import multiprocessing
from multiprocessing import freeze_support
from multiprocessing import Manager, freeze_support, Value
import statistics

import sys
import os


import cdw.common
import cdw.common.utils
from cdw.conf import config
from cdw.connections.connect_db import get_boto_S3_Connections as s3_con

iglog = cdw.common.utils.IglooLogger(source="EnsekInternalEstimates")


class InternalEstimates:
    max_calls = config.api_config["max_api_calls"]
    rate = config.api_config["allowed_period_in_secs"]

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
        timeout = config.api_config["connection_timeout"]
        retry_in_secs = config.api_config["retry_in_secs"]
        attempt_num = 0
        i = 0
        while True:
            try:
                response = session.get(api_url, headers=head)
                if response.status_code == 200:
                    response_json = json.loads(response.content.decode("utf-8"))
                    response_items_elec = response_json["Electricity"]
                    response_items_gas = response_json["Gas"]
                    return response_items_elec, response_items_gas
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

    def extract_internal_data_response_elec(self, data, account_id, k, dir_s3, metrics):
        """Processing meter points data"""
        df_internal_readings = pandas.json_normalize(data)
        if df_internal_readings.empty:
            metrics[0]["no_internal_estimates_elec_data"].append(account_id)
        else:
            df_internal_readings["account_id"] = account_id
            column_list = cdw.common.utils.get_common_info("ensek_column_order", "internal_estimates_elec")
            df_internal_readings_string = df_internal_readings.to_csv(None, columns=column_list, index=False)
            file_name_internal_readings = "internal_estimates_elec_" + str(account_id) + ".csv"
            k.key = dir_s3["s3_key"]["EstimatesElecInternal"] + file_name_internal_readings
            k.set_contents_from_string(df_internal_readings_string)

    def extract_internal_data_response_gas(self, data, account_id, k, dir_s3, metrics):
        """Processing meter points data"""
        df_internal_readings = pandas.json_normalize(data)
        if df_internal_readings.empty:
            metrics[0]["no_internal_estimates_gas_data"].append(account_id)
        else:
            df_internal_readings["account_id"] = account_id
            column_list = cdw.common.utils.get_common_info("ensek_column_order", "internal_estimates_gas")
            df_internal_readings_string = df_internal_readings.to_csv(None, columns=column_list, index=False)
            file_name_internal_readings = "internal_estimates_gas_" + str(account_id) + ".csv"
            k.key = dir_s3["s3_key"]["EstimatesGasInternal"] + file_name_internal_readings
            k.set_contents_from_string(df_internal_readings_string)

    """Read Users from S3"""

    def get_Users(k):
        # global k
        filename_Users = "users.csv"
        k.key = "ensek-meterpoints/Users/" + filename_Users
        k.open()
        l = k.read()
        s = l.decode("utf-8")
        p = s.splitlines()
        # print(len(p))
        return p

    """Format Json to handle null values"""

    def format_json_response(self, data):
        data_str = json.dumps(data, indent=4).replace("null", '""')
        data_json = json.loads(data_str)
        return data_json

    def processAccounts(self, account_ids, k, _dir_s3, metrics):

        api_url, head = cdw.common.utils.get_ensek_api_info1("internal_estimates")
        for account_id in account_ids:
            with metrics[0]["account_id_counter"].get_lock():
                metrics[0]["account_id_counter"].value += 1
                if metrics[0]["account_id_counter"].value % 1000 == 0:
                    iglog.in_prod_env("Account IDs processesed: {}".format(str(metrics[0]["account_id_counter"].value)))
            api_url1 = api_url.format(account_id)
            internal_data_response_elec, internal_data_response_gas = self.get_api_response(
                api_url1, head, account_id, metrics
            )

            if internal_data_response_elec:
                formatted_internal_data_elec = self.format_json_response(internal_data_response_elec)
                self.extract_internal_data_response_elec(formatted_internal_data_elec, account_id, k, _dir_s3, metrics)

            if internal_data_response_gas:
                formatted_internal_data_gas = self.format_json_response(internal_data_response_gas)
                self.extract_internal_data_response_gas(formatted_internal_data_gas, account_id, k, _dir_s3, metrics)

            if not internal_data_response_elec and internal_data_response_gas:
                metrics[0]["accounts_with_no_data"].append(account_id)


def main():
    freeze_support()

    dir_s3 = cdw.common.utils.get_dir()
    bucket_name = dir_s3["s3_bucket"]

    s3 = s3_con(bucket_name)

    account_ids = []
    """Enable this to test for 1 account id"""
    if config.test_config["enable_manual"] == "Y":
        account_ids = config.test_config["account_ids"]

    if config.test_config["enable_file"] == "Y":
        account_ids = cdw.common.utils.get_Users_from_s3(s3)

    if config.test_config["enable_db"] == "Y":
        account_ids = cdw.common.utils.get_accountID_fromDB(False)

    if config.test_config["enable_db_max"] == "Y":
        account_ids = cdw.common.utils.get_accountID_fromDB(True)

    ####### Multiprocessing Starts #########
    total_processes = cdw.common.utils.get_multiprocess("total_ensek_processes")
    accounts_per_process = int(len(account_ids) / total_processes)
    iglog.in_prod_env(
        "Number of account IDs to be processed per thread -  {}".format(
            "{:.2f}".format(len(account_ids) / total_processes)
        )
    )
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
            "no_internal_estimates_elec_data": manager.list(),
            "no_internal_estimates_gas_data": manager.list(),
            "account_id_counter": Value("i", 0),
            "accounts_with_no_data": manager.list(),
        }

        try:
            for i in range(1, total_processes + 1):
                p1 = InternalEstimates()
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
                if isinstance(metric_data, multiprocessing.managers.ListProxy) and len(metric_data) >= 50:
                    metric_data = len(metric_data)
                iglog.in_prod_env(str(metric_name).upper() + "\n" + str(metric_data) + "\n")


if __name__ == "__main__":
    main()
