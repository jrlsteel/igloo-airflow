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
from multiprocessing import Manager, Value
import sys
import os


sys.path.append("..")

from common import utils
from common import api_filters
from conf import config
import connections.connect_db
import statistics

iglog = utils.IglooLogger()


class RegistrationsMeterpointsStatus:
    max_calls = config.api_config["max_api_calls"]
    rate = config.api_config["allowed_period_in_secs"]

    def __init__(self):
        self.sql = api_filters.acc_mp_ids["daily"]  # there is no need for a weekly run here

    @sleep_and_retry
    @limits(calls=max_calls, period=rate)
    def get_api_response(self, api_url, headers, account_id, metrics):
        start_time = time.time()
        timeout = config.api_config["connection_timeout"]
        retry_in_secs = config.api_config["retry_in_secs"]
        i = 0
        attempt_num = 0
        total_api_time = 0.0
        while True:
            try:
                attempt_num += 1
                api_call_start = time.time()
                response = requests.get(api_url, headers=headers)
                total_api_time += time.time() - api_call_start
                if response.status_code == 200:
                    method_time = time.time() - start_time
                    response = response.json()
                    metrics["api_method_time"].append(method_time)
                    return response
                else:
                    metrics["api_error_codes"].append(response.status_code)
                    break

            except ConnectionError:
                total_api_time += time.time() - api_call_start
                if time.time() > start_time + timeout:
                    with metrics["connection_error_counter"].get_lock():
                        metrics["connection_error_counter"].value += 1
                        if metrics["connection_error_counter"].value % 100 == 0:
                            iglog.in_prod_env("Connection errors: {}".format(str(metrics["connection_error_counter"])))
                    break
                else:
                    with metrics["number_of_retries_total"].get_lock():
                        metrics["number_of_retries_total"].value += 1
                        if attempt_num > 0:
                            metrics["retries_per_account"][account_id] = attempt_num
                    time.sleep(retry_in_secs)
            i = i + retry_in_secs

    def extract_reg_elec_json(self, data, _df, k, _dir_s3):

        elec_dict = dict(
            account_id=str(_df["account_id"]).strip(),
            meter_point_id=str(_df["meter_point_id"]).strip(),
            meterpointnumber=str(_df["meterpointnumber"]).strip(),
            status=data,
        )
        elec_str = json.dumps(elec_dict)
        elec_json = json.loads(elec_str)
        df_elec = json_normalize(elec_json)
        filename_elec = "reg_elec_mp_status_" + str(_df["account_id"]) + "_" + str(_df["meterpointnumber"]) + ".csv"
        column_list = utils.get_common_info("ensek_column_order", "registrations_elec_meterpoint")
        df_elec_string = df_elec.to_csv(None, columns=column_list, index=False)

        k.key = _dir_s3["s3_key"]["RegistrationsElecMeterpoint"] + filename_elec
        k.set_contents_from_string(df_elec_string)

    def extract_reg_gas_json(self, data, _df, k, _dir_s3):

        gas_dict = dict(
            account_id=str(_df["account_id"]).strip(),
            meter_point_id=str(_df["meter_point_id"]).strip(),
            meterpointnumber=str(_df["meterpointnumber"]).strip(),
            status=data,
        )
        gas_str = json.dumps(gas_dict)
        gas_json = json.loads(gas_str)
        df_gas = json_normalize(gas_json)
        filename_gas = "reg_gas_mp_status_" + str(_df["account_id"]) + "_" + str(_df["meterpointnumber"]) + ".csv"
        column_list = utils.get_common_info("ensek_column_order", "registrations_gas_meterpoint")
        df_gas_string = df_gas.to_csv(None, columns=column_list, index=False)

        k.key = _dir_s3["s3_key"]["RegistrationsGasMeterpoint"] + filename_gas
        k.set_contents_from_string(df_gas_string)

    """Format Json to handle null values"""

    def format_json_response(self, data):
        data_str = json.dumps(data, indent=4).replace("null", '""')
        data_json = json.loads(data_str)
        return data_json

    def process_accounts(self, _df, k, _dir_s3, metrics):
        api_url_elec, head_elec = utils.get_ensek_api_info1("elec_mp_status")
        api_url_gas, head_gas = utils.get_ensek_api_info1("gas_mp_status")

        for index, df in _df.iterrows():
            with metrics["meterpointnumber_counter"].get_lock():
                metrics["meterpointnumber_counter"].value += 1
                if metrics["meterpointnumber_counter"].value % 1000 == 0:
                    iglog.in_prod_env(
                        "Meterpoints processesed: {}".format(str(metrics["meterpointnumber_counter"].value))
                    )

            # Get Elec details
            if df["meterpointtype"] == "E":
                formatted_url_elec = api_url_elec.format(df["account_id"], df["meterpointnumber"])
                response_elec = self.get_api_response(formatted_url_elec, head_elec, df["account_id"], metrics)

                if response_elec:
                    formated_response_elec = self.format_json_response(response_elec)
                    self.extract_reg_elec_json(formated_response_elec, df, k, _dir_s3)
                else:
                    metrics[0]["meterpointnumbers_with_no_data"].append(df["meterpointnumber"])

            # Get Gas details
            if df["meterpointtype"] == "G":
                api_url_gas1 = api_url_gas.format(df["account_id"], df["meterpointnumber"])
                response_gas = self.get_api_response(api_url_gas1, head_gas, df["account_id"], metrics)

                if response_gas:
                    formated_response_gas = self.format_json_response(response_gas)
                    self.extract_reg_gas_json(formated_response_gas, df, k, _dir_s3)
                else:
                    metrics[0]["meterpointnumbers_with_no_data"].append(df["meterpointnumber"])


def main():
    dir_s3 = utils.get_dir()
    bucket_name = dir_s3["s3_bucket"]

    p = RegistrationsMeterpointsStatus()

    df = utils.execute_query(p.sql)

    total_processes = utils.get_multiprocess("total_ensek_processes")

    k = int(len(df) / total_processes)  # get equal no of files for each process

    iglog.in_prod_env(f"Total processes: {total_processes}")
    iglog.in_prod_env(f"Total items to process: {len(df)}")

    processes = []
    lv = 0
    start = timeit.default_timer()

    with Manager() as manager:
        metrics = {
            "api_error_codes": manager.list(),
            "connection_error_counter": Value("i", 0),
            "number_of_retries_total": Value("i", 0),
            "retries_per_account": manager.dict(),
            "api_method_time": manager.list(),
            "meterpointnumber_counter": Value("i", 0),
            "meterpointnumbers_with_no_data": manager.list(),
        }

        try:
            for i in range(1, total_processes + 1):
                p1 = RegistrationsMeterpointsStatus()
                uv = i * k
                if i == total_processes:
                    iglog.in_prod_env(f"Starting process {i} with {len(df[lv:])} items")
                    t = multiprocessing.Process(
                        target=p1.process_accounts,
                        args=(df[lv:], connections.connect_db.get_boto_S3_Connections(bucket_name), dir_s3, metrics),
                    )
                else:
                    iglog.in_prod_env(f"Starting process {i} with {len(df[lv:uv])} items")
                    t = multiprocessing.Process(
                        target=p1.process_accounts,
                        args=(df[lv:uv], connections.connect_db.get_boto_S3_Connections(bucket_name), dir_s3, metrics),
                    )
                lv = uv

                processes.append(t)

            for p in processes:
                p.start()
                # time.sleep(2)

            for process in processes:
                process.join()

        finally:
            start_metrics = timeit.default_timer()

            if len(metrics["api_method_time"]) > 0:
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
            metrics["meterpointnumber_counter"] = metrics["meterpointnumber_counter"].value

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
