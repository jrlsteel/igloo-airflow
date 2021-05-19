import requests
import json
from pandas.io.json import json_normalize
from ratelimit import limits, sleep_and_retry
import time
from time import sleep
import timeit
from requests import ConnectionError
import csv
import multiprocessing
from multiprocessing import Manager, Process, freeze_support, Value
import datetime
import pandas
import logging
import statistics
from collections import defaultdict
import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)

import sys
import os


sys.path.append("..")
sys.path.append("../..")

from common import utils as util
from conf import config as con
from connections.connect_db import get_boto_S3_Connections as s3_con

# Logging Config
logger = logging.getLogger("igloo.etl.meterpoints")
logger.setLevel("DEBUG")
iglog = util.IglooLogger()

test_metric = []


class MeterPoints:
    max_calls = con.api_config["max_api_calls"]
    rate = con.api_config["allowed_period_in_secs"]

    def __init__(self, process_num):
        self.pnum = process_num

    @sleep_and_retry
    @limits(calls=max_calls, period=rate)
    def get_api_response(self, api_url, head, account_id, metrics):
        """
        get the response for the respective url that is passed as part of this function
        """
        start_time = time.time()
        timeout = con.api_config["connection_timeout"]
        retry_in_secs = con.api_config["retry_in_secs"]
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
                    response = json.loads(response.content.decode("utf-8"))
                    metrics[0]["api_method_time"].append(method_time)
                    test_metric.append(method_time)
                    return response
                else:
                    # opportunity for custom exception
                    metrics[0]["api_error_codes"].append(response.status_code)
                    break

            # opportunity for custom exception
            except ConnectionError:
                total_api_time += time.time() - api_call_start
                if time.time() > start_time + timeout:
                    try:
                        with metrics[0]["connection_error_counter"].get_lock():
                            metrics[0]["connection_error_counter"].value += 1
                            if metrics[0]["connection_error_counter"].value % 100 == 0:
                                iglog.in_prod_env(
                                    "Connection errors: {}".format(
                                        str(metrics[0]["connection_error_counter"])
                                    )
                                )
                        break
                    except:
                        pass
                else:
                    try:
                        with metrics[0]["number_of_retries_total"].get_lock():
                            metrics[0]["number_of_retries_total"].value += 1
                            if attempt_num > 0:
                                metrics[0]["retries_per_account"][
                                    account_id
                                ] = attempt_num
                    except:
                        pass
                    time.sleep(retry_in_secs)
            i = i + retry_in_secs
        method_time = time.time() - start_time
        metrics[0]["api_method_time"].append(method_time)

    def extract_meter_point_json(self, data, account_id, k, dir_s3, metrics):
        """
        Extracting meterpoints, registers, meters, attributes data
        Writing into meter_points.csv , registers.csv, meteres.csv, attributes.csv
        key = meter_point_id
        """
        meter_point_ids = []

        """ Processing meter points data"""
        meta_meters = [
            "associationStartDate",
            "associationEndDate",
            "supplyStartDate",
            "supplyEndDate",
            "isSmart",
            "isSmartCommunicating",
            "id",
            "meterPointNumber",
            "meterPointType",
        ]
        df_meterpoints = json_normalize(data)
        if df_meterpoints.empty:
            # opportunity for custom exception here
            metrics[0]["no_meterpoints_data"].append(account_id)
        else:
            column_list = util.get_common_info("ensek_column_order", "ref_meterpoints")
            df_meterpoints["account_id"] = account_id
            df_meterpoints1 = df_meterpoints[meta_meters + ["account_id"]]
            df_meterpoints1.rename(columns={"id": "meter_point_id"}, inplace=True)
            meter_point_ids = df_meterpoints1["meter_point_id"]
            df_meter_points_string = df_meterpoints1.to_csv(
                None, columns=column_list, index=False
            )
            file_name_meterpoints = "meter_points_" + str(account_id) + ".csv"
            k.key = dir_s3["s3_key"]["MeterPoints"] + file_name_meterpoints
            k.set_contents_from_string(df_meter_points_string)

        """ Processing meters data"""
        meta_meters = [
            "meterSerialNumber",
            "installedDate",
            "removedDate",
            "meterId",
            "meter_point_id",
        ]
        df_meters = json_normalize(
            data, record_path=["meters"], meta=["id"], meta_prefix="meter_point_"
        )
        if df_meters.empty:
            metrics[0]["no_meters_data"].append(account_id)
        else:
            column_list = util.get_common_info("ensek_column_order", "ref_meters")
            df_meters["account_id"] = account_id
            df_meters1 = df_meters[meta_meters + ["account_id"]]
            df_meters_string = df_meters1.to_csv(None, columns=column_list, index=False)
            filename_meters = "meters_" + str(account_id) + ".csv"
            k.key = dir_s3["s3_key"]["Meters"] + filename_meters
            k.set_contents_from_string(df_meters_string)

        """ Processing attributes data"""
        df_attributes = json_normalize(
            data,
            record_path=["attributes"],
            record_prefix="attributes_",
            meta=["id"],
            meta_prefix="meter_point_",
        )
        if df_attributes.empty:
            metrics[0]["no_meterpoints_attributes_data"].append(account_id)
        else:
            column_list = util.get_common_info(
                "ensek_column_order", "ref_meterpoints_attributes"
            )
            df_attributes["account_id"] = account_id
            df_attributes["attributes_attributeValue"] = df_attributes[
                "attributes_attributeValue"
            ].str.replace(",", " ")
            # df_attributes.to_csv('attributes_'  + str(account_id) + '.csv')
            df_attributes_string = df_attributes.to_csv(
                None, columns=column_list, index=False
            )
            filename_attributes = "mp_attributes_" + str(account_id) + ".csv"
            # k.key = 'ensek-meterpoints/Attributes/' + filename_attributes
            k.key = dir_s3["s3_key"]["MeterPointsAttributes"] + filename_attributes
            k.set_contents_from_string(df_attributes_string)

        """ Processing registers data"""
        ordered_columns = [
            "registers_eacAq",
            "registers_registerReference",
            "registers_sourceIdType",
            "registers_tariffComponent",
            "registers_tpr",
            "registers_tprPeriodDescription",
            "meter_point_meters_meterId",
            "registers_id",
            "meter_point_id",
        ]
        df_registers = json_normalize(
            data,
            record_path=["meters", "registers"],
            meta=["id", ["meters", "meterId"]],
            meta_prefix="meter_point_",
            record_prefix="registers_",
            sep="_",
        )
        if df_registers.empty:
            metrics[0]["no_registers_data"].append(account_id)
        else:
            column_list = util.get_common_info("ensek_column_order", "ref_registers")
            df_registers1 = df_registers[ordered_columns]
            df_registers1.rename(
                columns={
                    "meter_point_meters_meterId": "meter_id",
                    "registers_id": "register_id",
                },
                inplace=True,
            )
            # df_registers.rename(columns={'registers_id' : 'register_id'}, inplace=True)
            df_registers1["account_id"] = account_id
            df_registers1["registers_sourceIdType"] = (
                df_registers1["registers_sourceIdType"]
                .str.replace("\t", "")
                .str.strip()
            )
            df_registers_string = df_registers1.to_csv(
                None, columns=column_list, index=False
            )
            filename_registers = "registers_" + str(account_id) + ".csv"
            # k.key = 'ensek-meterpoints/Registers/' + filename_registers
            k.key = dir_s3["s3_key"]["Registers"] + filename_registers
            k.set_contents_from_string(df_registers_string)

        """ Prcessing registers -> attributes data """
        df_registersAttributes = json_normalize(
            data,
            record_path=["meters", "registers", "attributes"],
            meta=[["meters", "meterId"], ["meters", "registers", "id"], "id"],
            meta_prefix="meter_point_",
            record_prefix="registersAttributes_",
            sep="_",
        )
        if df_registersAttributes.empty:
            metrics[0]["no_registers_attributes_data"].append(account_id)

        else:
            column_list = util.get_common_info(
                "ensek_column_order", "ref_registers_attributes"
            )
            df_registersAttributes.rename(
                columns={"meter_point_meters_meterId": "meter_id"}, inplace=True
            )
            df_registersAttributes.rename(
                columns={"meter_point_meters_registers_id": "register_id"}, inplace=True
            )
            df_registersAttributes["account_id"] = account_id
            df_registersAttributes_string = df_registersAttributes.to_csv(
                None, columns=column_list, index=False
            )
            filename_registersAttributes = (
                "registersAttributes_" + str(account_id) + ".csv"
            )
            k.key = (
                dir_s3["s3_key"]["RegistersAttributes"] + filename_registersAttributes
            )
            k.set_contents_from_string(df_registersAttributes_string)

        """ Prcessing Meters -> attributes data"""
        df_metersAttributes = json_normalize(
            data,
            record_path=["meters", "attributes"],
            meta=[["meters", "meterId"], "id"],
            meta_prefix="meter_point_",
            record_prefix="metersAttributes_",
            sep="_",
        )
        if df_metersAttributes.empty:
            metrics[0]["no_meters_attributes_data"].append(account_id)

        else:
            column_list = util.get_common_info(
                "ensek_column_order", "ref_meters_attributes"
            )
            df_metersAttributes.rename(
                columns={"meter_point_meters_meterId": "meter_id"}, inplace=True
            )
            df_metersAttributes["account_id"] = account_id
            df_metersAttributes[
                "metersAttributes_attributeValue"
            ] = df_metersAttributes["metersAttributes_attributeValue"].str.replace(
                ",", " "
            )
            df_metersAttributes_string = df_metersAttributes.to_csv(
                None, columns=column_list, index=False
            )
            filename_metersAttributes = "metersAttributes_" + str(account_id) + ".csv"
            k.key = dir_s3["s3_key"]["MetersAttributes"] + filename_metersAttributes
            k.set_contents_from_string(df_metersAttributes_string)

        return meter_point_ids

    """Format Json to handle null values"""

    def format_json_response(self, data):
        data_str = json.dumps(data, indent=4).replace("null", '""')
        data_json = json.loads(data_str)
        return data_json

    def processAccounts(self, account_ids, S3, dir_s3, metrics):
        api_url_mp, head_mp = util.get_ensek_api_info1("meterpoints")
        api_url_mpr, head_mpr = util.get_ensek_api_info1("meterpoints_readings")
        api_url_mprb, head_mprb = util.get_ensek_api_info1(
            "meterpoints_readings_billeable"
        )

        current_account = ""
        for account_id in account_ids:
            current_account = account_id
            with metrics[0]["account_id_counter"].get_lock():
                metrics[0]["account_id_counter"].value += 1
                if metrics[0]["account_id_counter"].value % 1000 == 0:
                    iglog.in_prod_env(
                        "Account IDs processesed: {}".format(
                            str(metrics[0]["account_id_counter"].value)
                        )
                    )
            api_url_mp1 = api_url_mp.format(account_id)
            meter_info_response = self.get_api_response(
                api_url_mp1, head_mp, account_id, metrics
            )
            if meter_info_response:
                formatted_meter_info = self.format_json_response(meter_info_response)
                meter_points = self.extract_meter_point_json(
                    formatted_meter_info, account_id, S3, dir_s3, metrics
                )
                list_meterpoints = []
                for each_meter_point in meter_points:
                    list_meterpoints.append(each_meter_point)
                metrics[0]["each_account_meterpoints"].append(list_meterpoints)
            else:
                metrics[0]["accounts_with_no_data"].append(account_id)
        iglog.in_prod_env("Last account ID processed: {}".format(current_account))


def process_api_extract_meterpoints():
    freeze_support()

    # s3 bucket config

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

    # Enable to test without multiprocessing.
    # p = MeterPoints()
    # p.processAccounts(account_ids, s3, dir_s3)

    ####### Multiprocessing Starts #########
    env = util.get_env()
    total_processes = util.get_multiprocess("total_ensek_processes")

    if env == "uat":
        n = total_processes  # number of process to run in parallel
    else:
        n = total_processes

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
            "no_meterpoints_data": manager.list(),
            "no_meters_data": manager.list(),
            "no_meterpoints_attributes_data": manager.list(),
            "no_registers_data": manager.list(),
            "no_registers_attributes_data": manager.list(),
            "no_meters_attributes_data": manager.list(),
            "account_id_counter": Value("i", 0),
            "accounts_with_no_data": manager.list(),
            "each_account_meterpoints": manager.list(),
        }

        try:
            for i in range(n + 1):
                p1 = MeterPoints(i)
                uv = i * k
                if i == n:
                    # print(d18_keys_s3[l:])
                    t = multiprocessing.Process(
                        target=p1.processAccounts,
                        args=(account_ids[lv:], s3_con(bucket_name), dir_s3, [metrics]),
                    )
                else:
                    # print(d18_keys_s3[l:u])
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
                sleep(2)

            for process in processes:
                process.join()

        finally:
            start_metrics = timeit.default_timer()
            ####### Multiprocessing Ends #########
            if metrics["api_method_time"] != []:
                metrics["max_api_process_time"] = max(metrics["api_method_time"])
                metrics["min_api_process_time"] = min(metrics["api_method_time"])
                metrics["median_api_process_time"] = statistics.median(
                    metrics["api_method_time"]
                )
                metrics["average_api_process_time"] = statistics.mean(
                    metrics["api_method_time"]
                )
            else:
                metrics["max_api_process_time"] = "No api times processed"
                metrics["min_api_process_time"] = "No api times processed"
                metrics["median_api_process_time"] = "No api times processed"
                metrics["average_api_process_time"] = "No api times processed"

            metrics["connection_error_counter"] = metrics[
                "connection_error_counter"
            ].value
            metrics["number_of_retries_total"] = metrics[
                "number_of_retries_total"
            ].value
            metrics["account_id_counter"] = metrics["account_id_counter"].value

            iglog.in_prod_env(
                "Process completed in "
                + str(timeit.default_timer() - start)
                + " seconds\n"
            )
            iglog.in_prod_env(
                "Metrics completed in "
                + str(timeit.default_timer() - start_metrics)
                + " seconds\n"
            )
            iglog.in_prod_env("METRICS FROM CURRENT RUN...\n")
            for metric_name, metric_data in metrics.items():
                if metric_name in [
                    "api_method_time",
                    "each_account_meterpoints",
                ]:
                    continue
                iglog.in_prod_env(
                    str(metric_name).upper() + "\n" + str(metric_data) + "\n"
                )


if __name__ == "__main__":
    process_api_extract_meterpoints()
