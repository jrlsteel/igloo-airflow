import timeit

import requests
import json
import pandas
from ratelimit import limits, sleep_and_retry
import time
from requests import ConnectionError
import csv
import multiprocessing
from multiprocessing import Manager, Value
import sys
import os

sys.path.append('..')
sys.path.append('../..')

from common import utils
from conf import config
from connections.connect_db import get_boto_S3_Connections as s3_con
import statistics

iglog = utils.IglooLogger()

class TariffHistory(object):
    max_calls = config.api_config['max_api_calls']
    rate = config.api_config['allowed_period_in_secs']

    @sleep_and_retry
    @limits(calls=max_calls, period=rate)
    def get_api_response(self, api_url, headers, account_id, metrics):
        '''
             get the response for the respective url that is passed as part of this function
        '''
        start_time = time.time()
        timeout = config.api_config['connection_timeout']
        retry_in_secs = config.api_config['retry_in_secs']
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
                    response = json.loads(response.content.decode('utf-8'))
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
                            iglog.in_prod_env(
                                "Connection errors: {}".format(
                                    str(metrics["connection_error_counter"])
                                )
                            )
                    break
                else:
                    with metrics["number_of_retries_total"].get_lock():
                        metrics["number_of_retries_total"].value += 1
                        if attempt_num > 0:
                            metrics["retries_per_account"][
                                account_id
                            ] = attempt_num
                    time.sleep(retry_in_secs)
                    print('done sleeping, retrying now')
            i = i + retry_in_secs

    def extract_tariff_history_json(self, data, account_id, k, dir_s3):
        meta_tariff_history = ['tariffName', 'startDate', 'endDate', 'discounts', 'tariffType', 'exitFees', 'account_id']

        df_tariff_history = pandas.json_normalize(data)
        df_tariff_history['account_id'] = account_id
        df_tariff_history1 = df_tariff_history[meta_tariff_history]

        filename_tariff_history = 'df_tariff_history_' + str(account_id) + '.csv'
        column_list = utils.get_common_info('ensek_column_order', 'tariff_history')
        df_tariff_history_string = df_tariff_history1.to_csv(None, columns=column_list, index=False)
        k.key = dir_s3['s3_key']['TariffHistory'] + filename_tariff_history
        k.set_contents_from_string(df_tariff_history_string)

        # Elec
        if ('Electricity' in df_tariff_history.columns and not df_tariff_history['Electricity'].isnull) or not ('Electricity' in df_tariff_history.columns):
            # UnitRates
            meta_elec_unitrates = ['tariffName', 'startDate', 'endDate']
            df_elec_unit_rates = pandas.json_normalize(data, record_path=[['Electricity', 'unitRates']], meta=meta_elec_unitrates)

            if not df_elec_unit_rates.empty:
                df_elec_unit_rates['account_id'] = account_id
                filename_elec_unit_rates = 'th_elec_unitrates_' + str(account_id) + '.csv'
                column_list = utils.get_common_info('ensek_column_order', 'tariff_history_elec_unit_rates')
                df_elec_unit_rates_string = df_elec_unit_rates.to_csv(None, columns=column_list, index=False)
                k.key = dir_s3['s3_key']['TariffHistoryElecUnitRates'] + filename_elec_unit_rates
                k.set_contents_from_string(df_elec_unit_rates_string)

            #  StandingCharge
            meta_elec_standing_charge = ['tariffName', 'startDate', 'endDate']
            df_elec_standing_charge = pandas.json_normalize(data, record_path=[['Electricity', 'standingChargeRates']], meta=meta_elec_standing_charge)

            if not df_elec_standing_charge.empty:
                df_elec_standing_charge['account_id'] = account_id

                filename_elec_standing_charge = 'th_elec_standingcharge_' + str(account_id) + '.csv'
                column_list = utils.get_common_info('ensek_column_order', 'tariff_history_elec_standing_charge')
                df_elec_standing_charge_string = df_elec_standing_charge.to_csv(None, columns=column_list, index=False)
                k.key = dir_s3['s3_key']['TariffHistoryElecStandCharge'] + filename_elec_standing_charge
                k.set_contents_from_string(df_elec_standing_charge_string)

        # Gas
        if ('Gas' in df_tariff_history.columns and not df_tariff_history['Gas'].isnull) or not ('Gas' in df_tariff_history.columns):
            # UnitRates
            meta_gas_unitrates = ['tariffName', 'startDate', 'endDate']
            df_gas_unit_rates = pandas.json_normalize(data, record_path=[['Gas', 'unitRates']], meta=meta_gas_unitrates)

            if not df_gas_unit_rates.empty:
                df_gas_unit_rates['account_id'] = account_id

                filename_gas_unit_rates = 'th_gas_unitrates_' + str(account_id) + '.csv'
                column_list = utils.get_common_info('ensek_column_order', 'tariff_history_gas_unit_rates')
                df_gas_unit_rates_string = df_gas_unit_rates.to_csv(None, columns=column_list, index=False)
                k.key = dir_s3['s3_key']['TariffHistoryGasUnitRates'] + filename_gas_unit_rates
                k.set_contents_from_string(df_gas_unit_rates_string)

            # StandingCharge
            meta_gas_standing_charge = ['tariffName', 'startDate', 'endDate']
            df_elec_standing_charge = pandas.json_normalize(data, record_path=[['Gas', 'standingChargeRates']], meta=meta_gas_standing_charge)

            if not df_elec_standing_charge.empty:
                df_elec_standing_charge['account_id'] = account_id

                filename_gas_standing_charge = 'th_gas_standingcharge_' + str(account_id) + '.csv'
                column_list = utils.get_common_info('ensek_column_order', 'tariff_history_gas_standing_charge')
                df_gas_standing_charge_string = df_elec_standing_charge.to_csv(None, columns=column_list, index=False)
                k.key = dir_s3['s3_key']['TariffHistoryGasStandCharge'] + filename_gas_standing_charge
                k.set_contents_from_string(df_gas_standing_charge_string)

    def format_json_response(self, data):
        data_str = json.dumps(data, indent=4).replace('null', '""')
        data_json = json.loads(data_str)
        return data_json

    def process_accounts(self, account_ids, k, dir_s3, metrics):
        api_url_template, headers = utils.get_ensek_api_info1('tariff_history')
        for account_id in account_ids:
            with metrics["account_id_counter"].get_lock():
                metrics["account_id_counter"].value += 1
                if metrics["account_id_counter"].value % 1000 == 0:
                    iglog.in_prod_env(
                        "Account IDs processesed: {}".format(
                            str(metrics["account_id_counter"].value)
                        )
                    )
            api_url = api_url_template.format(account_id)
            response = self.get_api_response(api_url, headers, account_id, metrics)
            if response:
                formatted_tariff_history = self.format_json_response(response)
                self.extract_tariff_history_json(formatted_tariff_history, account_id, k, dir_s3)
            else:
                metrics["accounts_with_no_data"].append(account_id)


def process_ensek_tariffs_history():
    dir_s3 = utils.get_dir()
    bucket_name = dir_s3['s3_bucket']

    s3 = s3_con(bucket_name)

    account_ids = []
    # Enable this to test for 1 account id
    if config.test_config['enable_manual'] == 'Y':
        account_ids = config.test_config['account_ids']

    if config.test_config['enable_file'] == 'Y':
        account_ids = utils.get_Users_from_s3(s3)

    if config.test_config['enable_db'] == 'Y':
        account_ids = utils.get_accountID_fromDB(False, filter='tariff-diffs')

    if config.test_config['enable_db_max'] == 'Y':
        account_ids = utils.get_accountID_fromDB(True, filter='tariff-diffs')

    # Enable to test without multiprocessing.
    # p.process_accounts(account_ids, s3, dir_s3)

    total_processes = utils.get_multiprocess('total_ensek_processes')

    k = int(len(account_ids) / total_processes)  # get equal no of files for each process

    iglog.in_prod_env(f'Total processes: {total_processes}')
    iglog.in_prod_env(f'Total accounts: {len(account_ids)}')

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
            "no_tariffs_history_data": manager.list(),
            "account_id_counter": Value("i", 0),
            "accounts_with_no_data": manager.list(),
        }

        try:
            for i in range(1, total_processes + 1):
                p1 = TariffHistory()
                uv = i * k
                if i == total_processes:
                    iglog.in_prod_env(f'Starting process {i} with {len(account_ids[lv:])} accounts')
                    t = multiprocessing.Process(target=p1.process_accounts, args=(account_ids[lv:], s3_con(bucket_name), dir_s3, metrics))
                else:
                    iglog.in_prod_env(f'Starting process {i} with {len(account_ids[lv:uv])} accounts')
                    t = multiprocessing.Process(target=p1.process_accounts, args=(account_ids[lv:uv], s3_con(bucket_name), dir_s3, metrics))
                lv = uv

                processes.append(t)

            for p in processes:
                p.start()
                time.sleep(2)

            for process in processes:
                process.join()

        finally:
            start_metrics = timeit.default_timer()

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
                ]:
                    continue
                if isinstance(metric_data, multiprocessing.managers.ListProxy) and len(metric_data) >= 0:
                    metric_data = len(metric_data)
                iglog.in_prod_env(
                    str(metric_name).upper() + "\n" + str(metric_data) + "\n"
                )


if __name__ == "__main__":
    process_ensek_tariffs_history()
