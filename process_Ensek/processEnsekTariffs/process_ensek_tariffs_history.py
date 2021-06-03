import timeit

import requests
import json
import pandas
from ratelimit import limits, sleep_and_retry
import time
from requests import ConnectionError
import csv
import multiprocessing
from multiprocessing import freeze_support

import sys
import os

sys.path.append('..')

from common import utils
from conf import config
from connections.connect_db import get_boto_S3_Connections as s3_con


class TariffHistory(object):
    max_calls = config.api_config['max_api_calls']
    rate = config.api_config['allowed_period_in_secs']

    def __init__(self):
        pass

    @sleep_and_retry
    @limits(calls=max_calls, period=rate)
    def get_api_response(self, api_url, head):
        '''
             get the response for the respective url that is passed as part of this function
        '''
        start_time = time.time()
        timeout = config.api_config['connection_timeout']
        retry_in_secs = config.api_config['retry_in_secs']
        i = 0
        while True:
            try:
                response = requests.get(api_url, headers=head)
                if response.status_code == 200:
                    return json.loads(response.content.decode('utf-8'))
                else:
                    print ('Problem Grabbing Data: ', response.status_code)
                    #self.log_error('Response Error:  Problem grabbing data', response.status_code)
                    break

            except ConnectionError:
                if time.time() > start_time + timeout:
                    print('Unable to Connect after {} seconds of ConnectionErrors'.format(timeout))
                    #self.log_error('Unable to Connect after {} seconds of ConnectionErrors'.format(timeout))

                    break
                else:
                    print('Retrying connection in ' + str(retry_in_secs) + ' seconds' + str(i))
                    #self.log_error('Retrying connection in ' + str(retry_in_secs) + ' seconds' + str(i))

                    time.sleep(retry_in_secs)
            i = i + retry_in_secs


    ''' Extracting direct debit data'''


    def extract_tariff_history_json(self, data, account_id, k, dir_s3):
        # global k,
        meta_tariff_history = ['tariffName', 'startDate', 'endDate', 'discounts', 'tariffType', 'exitFees', 'account_id']

        df_tariff_history = pandas.json_normalize(data)
        df_tariff_history['account_id'] = account_id
        df_tariff_history1 = df_tariff_history[meta_tariff_history]

        filename_tariff_history = 'df_tariff_history_' + str(account_id) + '.csv'
        column_list = utils.get_common_info('ensek_column_order', 'tariff_history')
        df_tariff_history_string = df_tariff_history1.to_csv(None, columns=column_list, index=False)
        # k.key = 'ensek-meterpoints/TariffHistory/' + filename_tariff_history
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
                # k.key = 'ensek-meterpoints/TariffHistoryElecUnitRates/' + filename_elec_unit_rates
                k.key = dir_s3['s3_key']['TariffHistoryElecUnitRates'] + filename_elec_unit_rates
                k.set_contents_from_string(df_elec_unit_rates_string)
                # print(df_elec_unit_rates_string)

            #  StandingCharge
            meta_elec_standing_charge = ['tariffName', 'startDate', 'endDate']
            df_elec_standing_charge = pandas.json_normalize(data, record_path=[['Electricity', 'standingChargeRates']], meta=meta_elec_standing_charge)

            if not df_elec_standing_charge.empty:
                df_elec_standing_charge['account_id'] = account_id

                filename_elec_standing_charge = 'th_elec_standingcharge_' + str(account_id) + '.csv'
                column_list = utils.get_common_info('ensek_column_order', 'tariff_history_elec_standing_charge')
                df_elec_standing_charge_string = df_elec_standing_charge.to_csv(None, columns=column_list, index=False)
                # k.key = 'ensek-meterpoints/TariffHistoryElecStandCharge/' + filename_elec_standing_charge
                k.key = dir_s3['s3_key']['TariffHistoryElecStandCharge'] + filename_elec_standing_charge
                k.set_contents_from_string(df_elec_standing_charge_string)
                # print(df_elec_standing_charge_string)

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
                # k.key = 'ensek-meterpoints/TariffHistoryGasUnitRates/' + filename_gas_unit_rates
                k.key = dir_s3['s3_key']['TariffHistoryGasUnitRates'] + filename_gas_unit_rates
                k.set_contents_from_string(df_gas_unit_rates_string)
                # print(df_gas_unit_rates_string)

            # StandingCharge
            meta_gas_standing_charge = ['tariffName', 'startDate', 'endDate']
            df_elec_standing_charge = pandas.json_normalize(data, record_path=[['Gas', 'standingChargeRates']], meta=meta_gas_standing_charge)

            if not df_elec_standing_charge.empty:
                df_elec_standing_charge['account_id'] = account_id

                filename_gas_standing_charge = 'th_gas_standingcharge_' + str(account_id) + '.csv'
                column_list = utils.get_common_info('ensek_column_order', 'tariff_history_gas_standing_charge')
                df_gas_standing_charge_string = df_elec_standing_charge.to_csv(None, columns=column_list, index=False)
                # k.key = 'ensek-meterpoints/TariffHistoryGasStandCharge/' + filename_gas_standing_charge
                k.key = dir_s3['s3_key']['TariffHistoryGasStandCharge'] + filename_gas_standing_charge
                k.set_contents_from_string(df_gas_standing_charge_string)
                # print(df_gas_standing_charge_string)

    def format_json_response(self, data):
        print(f'data: {data}')
        data_str = json.dumps(data, indent=4).replace('null', '""')
        data_json = json.loads(data_str)
        return data_json

    def log_error(self, error_msg, error_code=''):
        logs_dir_path = sys.path[0] + '/logs/'
        if not os.path.exists(logs_dir_path):
            os.makedirs(logs_dir_path)
        with open(sys.path[0] + '/logs/' + 'tariff_history_logs_' + time.strftime('%d%m%Y') + '.csv', mode='a') as errorlog:
            employee_writer = csv.writer(errorlog, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            employee_writer.writerow([error_msg, error_code])

    def process_accounts(self, account_ids, k, dir_s3):
        api_url, head = utils.get_ensek_api_info1('tariff_history')
        for account_id in account_ids:
            t = config.api_config['total_no_of_calls']
            # Get Account Staus
            print('ac: ' + str(account_id))
            msg_ac = 'ac:' + str(account_id)
            #self.log_error(msg_ac, '')
            api_url1 = api_url.format(account_id)
            th_response = self.get_api_response(api_url1, head)
            print(f'th_response: {th_response}')
            if th_response:
                formatted_tariff_history = self.format_json_response(th_response)
                # print(json.dumps(formatted_tariff_history))
                self.extract_tariff_history_json(formatted_tariff_history, account_id, k, dir_s3)
            else:
                print('ac:' + str(account_id) + ' has no data')
                msg_ac = 'ac:' + str(account_id) + ' has no data'
                #self.log_error(msg_ac, '')


def process_ensek_tariffs_history():
    freeze_support()

    dir_s3 = utils.get_dir()
    bucket_name = dir_s3['s3_bucket']

    s3 = s3_con(bucket_name)

    account_ids = []
    '''Enable this to test for 1 account id'''
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

    ####### Multiprocessing Starts #########
    total_processes = utils.get_multiprocess('total_ensek_processes')

    k = int(len(account_ids) / total_processes)  # get equal no of files for each process

    processes = []
    lv = 0
    start = timeit.default_timer()

    for i in range(1, total_processes + 1):
        p1 = TariffHistory()
        uv = i * k
        if i == total_processes:
            t = multiprocessing.Process(target=p1.process_accounts, args=(account_ids[lv:], s3_con(bucket_name), dir_s3))
        else:
            t = multiprocessing.Process(target=p1.process_accounts, args=(account_ids[lv:uv], s3_con(bucket_name), dir_s3))
        lv = uv

        processes.append(t)

    for p in processes:
        p.start()
        time.sleep(2)

    for process in processes:
        process.join()
    ####### Multiprocessing Ends #########

    print("Process completed in " + str(timeit.default_timer() - start) + ' seconds')


if __name__ == "__main__":
    process_ensek_tariffs_history()
