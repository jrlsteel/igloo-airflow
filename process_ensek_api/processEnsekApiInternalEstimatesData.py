import timeit

import requests
import json
from pandas.io.json import json_normalize
from ratelimit import limits, sleep_and_retry
import time
import datetime
from requests import ConnectionError
import csv
import multiprocessing
from multiprocessing import freeze_support

import sys
import os

sys.path.append('..')

from conf import config as con
from common import utils as util
from connections.connect_db import get_boto_S3_Connections as s3_con


class InternalEstimates:
    max_calls = con.api_config['max_api_calls']
    rate = con.api_config['allowed_period_in_secs']

    def __init__(self):
        pass


    @sleep_and_retry
    @limits(calls=max_calls, period=rate)
    def get_api_response(self, api_url, head):
        """
            get the response for the respective url that is passed as part of this function
        """
        session = requests.Session()
        start_time = time.time()
        timeout = con.api_config['connection_timeout']
        retry_in_secs = con.api_config['retry_in_secs']
        i = 0
        while True:
            try:
                response = session.get(api_url, headers=head)
                if response.status_code == 200:
                    response_json = json.loads(response.content.decode('utf-8'))
                    response_items_elec = response_json['Electricity']
                    response_items_gas = response_json['Gas']
                    return response_items_elec, response_items_gas
                else:
                    print('Problem Grabbing Data: ', response.status_code)
                    log_error('Response Error: Problem grabbing data', response.status_code)
                    return None, None
                    break

            except ConnectionError:
                if time.time() > start_time + timeout:
                    print('Unable to Connect after {} seconds of ConnectionErrors'.format(timeout))
                    log_error('Unable to Connect after {} seconds of ConnectionErrors'.format(timeout))
                    break
                else:
                    print('Retrying connection in ' + str(retry_in_secs) + ' seconds' + str(i))
                    log_error('Retrying connection in ' + str(retry_in_secs) + ' seconds' + str(i))

                    time.sleep(retry_in_secs)
            i = i + retry_in_secs


    def extract_internal_data_response_elec(self, data, account_id, k, dir_s3):
        ''' Processing meter points data'''
        # meta_meters = ['associationStartDate', 'associationEndDate', 'supplyStartDate', 'supplyEndDate', 'isSmart', 'isSmartCommunicating', 'id', 'meterPointNumber', 'meterPointType']
        df_internal_readings = json_normalize(data)
        # print(df_internal_readings)
        if (df_internal_readings.empty):
            print(" - has no readings data")
        else:
            df_internal_readings['account_id'] = account_id
            df_internal_readings_string = df_internal_readings.to_csv(None, index=False)
            file_name_internal_readings = 'internal_estimates_elec_' + str(account_id) + '.csv'
            # k.key = 'ensek-meterpoints/EstimatesElecInternal/' + file_name_internal_readings
            k.key = dir_s3['s3_key']['EstimatesElecInternal'] + file_name_internal_readings
            k.set_contents_from_string(df_internal_readings_string)


    def extract_internal_data_response_gas(self, data, account_id, k, dir_s3):
        ''' Processing meter points data'''
        # meta_meters = ['associationStartDate', 'associationEndDate', 'supplyStartDate', 'supplyEndDate', 'isSmart', 'isSmartCommunicating', 'id', 'meterPointNumber', 'meterPointType']
        df_internal_readings = json_normalize(data)
        # print(df_internal_readings)
        if (df_internal_readings.empty):
            print(" - has no readings data")
        else:
            df_internal_readings['account_id'] = account_id
            df_internal_readings_string = df_internal_readings.to_csv(None, index=False)
            file_name_internal_readings = 'internal_estimates_gas_' + str(account_id) + '.csv'
            # k.key = 'ensek-meterpoints/EstimatesGasInternal/' + file_name_internal_readings
            k.key = dir_s3['s3_key']['EstimatesGasInternal'] + file_name_internal_readings
            k.set_contents_from_string(df_internal_readings_string)


    '''Read Users from S3'''


    def get_Users(k):
        # global k
        filename_Users = 'users.csv'
        k.key = 'ensek-meterpoints/Users/' + filename_Users
        k.open()
        l = k.read()
        s = l.decode('utf-8')
        p = s.splitlines()
        # print(len(p))
        return p


    '''Format Json to handle null values'''


    def format_json_response(self, data):
        data_str = json.dumps(data, indent=4).replace('null', '""')
        data_json = json.loads(data_str)
        return data_json


    def log_error(self, error_msg, error_code=''):
        logs_dir_path = sys.path[0] + '/logs/'
        if not os.path.exists(logs_dir_path):
            os.makedirs(logs_dir_path)
        with open(sys.path[0] + '/logs/' + 'internal_estimates_logs_' + time.strftime('%d%m%Y') + '.csv', mode='a') as errorlog:
            employee_writer = csv.writer(errorlog, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            employee_writer.writerow([error_msg, error_code])


    def processAccounts(self, account_ids, k, _dir_s3):

        api_url, head = util.get_ensek_api_info1('internal_estimates')
        for account_id in account_ids:
            t = con.api_config['total_no_of_calls']

            print('ac:' + str(account_id))
            msg_ac = 'ac:' + str(account_id)
            self.log_error(msg_ac, '')
            api_url1 = api_url.format(account_id)
            internal_data_response_elec, internal_data_response_gas = self.get_api_response(api_url1, head)
            # print(json.dumps(internal_data_response_elec, indent=4))

            if internal_data_response_elec:
                formatted_internal_data_elec = self.format_json_response(internal_data_response_elec)
                self.extract_internal_data_response_elec(formatted_internal_data_elec, account_id, k, _dir_s3)

            if internal_data_response_gas:
                formatted_internal_data_gas = self.format_json_response(internal_data_response_gas)
                self.extract_internal_data_response_gas(formatted_internal_data_gas, account_id, k, _dir_s3)


if __name__ == "__main__":
    freeze_support()

    dir_s3 = util.get_dir()
    bucket_name = dir_s3['s3_bucket']

    s3 = s3_con(bucket_name)

    account_ids = []
    '''Enable this to test for 1 account id'''
    if con.test_config['enable_manual'] == 'Y':
        account_ids = con.test_config['account_ids']

    if con.test_config['enable_file'] == 'Y':
        account_ids = util.get_Users_from_s3(s3)

    if con.test_config['enable_db'] == 'Y':
        account_ids = util.get_accountID_fromDB(False)

    if con.test_config['enable_db_max'] == 'Y':
        account_ids = util.get_accountID_fromDB(True)

    # Enable to test without multiprocessing.
    # p.processAccounts(account_ids, s3, dir_s3)

        ####### Multiprocessing Starts #########
    n = 5  # number of process to run in parallel
    k = int(len(account_ids) / n)  # get equal no of files for each process

    print(len(account_ids))
    print(k)

    processes = []
    lv = 0
    start = timeit.default_timer()

    for i in range(n + 1):
        p1 = InternalEstimates()
        print(i)
        uv = i * k
        if i == n:
            # print(d18_keys_s3[l:])
            t = multiprocessing.Process(target=p1.processAccounts, args=(account_ids[lv:], s3, dir_s3))
        else:
            # print(d18_keys_s3[l:u])
            t = multiprocessing.Process(target=p1.processAccounts, args=(account_ids[lv:uv], s3, dir_s3))
        lv = uv

        processes.append(t)

    for p in processes:
        p.start()
        time.sleep(2)

    for process in processes:
        process.join()
    ####### Multiprocessing Ends #########

    print("Process completed in " + str(timeit.default_timer() - start) + ' seconds')

