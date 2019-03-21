import timeit

import requests
import json
import pandas as pd
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


sys.path.append('../..')

from common import utils as util
from conf import config as con
from connections import connect_db as db
from connections.connect_db import get_boto_S3_Connections as s3_con


class StatusRegistrations:
    max_calls = con.api_config['max_api_calls']
    rate = con.api_config['allowed_period_in_secs']

    def __init__(self):
        self.sql = "select account_id, meter_point_id, meterpointnumber, meterpointtype from ref_meterpoints"

    @sleep_and_retry
    @limits(calls=max_calls, period=rate)
    def get_api_response(self, api_url, head):
       '''
            get the response for the respective url that is passed as part of this function
       '''
       start_time = time.time()
       timeout = con.api_config['connection_timeout']
       retry_in_secs = con.api_config['retry_in_secs']
       i=0
       while True:
            try:
                response = requests.get(api_url, headers=head)
                if response.status_code == 200:
                    return json.loads(response.content.decode('utf-8'))
                else:
                    print ('Problem Grabbing Data: ', response.status_code)
                    self.log_error('Response Error: Problem grabbing data', response.status_code)
                    break

            except ConnectionError:
                if time.time() > start_time + timeout:
                    print('Unable to Connect after {} seconds of ConnectionErrors'.format(timeout))
                    self.log_error('Unable to Connect after {} seconds of ConnectionErrors'.format(timeout))

                    break
                else:
                    print('Retrying connection in ' + str(retry_in_secs) +  ' seconds' + str(i))
                    self.log_error('Retrying connection in ' + str(retry_in_secs) +  ' seconds' + str(i))

                    time.sleep(retry_in_secs)
            i=i+retry_in_secs

    def extract_reg_elec_json(self, data, _df, k, _dir_s3):

        elec_dict = dict(account_id=str(_df['account_id']).strip(), meter_point_id=str(_df['meter_point_id']).strip(), meterpointnumber=str(_df['meterpointnumber']).strip(), status=data)
        elec_str = json.dumps(elec_dict)
        elec_json = json.loads(elec_str)
        df_elec = json_normalize(elec_json)
        filename_elec = 'reg_elec_mp_status_' + str(_df['account_id']) + '_' + str(_df['meterpointnumber']) + '.csv'
        df_elec_string = df_elec.to_csv(None, index=False)
        print(df_elec_string)

        k.key = _dir_s3['s3_key']['RegistrationsElecMeterpoint'] + filename_elec
        k.set_contents_from_string(df_elec_string)

    def extract_reg_gas_json(self, data, _df, k, _dir_s3):

        gas_dict = dict(account_id=str(_df['account_id']).strip(), meter_point_id=str(_df['meter_point_id']).strip(), meterpointnumber=str(_df['meterpointnumber']).strip(), status=data)
        gas_str = json.dumps(gas_dict)
        gas_json = json.loads(gas_str)
        df_gas = json_normalize(gas_json)
        filename_gas = 'reg_gas_mp_status_' + str(_df['account_id']) + '_' + str(_df['meterpointnumber']) + '.csv'
        df_gas_string = df_gas.to_csv(None, index=False)
        print(df_gas_string)

        k.key = _dir_s3['s3_key']['RegistrationsGasMeterpoint'] + filename_gas
        k.set_contents_from_string(df_gas_string)

    '''Format Json to handle null values'''
    def format_json_response(self, data):
        data_str = json.dumps(data, indent=4).replace('null', '""')
        data_json = json.loads(data_str)
        return data_json

    def log_error(self, error_msg, error_code=''):
        logs_dir_path = sys.path[0] + '/logs/'
        if not os.path.exists(logs_dir_path):
            os.makedirs(logs_dir_path)
        with open(sys.path[0] + '/logs/' + 'status_logs_' + time.strftime('%d%m%Y') + '.csv' , mode='a') as errorlog:
            employee_writer = csv.writer(errorlog, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            employee_writer.writerow([error_msg, error_code])

    def processAccounts(self, _df, k, _dir_s3):
        api_url_elec, head_elec = util.get_ensek_api_info1('elec_mp_status')
        api_url_gas, head_gas = util.get_ensek_api_info1('gas_mp_status')

        for index, df in _df.iterrows():
            # Get Elec details
            if df['meterpointtype'] == 'E':
                formatted_url_elec = api_url_elec.format(df['account_id'], df['meterpointnumber'])
                response_elec = self.get_api_response(formatted_url_elec, head_elec)
                # print(account_elec_response)

                if response_elec:
                    formated_response_elec = self.format_json_response(response_elec)
                    self.extract_reg_elec_json(formated_response_elec, df, k, _dir_s3)
                else:
                    print('ac:' + str(df['account_id']) + ' has no data for Elec status')
                    msg_ac = 'ac:' + str(df['account_id']) + ' has no data for Elec status'
                    self.log_error(msg_ac, '')

            # Get Gas details
            if df['meterpointtype'] == 'G':
                api_url_gas1 = api_url_gas.format(df['account_id'], df['meterpointnumber'])
                response_gas = self.get_api_response(api_url_gas1, head_gas)
                # print(account_gas_response)

                if response_gas:
                    formated_response_gas = self.format_json_response(response_gas)
                    self.extract_reg_gas_json(formated_response_gas, df, k, _dir_s3)
                else:
                    print('ac:' + str(df['account_id']) + ' has no data Gas status')
                    msg_ac = 'ac:' + str(df['account_id']) + ' has no data for Gas status'
                    self.log_error(msg_ac, '')


if __name__ == "__main__":
    freeze_support()

    dir_s3 = util.get_dir()
    bucket_name = dir_s3['s3_bucket']

    s3 = s3_con(bucket_name)

    # Enable to test without multiprocessing.
    p = StatusRegistrations()

    df = util.execute_query(p.sql)
    # p.processAccounts(df, s3, dir_s3)

    ##### Multiprocessing Starts #########
    env = util.get_env()
    if env == 'uat':
        n = 12  # number of process to run in parallel
    else:
        n = 24

    k = int(len(df) / n)  # get equal no of files for each process

    print(len(df))
    print(k)

    processes = []
    lv = 0
    start = timeit.default_timer()

    for i in range(n + 1):
        p1 = StatusRegistrations()
        print(i)
        uv = i * k
        if i == n:
            # print(d18_keys_s3[l:])
            t = multiprocessing.Process(target=p1.processAccounts, args=(df[lv:], s3, dir_s3))
        else:
            # print(d18_keys_s3[l:u])
            t = multiprocessing.Process(target=p1.processAccounts, args=(df[lv:uv], s3, dir_s3))
        lv = uv

        processes.append(t)

    for p in processes:
        p.start()
        time.sleep(2)

    for process in processes:
        process.join()
    ####### Multiprocessing Ends #########

    print("Process completed in " + str(timeit.default_timer() - start) + ' seconds')



