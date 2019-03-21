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

from common import utils as util
from conf import config as con
from connections import connect_db as db
from connections.connect_db import get_boto_S3_Connections as s3_con


class StatusRegistrations:
    max_calls = con.api_config['max_api_calls']
    rate = con.api_config['allowed_period_in_secs']

    def __init__(self):
        self.sql = "select account_id, meterpoint_id, meterpointnumber from ref_meterpoints"

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




    def extract_reg_elec_json(self, data,account_id,meterpointnumber,k, dir_s3):
        # global k

        elec_dict = dict(Account_id = account_id,  meterpointnumber=meterpointnumber,Status = data)
        elec_str = json.dumps(elec_dict)
        elec_json = json.loads(elec_str)
        df_elec = json_normalize(elec_json)
        filename_elec = 'reg_elec_meterpointnumber_' + str(account_id) + '_'+ str(meterpointnumber) + '.csv'
        df_elec_string = df_elec.to_csv(None, index=False)


        k.key = dir_s3['s3_key']['RegistrationsElecMeterpoints'] + filename_elec
        k.set_contents_from_string(df_elec_string)

    def extract_reg_gas_json(self, data,account_id,meterpointnumber,k, dir_s3):
        # global k

        gas_dict = dict(Account_id = account_id, meterpointnumber=meterpointnumber, Status = data)
        gas_str = json.dumps(gas_dict)
        gas_json = json.loads(gas_str)
        df_gas = json_normalize(gas_json)
        filename_gas = 'reg_gas_meterpointnumber_' + str(account_id) + '_' + str(meterpointnumber) + '.csv'
        df_gas_string = df_gas.to_csv(None, index=False)

        k.key = dir_s3['s3_key']['RegistrationsGasMeterpoints'] + filename_gas
        k.set_contents_from_string(df_gas_string)


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
        data_str = json.dumps(data, indent=4).replace('null','""')
        data_json = json.loads(data_str)
        return data_json


    def log_error(self, error_msg, error_code=''):
        logs_dir_path = sys.path[0] + '/logs/'
        if not os.path.exists(logs_dir_path):
            os.makedirs(logs_dir_path)
        with open(sys.path[0] + '/logs/' + 'status_logs_' + time.strftime('%d%m%Y') + '.csv' , mode='a') as errorlog:
            employee_writer = csv.writer(errorlog, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            employee_writer.writerow([error_msg, error_code])


    def processAccounts(self, account_ids,k, dir_s3):

        api_url_ac, head_ac = util.get_ensek_api_info1('account_status')
        api_url_elec, head_elec = util.get_ensek_api_info1('elec_status')
        api_url_gas, head_gas = util.get_ensek_api_info1('gas_status')

        for account_id in account_ids:
            t = con.api_config['total_no_of_calls']


            # Get Elec details
            api_url_elec1 = api_url_elec.format(account_id)
            account_elec_response = self.get_api_response(api_url_elec1, head_elec)
            # print(account_elec_response)

            if account_elec_response:
                formated_elec = account_elec_response
                self.extract_reg_elec_json(formated_elec,account_id, k, dir_s3)
            else:
                print('ac:' + str(account_id) + ' has no data for Elec status')
                msg_ac = 'ac:' + str(account_id) + ' has no data for Elec status'
                self.log_error(msg_ac, '')

            # Get Gas details
            api_url_gas1 = api_url_gas.format(account_id)
            account_gas_response = self.get_api_response(api_url_gas1, head_gas)
            # print(account_gas_response)
            if account_gas_response:
                formated_gas = account_gas_response
                self.extract_reg_gas_json(formated_gas,account_id,k, dir_s3)
            else:
                print('ac:' + str(account_id) + ' has no data Gas status')
                msg_ac = 'ac:' + str(account_id) + ' has no data for Gas status'
                self.log_error(msg_ac, '')



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
    env = util.get_env()
    if env == 'uat':
        n = 12  # number of process to run in parallel
    else:
        n = 24

    k = int(len(account_ids) / n)  # get equal no of files for each process

    print(len(account_ids))
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



