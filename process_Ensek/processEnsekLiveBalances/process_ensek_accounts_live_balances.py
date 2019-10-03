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
from connections.connect_db import get_boto_S3_Connections as s3_con


class LiveBalances:
    max_calls = con.api_config['max_api_calls']
    rate = con.api_config['allowed_period_in_secs']

    def __init__(self):
        pass

    @sleep_and_retry
    @limits(calls=max_calls, period=rate)
    def get_api_response(self, api_url, head):
        '''
             get the response for the respective url that is passed as part of this function
        '''
        start_time = time.time()
        timeout = con.api_config['connection_timeout']
        retry_in_secs = con.api_config['retry_in_secs']
        i = 0
        while True:
            try:
                response = requests.get(api_url, headers=head)
                if response.status_code == 200:
                    return json.loads(response.content.decode('utf-8'))
                else:
                    print('Problem Grabbing Data: ', response.status_code)
                    self.log_error('Response Error: Problem grabbing data', response.status_code)
                    break

            except ConnectionError:
                if time.time() > start_time + timeout:
                    print('Unable to Connect after {} seconds of ConnectionErrors'.format(timeout))
                    self.log_error('Unable to Connect after {} seconds of ConnectionErrors'.format(timeout))

                    break
                else:
                    print('Retrying connection in ' + str(retry_in_secs) + ' seconds' + str(i))
                    self.log_error('Retrying connection in ' + str(retry_in_secs) + ' seconds' + str(i))

                    time.sleep(retry_in_secs)
            i = i + retry_in_secs

    def extract_live_balances_json(self, data, account_id, k, dir_s3):

        df_live_balances = json_normalize(data)
        df_live_balances.columns = df_live_balances.columns.str.replace('.', '_')
        df_live_balances['account_id'] = account_id
        filename_live_balances = 'live_balances_' + str(account_id) + '.csv'
        df_live_balances_string = df_live_balances.to_csv(None, index=False)
        # print(df_live_balances_string)

        k.key = dir_s3['s3_key']['LiveBalances'] + filename_live_balances
        k.set_contents_from_string(df_live_balances_string)

    def extract_live_balances_with_details_json(self, data, account_id, k, dir_s3):
        meta_balances_with_detail = ['TotalCharges']
        df_live_balances_with_detail = json_normalize(data, record_path=['Charges'], meta=meta_balances_with_detail)
        df_live_balances_with_detail['account_id'] = account_id
        filename_live_balances_with_detail = 'live_balances_with_detail_' + str(account_id) + '.csv'
        df_live_balances_with_detail_string = df_live_balances_with_detail.to_csv(None, index=False)
        # print(df_live_balances_with_detail_string)

        k.key = dir_s3['s3_key']['LiveBalancesWithDetail'] + filename_live_balances_with_detail
        k.set_contents_from_string(df_live_balances_with_detail_string)

    def format_json_response(self, data):
        """
        This function replaces the null value with empty string as json normalize method will not accept null values in the json data
        :param data: source json data
        :return: formatted json data
        """
        data_str = json.dumps(data, indent=4).replace('null', '""')
        data_json = json.loads(data_str)
        return data_json

    def log_error(self, error_msg, error_code=''):
        logs_dir_path = sys.path[0] + '/logs/'
        if not os.path.exists(logs_dir_path):
            os.makedirs(logs_dir_path)
        with open(sys.path[0] + '/logs/' + 'live_balances_logs_' + time.strftime('%d%m%Y') + '.csv', mode='a') as errorlog:
            employee_writer = csv.writer(errorlog, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            employee_writer.writerow([error_msg, error_code])

    def processAccounts(self, account_ids, k, dir_s3):

        api_url_lb, head_lb = util.get_ensek_api_info1('live_balances')
        api_url_lbd, head_ldb = util.get_ensek_api_info1('live_balances_with_detail')

        for account_id in account_ids:
            print('ac: ' + str(account_id))

            # Get Accounts Live Balances
            api_url_lb1 = api_url_lb.format(account_id)
            api_response_lb = self.get_api_response(api_url_lb1, head_lb)

            if api_response_lb:
                formatted_reponse_lb = api_response_lb
                self.extract_live_balances_json(formatted_reponse_lb, account_id, k, dir_s3)
            else:
                print('ac:' + str(account_id) + ' has no data for Live Balances')
                msg_ac = 'ac:' + str(account_id) + ' has no data for Live Balances'
                self.log_error(msg_ac, '')

            # Get Accounts Live Balances with Detail
            api_url_lbd1 = api_url_lbd.format(account_id)
            api_response_lbd = self.get_api_response(api_url_lbd1, head_lb)

            if api_response_lbd:
                formatted_reponse_lbd = api_response_lbd
                self.extract_live_balances_with_details_json(formatted_reponse_lbd, account_id, k, dir_s3)
            else:
                print('ac:' + str(account_id) + ' has no data for Live Balances With Details')
                msg_ac = 'ac:' + str(account_id) + ' has no data for Live Balances With Details'
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
    # p = LiveBalances()
    # p.processAccounts(account_ids, s3, dir_s3)

    ####### Multiprocessing Starts #########
    env = util.get_env()
    if env == 'uat':
        n = 12  # number of process to run in parallel
    else:
        n = 12

    k = int(len(account_ids) / n)  # get equal no of files for each process


    print(len(account_ids))
    print(k)

    processes = []
    lv = 0
    start = timeit.default_timer()

    for i in range(n + 1):
        p1 = LiveBalances()
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
    ###### Multiprocessing Ends #########

    print("Process completed in " + str(timeit.default_timer() - start) + ' seconds')




