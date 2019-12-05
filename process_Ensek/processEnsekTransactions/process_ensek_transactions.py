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


class AccountTransactions:
    max_calls = con.api_config['max_api_calls']
    rate = con.api_config['allowed_period_in_secs']

    def __init__(self, process_num):
        logs_dir_path = sys.path[0] + '/logs/'
        if not os.path.exists(logs_dir_path):
            os.makedirs(logs_dir_path)
        self.pnum = process_num
        self.generic_log_file = sys.path[0] + '/logs/' + 'account_transactions_generic_logs_' + time.strftime(
            '%Y%m%d') + '.csv'

    @sleep_and_retry
    @limits(calls=max_calls, period=rate)
    def get_api_response(self, api_url, head, account_id):
        '''
             get the response for the respective url that is passed as part of this function
        '''
        start_time = time.time()
        timeout = con.api_config['connection_timeout']
        retry_in_secs = con.api_config['retry_in_secs']
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
                    self.log_msg('{0}, {1}, {2}, {3}, {4}'.format(account_id, 'success', total_api_time, attempt_num,
                                                                  method_time),
                                 msg_type='timing')
                    return json.loads(response.content.decode('utf-8'))
                else:
                    print('Problem Grabbing Data: ', response.status_code)
                    exit_type = 'format_error'
                    self.log_error('Response Error: Problem grabbing data', response.status_code)
                    break

            except ConnectionError:
                total_api_time += time.time() - api_call_start
                if time.time() > start_time + timeout:
                    print('Unable to Connect after {} seconds of ConnectionErrors'.format(timeout))
                    self.log_error('Unable to Connect after {} seconds of ConnectionErrors'.format(timeout))
                    exit_type = 'connection_error'
                    break
                else:
                    print('Retrying connection in ' + str(retry_in_secs) + ' seconds' + str(i))
                    self.log_error('Retrying connection in ' + str(retry_in_secs) + ' seconds' + str(i))

                    time.sleep(retry_in_secs)
            i = i + retry_in_secs
        method_time = time.time() - start_time
        self.log_msg('{0}, {1}, {2}, {3}, {4}'.format(account_id, exit_type, total_api_time, attempt_num, method_time),
                     msg_type='timing')

    def extract_account_transactions_json(self, data, account_id, k, dir_s3):
        df_account_transactions = json_normalize(data)
        df_account_transactions.columns = df_account_transactions.columns.str.replace('.', '_')
        df_account_transactions['account_id'] = account_id
        filename_account_transactions = 'account_transactions_' + str(account_id) + '.csv'
        df_account_transactions_string = df_account_transactions.to_csv(None, index=False)
        # print(df_account_transactions_string)

        k.key = dir_s3['s3_key']['AccountTransactions'] + filename_account_transactions
        k.set_contents_from_string(df_account_transactions_string)

    def format_json_response(self, data):
        """
        This function replaces the null value with empty string as json normalize method will not accept null values in the json data
        :param data: source json data
        :return: formatted json data
        """
        data_str = json.dumps(data, indent=4).replace('null', '""')
        data_json = json.loads(data_str)
        return data_json

    def log_msg(self, msg, msg_type='diagnostic'):
        with open(self.generic_log_file, mode='a+') as log_msg_file:
            log_msg_file.write(
                '{0}, {1}, {2}, {3}\n'.format(self.pnum, datetime.datetime.now().strftime('%H:%M:%S'), msg, msg_type))

    def log_error(self, error_msg, error_code=''):
        logs_dir_path = sys.path[0] + '/logs/'
        if not os.path.exists(logs_dir_path):
            os.makedirs(logs_dir_path)
        with open(sys.path[0] + '/logs/' + 'account_transactions_logs_' + time.strftime('%d%m%Y') + '.csv',
                  mode='a') as errorlog:
            employee_writer = csv.writer(errorlog, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            employee_writer.writerow([error_msg, error_code])

    def processAccounts(self, account_ids, k, dir_s3):
        api_url_at, head_lb = util.get_ensek_api_info1('account_transactions')

        for account_id in account_ids:
            print('ac: ' + str(account_id))

            # Get Accounts Transactions
            api_url_at1 = api_url_at.format(account_id)
            api_response_at = self.get_api_response(api_url_at1, head_lb, account_id)

            if api_response_at:
                formatted_reponse_at = self.format_json_response(api_response_at)
                self.extract_account_transactions_json(formatted_reponse_at, account_id, k, dir_s3)
            else:
                print('ac:' + str(account_id) + ' has no data for Account Transactions')
                msg_ac = 'ac:' + str(account_id) + ' has no data for Account Transactions'
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

    account_ids = account_ids[:10]

    # Enable to test without multiprocessing.
    # p = AccountTransactions()
    # p.processAccounts(account_ids, s3, dir_s3)

    ####### Multiprocessing Starts #########
    env = util.get_env()
    total_processes = 2  # util.get_multiprocess('total_ensek_processes')

    if env == 'uat':
        n = total_processes  # number of process to run in parallel
    else:
        n = total_processes

    k = int(len(account_ids) / n)  # get equal no of files for each process

    print(len(account_ids))
    print(k)

    processes = []
    lv = 0
    start = timeit.default_timer()

    for i in range(n + 1):
        p1 = AccountTransactions(i)
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
