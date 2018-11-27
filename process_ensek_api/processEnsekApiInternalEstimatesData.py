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


max_calls = con.api_config['max_api_calls']
rate = con.api_config['allowed_period_in_secs']


@sleep_and_retry
@limits(calls=max_calls, period=rate)
def get_api_response(api_url, head):
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


def extract_internal_data_response_elec(data, account_id, k, dir_s3):
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


def extract_internal_data_response_gas(data, account_id, k, dir_s3):
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


def format_json_response(data):
    data_str = json.dumps(data, indent=4).replace('null', '""')
    data_json = json.loads(data_str)
    return data_json


def log_error(error_msg, error_code=''):
    logs_dir_path = sys.path[0] + '/logs/'
    if not os.path.exists(logs_dir_path):
        os.makedirs(logs_dir_path)
    with open(sys.path[0] + '/logs/' + 'internal_estimates_logs_' + time.strftime('%d%m%Y') + '.csv', mode='a') as errorlog:
        employee_writer = csv.writer(errorlog, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        employee_writer.writerow([error_msg, error_code])


def processAccounts(account_ids, k, _dir_s3):

    api_url, head = util.get_ensek_api_info1('internal_estimates')
    for account_id in account_ids:
        t = con.api_config['total_no_of_calls']
        # run for configured account ids
        # api_url, head = get_estimates_internal_api_info(account_id, token)
        # print('ac:' + str(account_id) + str(multiprocessing.current_process()))
        print('ac:' + str(account_id))
        msg_ac = 'ac:' + str(account_id)
        log_error(msg_ac, '')
        api_url1 = api_url.format(account_id)
        internal_data_response_elec, internal_data_response_gas = get_api_response(api_url1, head)
        # print(json.dumps(internal_data_response_elec, indent=4))

        if internal_data_response_elec:
            formatted_internal_data_elec = format_json_response(internal_data_response_elec)
            extract_internal_data_response_elec(formatted_internal_data_elec, account_id, k, _dir_s3)

        if internal_data_response_gas:
            formatted_internal_data_gas = format_json_response(internal_data_response_gas)
            extract_internal_data_response_gas(formatted_internal_data_gas, account_id, k, _dir_s3)


if __name__ == "__main__":
    freeze_support()

    dir_s3 = util.get_environment()
    bucket_name = dir_s3['s3_bucket']

    k = s3_con(bucket_name)

    '''Enable this to test for 1 account id'''
    if con.test_config['enable_manual'] == 'Y':
        account_ids = con.test_config['account_ids']

    if con.test_config['enable_file'] == 'Y':
        account_ids = get_Users(k)

    if con.test_config['enable_db'] == 'Y':
        account_ids = util.get_accountID_fromDB(False)

    if con.test_config['enable_db_max'] == 'Y':
        account_ids = util.get_accountID_fromDB(True)


    # threads = 5
    # chunksize = 100

    # with Pool(threads) as pool:
    #     pool.starmap(processAccounts, zip(account_ids), chunksize)

    print(len(account_ids))
    print(int(len(account_ids) / 12))
    p = int(len(account_ids) / 12)

    # print(account_ids)

    # processAccounts(account_ids, k, dir_s3)
    ######### Multiprocessing starts  ##########
    p1 = multiprocessing.Process(target=processAccounts, args=(account_ids[0:p], k, dir_s3))
    p2 = multiprocessing.Process(target=processAccounts, args=(account_ids[p:2 * p], k, dir_s3))
    p3 = multiprocessing.Process(target=processAccounts, args=(account_ids[2 * p:3 * p], k, dir_s3))
    p4 = multiprocessing.Process(target=processAccounts, args=(account_ids[3 * p:4 * p], k, dir_s3))
    p5 = multiprocessing.Process(target=processAccounts, args=(account_ids[4 * p:5 * p], k, dir_s3))
    p6 = multiprocessing.Process(target=processAccounts, args=(account_ids[5 * p:6 * p], k, dir_s3))
    p7 = multiprocessing.Process(target=processAccounts, args=(account_ids[6 * p:7 * p], k, dir_s3))
    p8 = multiprocessing.Process(target=processAccounts, args=(account_ids[7 * p:8 * p], k, dir_s3))
    p9 = multiprocessing.Process(target=processAccounts, args=(account_ids[8 * p:9 * p], k, dir_s3))
    p10 = multiprocessing.Process(target=processAccounts, args=(account_ids[9 * p:10 * p], k, dir_s3))
    p11 = multiprocessing.Process(target=processAccounts, args=(account_ids[10 * p:11 * p], k, dir_s3))
    p12 = multiprocessing.Process(target=processAccounts, args=(account_ids[11 * p:], k, dir_s3))
    end_time = datetime.datetime.now()

    p1.start()
    p2.start()
    p3.start()
    p4.start()
    p5.start()
    p6.start()
    p7.start()
    p8.start()
    p9.start()
    p10.start()
    p11.start()
    p12.start()

    p1.join()
    p2.join()
    p3.join()
    p4.join()
    p5.join()
    p6.join()
    p7.join()
    p8.join()
    p9.join()
    p10.join()
    p11.join()
    p12.join()
    ####### Multiprocessing Ends ########


