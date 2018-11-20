import requests
import json
from pandas.io.json import json_normalize
from ratelimit import limits, sleep_and_retry
import boto
from boto.s3.key import Key
import time
import datetime
from requests import ConnectionError
import csv
import multiprocessing
from multiprocessing import freeze_support
from process_ensek_api import get_account_ids as g


import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from conf import config as con


max_calls = con.api_config['max_api_calls']
rate = con.api_config['allowed_period_in_secs']


# k = Key()

# get meter point api info
def get_readings_internal_api_info(account_id, token):
    # UAT
    # api_url = 'https://api.uat.igloo.ignition.ensek.co.uk/Accounts/{0}/MeterPoints'.format(account_id)
    # token = 'QUtYcjkhJXkmVmVlUEJwNnAxJm1Md1kjU2RaTkRKcnZGVzROdHRiI0deS0EzYVpFS3ZYdCFQSEs0elNrMmxDdQ=='

    # prod
    api_url = 'https://igloo.ignition.ensek.co.uk/api/account/{0}/meter-readings?sortField=meterReadingDateTime&sortDirection=Descending'.format(
        account_id)
    head = {'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json',
            'Referrer': 'https://igloo.ignition.ensek.co.uk',
            'Authorization': 'Bearer {0}'.format(token)}
    return api_url, head


def get_auth_code():
    oauth_url = 'https://igloo.ignition.ensek.co.uk/api/Token'
    data = dict(
        username=con.internalapi_config['username'],
        password=con.internalapi_config['password'],
        grant_type=con.internalapi_config['grant_type']
    )

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'application/json',
        'Referrer': 'https://igloo.ignition.ensek.co.uk'
    }
    response = requests.post(oauth_url, data=data, headers=headers)
    response = response.json()
    return response.get('access_token')


@sleep_and_retry
@limits(calls=max_calls, period=rate)
def get_api_response(api_url, head):
    '''
        get the response for the respective url that is passed as part of this function
   '''
    session = requests.Session()
    start_time = time.time()
    timeout = con.api_config['connection_timeout']
    retry_in_secs = con.api_config['retry_in_secs']
    i = 0
    while True:
        try:
            response = session.post(api_url, headers=head, params={'page': 1})

            if response.status_code == 200:
                response_json = json.loads(response.content.decode('utf-8'))
                total_pages = response_json['totalPages']
                response_items = response_json['items']

                for page in range(2, total_pages + 1):
                    response_next_page = session.post(api_url, headers=head, params={'page': page})
                    response_next_page_json = json.loads(response_next_page.content.decode('utf-8'))['items']
                    response_items.extend(response_next_page_json)
                return response_items
            else:
                print('Problem Grabbing Data: ', response.status_code)
                log_error('Response Error: Problem grabbing data', response.status_code)
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


def extract_internal_data_response(data, account_id, k):
    ''' Processing meter points data'''
    df_internal_readings = json_normalize(data)
    # print(df_internal_readings)
    if (df_internal_readings.empty):
        print(" - has no readings data")
    else:
        df_internal_readings_string = df_internal_readings.to_csv(None, index=False)
        # print(df_internal_readings_string)
        file_name_internal_readings = 'internal_readings_' + str(account_id) + '.csv'
        k.key = 'ensek-meterpoints/ReadingsInternal/' + file_name_internal_readings
        k.set_contents_from_string(df_internal_readings_string)


'''Get S3 connection'''


def get_S3_Connections():
    # global k
    access_key = con.s3_config['access_key']
    secret_key = con.s3_config['secret_key']
    # print(access_key)
    # print(secret_key)

    s3 = boto.connect_s3(aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    bucket = s3.get_bucket('igloo-uat-bucket')
    k = Key(bucket)
    return k


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
    with open(sys.path[0] + '/logs/' + 'internal_readings_logs_' + time.strftime('%d%m%Y') + '.csv', mode='a') as errorlog:
        employee_writer = csv.writer(errorlog, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        employee_writer.writerow([error_msg, error_code])


def processAccounts(account_ids, k, token):
    for account_id in account_ids:
        t = con.api_config['total_no_of_calls']
        # run for configured account ids
        api_url, head = get_readings_internal_api_info(account_id, token)
        # print('ac:' + str(account_id) + str(multiprocessing.current_process()))
        print('ac:' + str(account_id))
        msg_ac = 'ac:' + str(account_id)
        log_error(msg_ac, '')

        internal_data_response = get_api_response(api_url, head)
        # print(json.dumps(internal_data_response, indent=4))

        formatted_internal_data = format_json_response(internal_data_response)
        extract_internal_data_response(formatted_internal_data, account_id, k)


if __name__ == "__main__":
    freeze_support()

    k = get_S3_Connections()

    t = get_auth_code()

    '''Enable this to test for 1 account id'''
    if con.test_config['enable_manual'] == 'Y':
        account_ids = con.test_config['account_ids']

    if con.test_config['enable_file'] == 'Y':
        account_ids = get_Users(k)

    if con.test_config['enable_db'] == 'Y':
        account_ids = g.get_accountID_fromDB(False)

    if con.test_config['enable_db_max'] == 'Y':
        account_ids = g.get_accountID_fromDB(True)

    print(len(account_ids))
    print(int(len(account_ids) / 12))
    p = int(len(account_ids) / 12)

    # print(account_ids)

    p1 = multiprocessing.Process(target=processAccounts, args=(account_ids[0:p], k, t))
    p2 = multiprocessing.Process(target=processAccounts, args=(account_ids[p:2 * p], k, t))
    p3 = multiprocessing.Process(target=processAccounts, args=(account_ids[2 * p:3 * p], k, t))
    p4 = multiprocessing.Process(target=processAccounts, args=(account_ids[3 * p:4 * p], k, t))
    p5 = multiprocessing.Process(target=processAccounts, args=(account_ids[4 * p:5 * p], k, t))
    p6 = multiprocessing.Process(target=processAccounts, args=(account_ids[5 * p:6 * p], k, t))
    p7 = multiprocessing.Process(target=processAccounts, args=(account_ids[6 * p:7 * p], k, t))
    p8 = multiprocessing.Process(target=processAccounts, args=(account_ids[7 * p:8 * p], k, t))
    p9 = multiprocessing.Process(target=processAccounts, args=(account_ids[8 * p:9 * p], k, t))
    p10 = multiprocessing.Process(target=processAccounts, args=(account_ids[9 * p:10 * p], k, t))
    p11 = multiprocessing.Process(target=processAccounts, args=(account_ids[10 * p:11 * p], k, t))
    p12 = multiprocessing.Process(target=processAccounts, args=(account_ids[11 * p:], k, t))

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

