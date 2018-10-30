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
import pymysql
import multiprocessing
from multiprocessing import freeze_support


import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from conf import config as con


max_calls = con.api_config['max_api_calls']
rate = con.api_config['allowed_period_in_secs']


# k = Key()


# get account status api info
def get_tariff_history_api_info(account_id):
    # UAT
    # api_url = 'https://api.uat.igloo.ignition.ensek.co.uk/Accounts/{0}/TariffsWithHistory'.format(account_id)
    # token = 'QUtYcjkhJXkmVmVlUEJwNnAxJm1Md1kjU2RaTkRKcnZGVzROdHRiI0deS0EzYVpFS3ZYdCFQSEs0elNrMmxDdQ=='

    # prod
    # UAT
    api_url = 'https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/TariffsWithHistory'.format(account_id)
    token = 'Wk01QnVWVU01aWlLTiVeUWtwMUIyRU5EbCN0VTJUek01KmJJVFcyVGFaeiNtJkFpYUJwRUNNM2MzKjVHcjVvIQ=='
    head = {'Content-Type': 'application/json',
            'Authorization': 'Bearer {0}'.format(token)}
    return api_url, token, head

@sleep_and_retry
@limits(calls=max_calls, period=rate)
def get_api_response(api_url, token, head):
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
                print ('Problem Grabbing Data: ', response.status_code)
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


''' Extracting direct debit data'''


def extract_tariff_history_json(data, account_id, k):
    # global k,
    meta_tariff_history = ['tariffName', 'startDate', 'endDate', 'discounts', 'tariffType', 'exitFees', 'account_id']

    df_tariff_history = json_normalize(data)
    df_tariff_history['account_id'] = account_id
    df_tariff_history1 = df_tariff_history[meta_tariff_history]

    filename_tariff_history = 'df_tariff_history_' + str(account_id) + '.csv'
    df_tariff_history_string = df_tariff_history1.to_csv(None, index=False)
    k.key = 'ensek-meterpoints/TariffHistory/' + filename_tariff_history
    k.set_contents_from_string(df_tariff_history_string)

    # Elec
    if ('Electricity' in df_tariff_history.columns and not df_tariff_history['Electricity'].isnull) or not ('Electricity' in df_tariff_history.columns):
        # UnitRates
        meta_elec_unitrates = ['tariffName', 'startDate', 'endDate']
        df_elec_unit_rates = json_normalize(data, record_path=[['Electricity', 'unitRates']], meta=meta_elec_unitrates)

        if not df_elec_unit_rates.empty:
            df_elec_unit_rates['account_id'] = account_id
            filename_elec_unit_rates = 'th_elec_unitrates_' + str(account_id) + '.csv'
            df_elec_unit_rates_string = df_elec_unit_rates.to_csv(None, index=False)
            k.key = 'ensek-meterpoints/TariffHistoryElecUnitRates/' + filename_elec_unit_rates
            k.set_contents_from_string(df_elec_unit_rates_string)
            # print(df_elec_unit_rates_string)

        #  StandingCharge
        meta_elec_standing_charge = ['tariffName', 'startDate', 'endDate']
        df_elec_standing_charge = json_normalize(data, record_path=[['Electricity', 'standingChargeRates']], meta=meta_elec_standing_charge)

        if not df_elec_standing_charge.empty:
            df_elec_standing_charge['account_id'] = account_id

            filename_elec_standing_charge = 'th_elec_standingcharge_' + str(account_id) + '.csv'
            df_elec_standing_charge_string = df_elec_standing_charge.to_csv(None, index=False)
            k.key = 'ensek-meterpoints/TariffHistoryElecStandCharge/' + filename_elec_standing_charge
            k.set_contents_from_string(df_elec_standing_charge_string)
            # print(df_elec_standing_charge_string)

    # Gas
    if ('Gas' in df_tariff_history.columns and not df_tariff_history['Gas'].isnull) or not ('Gas' in df_tariff_history.columns):
        # UnitRates
        meta_gas_unitrates = ['tariffName', 'startDate', 'endDate']
        df_gas_unit_rates = json_normalize(data, record_path=[['Gas', 'unitRates']], meta=meta_gas_unitrates)

        if not df_gas_unit_rates.empty:
            df_gas_unit_rates['account_id'] = account_id

            filename_gas_unit_rates = 'th_gas_unitrates_' + str(account_id) + '.csv'
            df_gas_unit_rates_string = df_gas_unit_rates.to_csv(None, index=False)
            k.key = 'ensek-meterpoints/TariffHistoryGasUnitRates/' + filename_gas_unit_rates
            k.set_contents_from_string(df_gas_unit_rates_string)
            # print(df_gas_unit_rates_string)

        # StandingCharge
        meta_gas_standing_charge = ['tariffName', 'startDate', 'endDate']
        df_elec_standing_charge = json_normalize(data, record_path=[['Gas', 'standingChargeRates']], meta=meta_gas_standing_charge)

        if not df_elec_standing_charge.empty:
            df_elec_standing_charge['account_id'] = account_id

            filename_gas_standing_charge = 'th_gas_standingcharge_' + str(account_id) + '.csv'
            df_gas_standing_charge_string = df_elec_standing_charge.to_csv(None, index=False)
            k.key = 'ensek-meterpoints/TariffHistoryGasStandCharge/' + filename_gas_standing_charge
            k.set_contents_from_string(df_gas_standing_charge_string)
            # print(df_gas_standing_charge_string)

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
    with open(sys.path[0] + '/logs/' + 'tariff_history_logs_' + time.strftime('%d%m%Y') + '.csv', mode='a') as errorlog:
        employee_writer = csv.writer(errorlog, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        employee_writer.writerow([error_msg, error_code])


def get_accountID_fromDB():
    conn = pymysql.connect(host=con.rds_config['host'], port=con.rds_config['port'], user=con.rds_config['user'],
                           passwd=con.rds_config['pwd'], db=con.rds_config['db'])
    cur = conn.cursor()
    cur.execute(con.test_config['account_ids_sql'])

    account_ids = [row[0] for row in cur]
    cur.close()
    conn.close()

    return account_ids


def processAccounts(account_ids, k):
    for account_id in account_ids:
        t = con.api_config['total_no_of_calls']
        # Get Account Staus
        print('ac: ' + str(account_id))
        msg_ac = 'ac:' + str(account_id)
        log_error(msg_ac, '')
        api_url, token, head = get_tariff_history_api_info(account_id)
        th_response = get_api_response(api_url, token, head)
        if th_response:
            formatted_tariff_history = format_json_response(th_response)
            # print(json.dumps(formatted_tariff_history))
            extract_tariff_history_json(formatted_tariff_history, account_id, k)
        else:
            print('ac:' + str(account_id) + ' has no data')
            msg_ac = 'ac:' + str(account_id) + ' has no data'
            log_error(msg_ac, '')


if __name__ == "__main__":
    freeze_support()

    k = get_S3_Connections()

    '''Enable this to test for 1 account id'''
    if con.test_config['enable_manual'] == 'Y':
        account_ids = con.test_config['account_ids']

    if con.test_config['enable_file'] == 'Y':
        account_ids = get_Users(k)

    if con.test_config['enable_db'] == 'Y':
        account_ids = get_accountID_fromDB()

    # threads = 5
    # chunksize = 100

    # with Pool(threads) as pool:
    #     pool.starmap(processAccounts, zip(account_ids), chunksize)

    print(len(account_ids))
    print(int(len(account_ids) / 12))
    p = int(len(account_ids) / 12)

    # print(account_ids)

    start_time = datetime.datetime.now()
    p1 = multiprocessing.Process(target=processAccounts, args=(account_ids[0:p], k))
    p2 = multiprocessing.Process(target=processAccounts, args=(account_ids[p:2 * p], k))
    p3 = multiprocessing.Process(target=processAccounts, args=(account_ids[2 * p:3 * p], k))
    p4 = multiprocessing.Process(target=processAccounts, args=(account_ids[3 * p:4 * p], k))
    p5 = multiprocessing.Process(target=processAccounts, args=(account_ids[4 * p:5 * p], k))
    p6 = multiprocessing.Process(target=processAccounts, args=(account_ids[5 * p:6 * p], k))
    p7 = multiprocessing.Process(target=processAccounts, args=(account_ids[6 * p:7 * p], k))
    p8 = multiprocessing.Process(target=processAccounts, args=(account_ids[7 * p:8 * p], k))
    p9 = multiprocessing.Process(target=processAccounts, args=(account_ids[8 * p:9 * p], k))
    p10 = multiprocessing.Process(target=processAccounts, args=(account_ids[9 * p:10 * p], k))
    p11 = multiprocessing.Process(target=processAccounts, args=(account_ids[10 * p:11 * p], k))
    p12 = multiprocessing.Process(target=processAccounts, args=(account_ids[11 * p:], k))
    end_time = datetime.datetime.now()

    diff = end_time - start_time
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

    print("Process completed. Time taken: " + str(diff))