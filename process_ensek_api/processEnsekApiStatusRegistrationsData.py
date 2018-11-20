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



# get account status api info
def get_account_status_api_info(account_id):
    #UAT
    # api_url = 'https://api.uat.igloo.ignition.ensek.co.uk/Accounts/{0}/MeterPoints'.format(account_id)
    # token = 'QUtYcjkhJXkmVmVlUEJwNnAxJm1Md1kjU2RaTkRKcnZGVzROdHRiI0deS0EzYVpFS3ZYdCFQSEs0elNrMmxDdQ=='

    #prod  
    api_url = 'https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/SupplyStatus'.format(account_id)
    token = 'Wk01QnVWVU01aWlLTiVeUWtwMUIyRU5EbCN0VTJUek01KmJJVFcyVGFaeiNtJkFpYUJwRUNNM2MzKjVHcjVvIQ=='
    head = {'Content-Type': 'application/json',
           'Authorization': 'Bearer {0}'.format(token)}
    return api_url,token,head

def get_reg_elec_api_info(account_id):
    #UAT
    # api_url = 'https://api.uat.igloo.ignition.ensek.co.uk/Accounts/{0}/MeterPoints'.format(account_id)
    # token = 'QUtYcjkhJXkmVmVlUEJwNnAxJm1Md1kjU2RaTkRKcnZGVzROdHRiI0deS0EzYVpFS3ZYdCFQSEs0elNrMmxDdQ=='

    #prod  
    api_url = 'https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/Processes/Registrations/Elec'.format(account_id)
    token = 'Wk01QnVWVU01aWlLTiVeUWtwMUIyRU5EbCN0VTJUek01KmJJVFcyVGFaeiNtJkFpYUJwRUNNM2MzKjVHcjVvIQ=='
    head = {'Content-Type': 'application/json',
           'Authorization': 'Bearer {0}'.format(token)}
    return api_url,token,head

def get_reg_gas_api_info(account_id):
    #UAT
    # api_url = 'https://api.uat.igloo.ignition.ensek.co.uk/Accounts/{0}/MeterPoints'.format(account_id)
    # token = 'QUtYcjkhJXkmVmVlUEJwNnAxJm1Md1kjU2RaTkRKcnZGVzROdHRiI0deS0EzYVpFS3ZYdCFQSEs0elNrMmxDdQ=='

    #prod  
    api_url = 'https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/Processes/Registrations/Gas'.format(account_id)
    token = 'Wk01QnVWVU01aWlLTiVeUWtwMUIyRU5EbCN0VTJUek01KmJJVFcyVGFaeiNtJkFpYUJwRUNNM2MzKjVHcjVvIQ=='
    head = {'Content-Type': 'application/json',
           'Authorization': 'Bearer {0}'.format(token)}
    return api_url,token,head

@sleep_and_retry
@limits(calls=max_calls, period=rate)
def get_api_response(api_url,token,head):
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
                log_error('Response Error: Problem grabbing data', response.status_code)
                break

        except ConnectionError:
            if time.time() > start_time + timeout:
                print('Unable to Connect after {} seconds of ConnectionErrors'.format(timeout))
                log_error('Unable to Connect after {} seconds of ConnectionErrors'.format(timeout))

                break
            else:
                print('Retrying connection in ' + str(retry_in_secs) +  ' seconds' + str(i))
                log_error('Retrying connection in ' + str(retry_in_secs) +  ' seconds' + str(i))

                time.sleep(retry_in_secs)
        i=i+retry_in_secs

   


''' Processing meter readings data billeable'''
def extract_account_status_json(data,account_id,k):
    # global k
   
    status_dict = dict(Account_id = account_id, Status = data)
    status_str = json.dumps(status_dict) 
    status_json = json.loads(status_str) 
    df_account_status = json_normalize(status_json)
    filename_account_status = 'account_status_' + str(account_id) + '.csv'
    df_account_status_string = df_account_status.to_csv(None, index=False)
    
    k.key = 'ensek-meterpoints/AccountStatus/' + filename_account_status
    k.set_contents_from_string(df_account_status_string)
    # print(df_meter_readings_string)
    # print(filename_readings)

def extract_reg_elec_json(data,account_id,k):
    # global k
   
    elec_dict = dict(Account_id = account_id, Status = data)
    elec_str = json.dumps(elec_dict) 
    elec_json = json.loads(elec_str) 
    df_elec = json_normalize(elec_json)
    filename_elec = 'reg_elec_' + str(account_id) + '.csv'
    df_elec_string = df_elec.to_csv(None, index=False)
    
    k.key = 'ensek-meterpoints/RegistrationsElec/' + filename_elec
    k.set_contents_from_string(df_elec_string)
    # print(df_meter_readings_string)
    # print(filename_readings)

def extract_reg_gas_json(data,account_id,k):
    # global k
   
    gas_dict = dict(Account_id = account_id, Status = data)
    gas_str = json.dumps(gas_dict) 
    gas_json = json.loads(gas_str) 
    df_gas = json_normalize(gas_json)
    filename_gas = 'reg_gas_' + str(account_id) + '.csv'
    df_gas_string = df_gas.to_csv(None, index=False)
    
    k.key = 'ensek-meterpoints/RegistrationsGas/' + filename_gas
    k.set_contents_from_string(df_gas_string)
    # print(df_meter_readings_string)
    # print(filename_readings)

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
    data_str = json.dumps(data, indent=4).replace('null','""')
    data_json = json.loads(data_str)
    return data_json

def log_error(error_msg, error_code=''):
    logs_dir_path = sys.path[0] + '/logs/'
    if not os.path.exists(logs_dir_path):
        os.makedirs(logs_dir_path)
    with open(sys.path[0] + '/logs/' + 'status_logs_' + time.strftime('%d%m%Y') + '.csv' , mode='a') as errorlog:
        employee_writer = csv.writer(errorlog, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        employee_writer.writerow([error_msg, error_code])


def processAccounts(account_ids,k):
    for account_id in account_ids:
        t = con.api_config['total_no_of_calls']
        # Get Account Staus
        print('ac: '+ str(account_id))
        api_url,token,head = get_account_status_api_info(account_id)
        account_status_response = get_api_response(api_url,token,head)
        if(account_status_response):
            formated_account_status = account_status_response
            extract_account_status_json(formated_account_status,account_id,k)
        else:
            print('ac:' + str(account_id) + ' has no data for account status')
            msg_ac = 'ac:' + str(account_id) + ' has no data for account status'
            log_error(msg_ac, '')
        
        # Get Elec details
        api_url,token,head = get_reg_elec_api_info(account_id)
        account_elec_response = get_api_response(api_url,token,head)
        # print(account_elec_response)

        if(account_elec_response):
            formated_elec = account_elec_response
            extract_reg_elec_json(formated_elec,account_id,k)
        else:
            print('ac:' + str(account_id) + ' has no data for Elec status')
            msg_ac = 'ac:' + str(account_id) + ' has no data for Elec status'
            log_error(msg_ac, '')

        # Get Gas details
        api_url,token,head = get_reg_gas_api_info(account_id)
        account_gas_response = get_api_response(api_url,token,head)
        # print(account_gas_response)
        if(account_gas_response):
            formated_gas = account_gas_response
            extract_reg_gas_json(formated_gas,account_id,k)
        else:
            print('ac:' + str(account_id) + ' has no data Gas status')
            msg_ac = 'ac:' + str(account_id) + ' has no data for Gas status'
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
        account_ids = g.get_accountID_fromDB(False)

    if con.test_config['enable_db_max'] == 'Y':
        account_ids = g.get_accountID_fromDB(True)

    # threads = 5
    # chunksize = 100

    # with Pool(threads) as pool:
    #     pool.starmap(processAccounts, zip(account_ids), chunksize)

    print(len(account_ids))
    print(int(len(account_ids)/12))
    p = int(len(account_ids)/12)

    # print(account_ids)

    p1 = multiprocessing.Process(target = processAccounts, args=(account_ids[0:p],k))
    p2 = multiprocessing.Process(target = processAccounts, args=(account_ids[p:2*p],k))
    p3 = multiprocessing.Process(target = processAccounts, args=(account_ids[2*p:3*p],k))
    p4 = multiprocessing.Process(target = processAccounts, args=(account_ids[3*p:4*p],k))
    p5 = multiprocessing.Process(target = processAccounts, args=(account_ids[4*p:5*p],k))
    p6 = multiprocessing.Process(target = processAccounts, args=(account_ids[5*p:6*p],k))
    p7 = multiprocessing.Process(target = processAccounts, args=(account_ids[6*p:7*p],k))
    p8 = multiprocessing.Process(target = processAccounts, args=(account_ids[7*p:8*p],k))
    p9 = multiprocessing.Process(target = processAccounts, args=(account_ids[8*p:9*p],k))
    p10 = multiprocessing.Process(target = processAccounts, args=(account_ids[9*p:10*p],k))
    p11 = multiprocessing.Process(target = processAccounts, args=(account_ids[10*p:11*p],k))
    p12 = multiprocessing.Process(target = processAccounts, args=(account_ids[11*p:],k))
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


