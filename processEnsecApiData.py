import requests
import json
import pandas as pd
from pandas.io.json import json_normalize 
from ratelimit import limits, sleep_and_retry
import ig_config as con
import boto
from io import BytesIO
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from retrying import retry
import time
import datetime
from requests import ConnectionError
import csv
import pymysql
import multiprocessing
from multiprocessing import Pool, freeze_support, Process
from time import sleep
from itertools import repeat
from functools import partial


max_calls = con.api_config['max_api_calls']
rate = con.api_config['allowed_period_in_secs']
# k = Key()

# get meter point api info
def get_meter_point_api_info(account_id):
    #UAT
    # api_url = 'https://api.uat.igloo.ignition.ensek.co.uk/Accounts/{0}/MeterPoints'.format(account_id)
    # token = 'QUtYcjkhJXkmVmVlUEJwNnAxJm1Md1kjU2RaTkRKcnZGVzROdHRiI0deS0EzYVpFS3ZYdCFQSEs0elNrMmxDdQ=='

    #prod
    api_url = 'https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/MeterPoints'.format(account_id)
    token = 'Wk01QnVWVU01aWlLTiVeUWtwMUIyRU5EbCN0VTJUek01KmJJVFcyVGFaeiNtJkFpYUJwRUNNM2MzKjVHcjVvIQ=='
    head = {'Content-Type': 'application/json',
           'Authorization': 'Bearer {0}'.format(token)}
    return api_url,token,head

# get meter point readings api info 
def get_meter_readings_api_info(account_id, meter_point_id):
    #UAT
    # api_url = 'https://api.uat.igloo.ignition.ensek.co.uk/Accounts/{0}/MeterPoints/{1}/Readings'.format(account_id,meter_point_id)
    # token = 'QUtYcjkhJXkmVmVlUEJwNnAxJm1Md1kjU2RaTkRKcnZGVzROdHRiI0deS0EzYVpFS3ZYdCFQSEs0elNrMmxDdQ=='
    #prod
    api_url = 'https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/MeterPoints/{1}/Readings'.format(account_id,meter_point_id)
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

   

def extract_meter_point_json(data, account_id,k):

    '''
    Extracting meterpoints, registers, meters, attributes data
    Writing into meter_points.csv , registers.csv, meteres.csv, attributes.csv
    key = meter_point_id
    '''
    meter_point_ids = []
    # global k

    ''' Processing meter points data'''
    meta_meters = ['associationStartDate', 'associationEndDate', 'supplyStartDate', 'supplyEndDate', 'isSmart', 'isSmartCommunicating', 'id', 'meterPointNumber', 'meterPointType']
    df_meterpoints = json_normalize(data)
    if(df_meterpoints.empty):
        print(" - has no meters points data")
    else:
        df_meterpoints['account_id'] = account_id
        df_meterpoints2 = df_meterpoints[meta_meters + ['account_id']]
        df_meterpoints1 = df_meterpoints2.rename(columns={'id' : 'meter_point_id'})
        meter_point_ids = df_meterpoints1['meter_point_id']
        # df_meterpoints1.to_csv('meter_points_' + str(account_id) + '_.csv')
        df_meter_points_string = df_meterpoints1.to_csv(None, index=False)
        file_name_meterpoints = 'meter_points_' + str(account_id) + '.csv'
        k.key = 'ensek-meterpoints/MeterPoints/' + file_name_meterpoints
        k.set_contents_from_string(df_meter_points_string)


    ''' Processing meters data'''
    meta_meters = ['meterSerialNumber', 'installedDate', 'removedDate','meterId', 'meter_point_id']
    df_meters = json_normalize(data, record_path=['meters'], meta=['id'], meta_prefix='meter_point_')
    if(df_meters.empty):
        print(" - has no meters data")
        log_error(" - has no meters data")
    else:
        df_meters['account_id'] = account_id
        df_meters1 = df_meters[meta_meters + ['account_id']]
        # df_meters1.to_csv('meters_'  + str(account_id) + '.csv')
        df_meters_string = df_meters1.to_csv(None, index=False)
        filename_meters = 'meters_' + str(account_id) + '.csv'
        k.key = 'ensek-meterpoints/Meters/' + filename_meters
        k.set_contents_from_string(df_meters_string)
        # print(df_meters_string)

    ''' Processing attributes data'''
    df_attributes = json_normalize(data, record_path=['attributes'], record_prefix='attributes_', meta=['id'], meta_prefix='meter_point_')
    if(df_attributes.empty):
        print(" - has no attributes data")
        log_error(" - has no attributes data")

    else:
        df_attributes['account_id'] = account_id
        # df_attributes.to_csv('attributes_'  + str(account_id) + '.csv')
        df_attributes_string = df_attributes.to_csv(None, index=False)
        filename_attributes =  'attributes_'  + str(account_id) + '.csv'
        k.key = 'ensek-meterpoints/Attributes/' + filename_attributes
        k.set_contents_from_string(df_attributes_string)
        # print(df_attributes_string)

    ''' Processing registers data'''
    ordered_columns = ['registers_eacAq','registers_registerReference','registers_sourceIdType','registers_tariffComponent','registers_tpr','registers_tprPeriodDescription','meter_point_meters_meterId','registers_id','meter_point_id']
    df_registers = json_normalize(data, record_path=['meters','registers'],meta=['id',['meters', 'meterId']], 
    meta_prefix='meter_point_', record_prefix='registers_', sep='_')
    if(df_registers.empty):
        print(" - has no registers data")
        log_error(" - has no registers data")
        
    else:
        df_registers1 = df_registers[ordered_columns]
        df_registers1.rename(columns={'meter_point_meters_meterId' : 'meter_id', 'registers_id' : 'register_id'}, inplace=True)
        # df_registers.rename(columns={'registers_id' : 'register_id'}, inplace=True)
        df_registers1['account_id'] = account_id
        # df_registers.to_csv('registers_' + str(account_id) + '.csv')
        df_registers_string = df_registers1.to_csv(None, index=False)
        filename_registers =  'registers_'  + str(account_id) + '.csv'
        k.key = 'ensek-meterpoints/Registers/' + filename_registers
        k.set_contents_from_string(df_registers_string)
        # print(df_registers_string)

    ''' Prcessing registers -> attributes data '''
    df_registersAttributes = json_normalize(data, record_path=['meters','registers','attributes'],meta=[['meters','meterId'],['meters','registers','id'],'id'], 
    meta_prefix='meter_point_', record_prefix='registersAttributes_', sep='_')
    if(df_registersAttributes.empty):
        print(" - has no registers data")
        log_error(" - has no registers data")
        
    else:
        df_registersAttributes.rename(columns={'meter_point_meters_meterId' : 'meter_id'}, inplace=True)
        df_registersAttributes.rename(columns={'meter_point_meters_registers_id' : 'register_id'}, inplace=True)
        df_registersAttributes['account_id'] = account_id
        # df_registersAttributes.to_csv('registers_' + str(account_id) + '.csv')
        df_registersAttributes_string = df_registersAttributes.to_csv(None, index=False)
        filename_registersAttributes =  'registers_'  + str(account_id) + '.csv'
        k.key = 'ensek-meterpoints/RegistersAttributes/' + filename_registersAttributes
        k.set_contents_from_string(df_registersAttributes_string)
        # print(df_registersAttributes_string)
    
    ''' Prcessing Meters -> attributes data '''
    df_metersAttributes = json_normalize(data, record_path=['meters','attributes'],meta=[['meters','meterId'],'id'], 
    meta_prefix='meter_point_', record_prefix='metersAttributes_', sep='_')
    if(df_metersAttributes.empty):
        print(" - has no registers data")
        log_error(" - has no registers data")
        
    else:
        df_metersAttributes.rename(columns={'meter_point_meters_meterId' : 'meter_id'}, inplace=True)
        df_metersAttributes['account_id'] = account_id
        # df_metersAttributes.to_csv('metersAttributes_' + str(account_id) + '.csv')
        df_metersAttributes_string = df_metersAttributes.to_csv(None, index=False)
        filename_metersAttributes =  'metersAttributes_'  + str(account_id) + '.csv'
        k.key = 'ensek-meterpoints/MetersAttributes/' + filename_metersAttributes
        k.set_contents_from_string(df_metersAttributes_string)
        # print(df_metersAttributes_string)

    return meter_point_ids 

''' Processing meter readings data'''
def extract_meter_readings_json(data, account_id, meter_point_id,k):
    # global k
    meta_readings = ['id', 'readingType', 'meterPointId', 'dateTime', 'createdDate', 'meterReadingSource']
    df_meter_readings = json_normalize(data, record_path=['readings'], meta=meta_readings, record_prefix='reading_')
    df_meter_readings['account_id'] = account_id
    df_meter_readings['meter_point_id'] = meter_point_id
    # df_meter_readings.to_csv('meter_point_readings_' + str(account_id) + '_' + str(meter_point_id) + '_' + '.csv')
    filename_readings = 'meter_point_readings_' + str(account_id) + '_' + str(meter_point_id) + '.csv'
    df_meter_readings_string = df_meter_readings.to_csv(None, index=False)
    k.key = 'ensek-meterpoints/Readings/' + filename_readings
    k.set_contents_from_string(df_meter_readings_string)
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
    with open('meterpoint_logs' + time.strftime('%m%d%Y') + '.csv' , mode='a') as errorlog:
        employee_writer = csv.writer(errorlog, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        employee_writer.writerow([error_msg, error_code])

def get_accountID_fromDB():

    conn = pymysql.connect(host=con.rds_config['host'], port=con.rds_config['port'], user=con.rds_config['user'], passwd=con.rds_config['pwd'], db=con.rds_config['db'])

    cur = conn.cursor()

    cur.execute(con.test_config['account_ids_sql'])

    account_ids = [row[0] for row in cur]
    cur.close()
    conn.close()
    
    return account_ids

def processAccounts(account_ids,k):
    for account_id in account_ids:
        t = con.api_config['total_no_of_calls']
        # run for configured account ids
        api_url,token,head = get_meter_point_api_info(account_id)
        print('ac:' + str(account_id) + str(multiprocessing.current_process()))
        msg_ac = 'ac:' + str(account_id)
        log_error(msg_ac, '')
        meter_info_response = get_api_response(api_url,token,head)
        # print(json.dumps(meter_info_response, indent=4))
        if(meter_info_response):
            
            formatted_meter_info = format_json_response(meter_info_response)
            meter_points = extract_meter_point_json(formatted_meter_info, account_id,k)
            for each_meter_point in meter_points:
                print('mp:' + str(each_meter_point))
                msg_mp = 'mp:' + str(each_meter_point)
                log_error(msg_mp, '')
                api_url,token,head = get_meter_readings_api_info(account_id, each_meter_point)
                meter_reading_response = get_api_response(api_url,token,head)
                # print(json.dumps(meter_reading_response, indent=4))
                if(meter_reading_response):
                    formatted_meter_reading = format_json_response(meter_reading_response)
                    extract_meter_readings_json(formatted_meter_reading, account_id, each_meter_point,k)
                else:
                    print('mp:' + str(each_meter_point) + ' has no data')
                    msg_mp = 'mp:' + str(each_meter_point) + ' has no data'
                    log_error(msg_mp, '')
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
    print(int(len(account_ids)/6))
    p = int(len(account_ids)/6)

    print(account_ids)

    start_time = datetime.datetime.now()
    p1 = multiprocessing.Process(target = processAccounts, args=(account_ids[0:p],k))
    p2 = multiprocessing.Process(target = processAccounts, args=(account_ids[p:2*p],k))
    p3 = multiprocessing.Process(target = processAccounts, args=(account_ids[2*p:3*p],k))
    p4 = multiprocessing.Process(target = processAccounts, args=(account_ids[3*p:4*p],k))
    p5 = multiprocessing.Process(target = processAccounts, args=(account_ids[4*p:5*p],k))
    p6 = multiprocessing.Process(target = processAccounts, args=(account_ids[5*p:],k))
    end_time = datetime.datetime.now()

    diff = end_time - start_time
    p1.start()
    p2.start()
    p3.start()
    p4.start()
    p5.start()
    p6.start()

    p1.join()
    p2.join()
    p3.join()
    p4.join()
    p5.join()
    p6.join()

    print("Process completed. Time taken: " + str(diff))