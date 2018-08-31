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
from requests import ConnectionError

max_calls = con.api_config['max_api_calls']
rate = con.api_config['allowed_period_in_secs']

# get meter point api info
def get_meter_point_api_info(account_id):
    api_url = 'https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/MeterPoints'.format(account_id)
    # token = 'QUtYcjkhJXkmVmVlUEJwNnAxJm1Md1kjU2RaTkRKcnZGVzROdHRiI0deS0EzYVpFS3ZYdCFQSEs0elNrMmxDdQ=='
    token = 'Wk01QnVWVU01aWlLTiVeUWtwMUIyRU5EbCN0VTJUek01KmJJVFcyVGFaeiNtJkFpYUJwRUNNM2MzKjVHcjVvIQ=='
    head = {'Content-Type': 'application/json',
           'Authorization': 'Bearer {0}'.format(token)}
    return api_url,token,head

# get meter point readings api info
def get_meter_readings_api_info(account_id, meter_point_id):
    api_url = 'https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/MeterPoints/{1}/Readings'.format(account_id,meter_point_id)
    # token = 'QUtYcjkhJXkmVmVlUEJwNnAxJm1Md1kjU2RaTkRKcnZGVzROdHRiI0deS0EzYVpFS3ZYdCFQSEs0elNrMmxDdQ=='
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

        except ConnectionError:
            if time.time() > start_time + timeout:
                print('Unable to Connect after {} seconds of ConnectionErrors'.format(timeout))
                break
            else:
                print('Retrying connection in ' + str(retry_in_secs) +  ' seconds' + str(i))
                time.sleep(retry_in_secs)
        i=i+retry_in_secs

   

def extract_meter_point_json(data, account_id, k):
    '''
    Extracting meterpoints, registers, meters, attributes data
    Writing into meter_points.csv , registers.csv, meteres.csv, attributes.csv
    key = meter_point_id
    '''
    meter_point_ids = []

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
    meta_meters = ['meterId', 'meterSerialNumber', 'installedDate', 'removedDate', 'meter_point_id']
    df_meters = json_normalize(data, record_path=['meters'], meta=['id'], meta_prefix='meter_point_')
    if(df_meters.empty):
        print(" - has no meters data")
    else:
        df_meters['account_id'] = account_id
        df_meters1 = df_meters[meta_meters + ['account_id']]
        # df_meters1.to_csv('meters_'  + str(account_id) + '.csv')
        df_meters_string = df_meters1.to_csv(None, index=False)
        filename_meters = 'meters_' + str(account_id) + '.csv'
        k.key = 'ensek-meterpoints/Meters/' + filename_meters
        k.set_contents_from_string(df_meters_string)
    

    ''' Processing attributes data'''
    df_attributes = json_normalize(data, record_path=['attributes'], record_prefix='attributes_', meta=['id'], meta_prefix='meter_point_')
    if(df_attributes.empty):
        print(" - has no attributes data")
    else:
        df_attributes['account_id'] = account_id
        # df_attributes.to_csv('attributes_'  + str(account_id) + '.csv')
        df_attributes_string = df_attributes.to_csv(None, index=False)
        filename_attributes =  'attributes_'  + str(account_id) + '.csv'
        k.key = 'ensek-meterpoints/Attributes/' + filename_attributes
        k.set_contents_from_string(df_attributes_string)

    ''' Processing registers data'''
    df_registers = json_normalize(data, record_path=['meters','registers'],meta=['id'], 
    meta_prefix='meter_point_', record_prefix='registers_')
    if(df_registers.empty):
        print(" - has no registers data")
    else:
        df_registers['account_id'] = account_id
        # df_registers.to_csv('registers_' + str(account_id) + '.csv')
        df_registers_string = df_registers.to_csv(None, index=False)
        filename_registers =  'registers_'  + str(account_id) + '.csv'
        k.key = 'ensek-meterpoints/Registers/' + filename_registers
        k.set_contents_from_string(df_registers_string)

    return meter_point_ids 

''' Processing meter readings data'''
def extract_meter_readings_json(data, account_id, meter_point_id,k):
    meta_readings = ['id', 'readingType', 'meterPointId', 'dateTime', 'createdDate', 'meterReadingSource']
    df_meter_readings = json_normalize(data, record_path=['readings'], meta=meta_readings, record_prefix='reading_')
    df_meter_readings['account_id'] = account_id
    df_meter_readings['meter_point_id'] = meter_point_id
    # df_meter_readings.to_csv('meter_point_readings_' + str(account_id) + '_' + str(meter_point_id) + '_' + '.csv')
    filename_readings = 'meter_point_readings_' + str(account_id) + '_' + str(meter_point_id) + '.csv'
    df_meter_readings_string = df_meter_readings.to_csv(None, index=False)
    k.key = 'ensek-meterpoints/Readings/' + filename_readings
    k.set_contents_from_string(df_meter_readings_string)

'''Get S3 connection'''
def get_S3_Connections():
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


def main():
    '''Enable this to test for 1 account id'''
    # account_ids = con.api_config['account_ids']
    
    t = con.api_config['total_no_of_calls']

    k = get_S3_Connections()
    
    account_ids = get_Users(k)

    # run for configured account ids
    for account_id in account_ids[:t]:
        api_url,token,head = get_meter_point_api_info(account_id)
        meter_info_response = get_api_response(api_url,token,head)

        if(meter_info_response):
            print('ac:' + str(account_id))
            formatted_meter_info = format_json_response(meter_info_response)
            meter_points = extract_meter_point_json(formatted_meter_info, account_id, k)
            for each_meter_point in meter_points:
                api_url,token,head = get_meter_readings_api_info(account_id, each_meter_point)
                meter_reading_response = get_api_response(api_url,token,head)
                if(meter_reading_response):
                    print('mp:' + str(each_meter_point))
                    formatted_meter_reading = format_json_response(meter_reading_response)
                    extract_meter_readings_json(formatted_meter_reading, account_id, each_meter_point,k)
                else:
                    print('mp:' + str(each_meter_point) + ' has no data')
        else:
            print('ac:' + str(account_id) + 'has no data')

if __name__ == "__main__":
    main()