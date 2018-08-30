import requests
import json
import pandas as pd
from pandas.io.json import json_normalize 
from ratelimit import limits, sleep_and_retry
import ig_config as con
import boto3 
from boto.s3.connection import S3Connection

max_calls = con.api_config['max_api_calls']
rate = con.api_config['allowed_period_in_secs']

# get meter point api info
def get_meter_point_api_info(account_id):
    api_url = 'https://api.uat.igloo.ignition.ensek.co.uk/Accounts/{0}/MeterPoints'.format(account_id)
    token = 'QUtYcjkhJXkmVmVlUEJwNnAxJm1Md1kjU2RaTkRKcnZGVzROdHRiI0deS0EzYVpFS3ZYdCFQSEs0elNrMmxDdQ=='
    head = {'Content-Type': 'application/json',
           'Authorization': 'Bearer {0}'.format(token)}
    return api_url,token,head

# get meter point readings api info
def get_meter_readings_api_info(account_id, meter_point_id):
    api_url = 'https://api.uat.igloo.ignition.ensek.co.uk/Accounts/{0}/MeterPoints/{1}/Readings'.format(account_id,meter_point_id)
    token = 'QUtYcjkhJXkmVmVlUEJwNnAxJm1Md1kjU2RaTkRKcnZGVzROdHRiI0deS0EzYVpFS3ZYdCFQSEs0elNrMmxDdQ=='
    head = {'Content-Type': 'application/json',
           'Authorization': 'Bearer {0}'.format(token)}
    return api_url,token,head

@sleep_and_retry
@limits(calls=max_calls, period=rate)
def get_api_response(api_url,token,head):
   ''' 
        get the response for the respective url that is passed as part of this function
   '''
   response = requests.get(api_url, headers=head)

   if response.status_code == 200:
       return json.loads(response.content.decode('utf-8'))

   else:
       print ('Problem Grabbing Data: ', response.status_code)
       return None


def extract_meter_point_json(data, account_id):
    '''
    Extracting meterpoints, registers, meters, attributes data
    Writing into meter_points.csv , registers.csv, meteres.csv, attributes.csv
    key = meter_point_id
    '''

    meta_meters = ['associationStartDate', 'associationEndDate', 'supplyStartDate', 'supplyEndDate', 'isSmart', 'isSmartCommunicating', 'id', 'meterPointNumber', 'meterPointType']
    df_meterpoints = json_normalize(data, record_path=['meters'], meta=meta_meters)
    df_meterpoints['account_id'] = account_id
    df_meterpoints1 = df_meterpoints[meta_meters + ['account_id']]
    df_meterpoints1.rename(columns={'id' : 'meter_point_id'}, inplace=True)
    df_meterpoints1.to_csv('meter_points_' + str(account_id) + '_.csv')
    
    
    meta_meters = ['meterId', 'meterSerialNumber', 'installedDate', 'removedDate', 'meter_point_id']
    df_meters = json_normalize(data, record_path=['meters'], meta=['id'], meta_prefix='meter_point_')
    df_meters['account_id'] = account_id
    df_meters1 = df_meters[meta_meters + ['account_id']]
    df_meters1.to_csv('meters_'  + str(account_id) + '.csv')

    df_attributes = json_normalize(data, record_path=['attributes'], record_prefix='attributes_', meta=['id'], meta_prefix='meter_point_')
    df_attributes['account_id'] = account_id
    df_attributes.to_csv('attributes_'  + str(account_id) + '.csv')

    df_registers = json_normalize(data, record_path=['meters','registers'],meta=['id'], 
    meta_prefix='meter_point_', record_prefix='registers_')
    df_registers['account_id'] = account_id
    df_registers.to_csv('registers_' + str(account_id) + '.csv')

    return df_meterpoints1['meter_point_id']

def extract_meter_readings_json(data, account_id, meter_point_id):
    meta_readings = ['id', 'readingType', 'meterPointId', 'dateTime', 'createdDate', 'meterReadingSource']
    df_meter_readings = json_normalize(data, record_path=['readings'], meta=meta_readings, record_prefix='reading_')
    df_meter_readings['account_id'] = account_id
    df_meter_readings['meter_point_id'] = meter_point_id
    df_meter_readings.to_csv('meter_point_readings_' + str(account_id) + '_' + str(meter_point_id) + '_' + '.csv')
    # print(df_meter_readings)

def get_S3_Connections():
    access_key = con.s3_config['access_key']
    secret_key = con.s3_config['secret_key']
    print(access_key)
    print(secret_key)

    s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    print(s3.list_objects(Bucket = 'igloo-uat-bucket'))
    


def main():
    account_ids = con.api_config['account_ids']

    # get_S3_Connections()

    
    for account_id in account_ids:
        api_url,token,head = get_meter_point_api_info(account_id)
        meter_info_response = get_api_response(api_url,token,head)
        meter_points = extract_meter_point_json(meter_info_response, account_id)

        for each_meter_point in meter_points:
            api_url,token,head = get_meter_readings_api_info(account_id, each_meter_point)
            meter_reading_response = get_api_response(api_url,token,head)
            extract_meter_readings_json(meter_reading_response, account_id, each_meter_point)
    
if __name__ == "__main__":
    main()