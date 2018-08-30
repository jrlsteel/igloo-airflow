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
   response = requests.get(api_url, headers=head)
   
   if response.status_code == 200:
       return json.loads(response.content.decode('utf-8'))

   else:
       print ('Problem Grabbing Data: ', response.status_code)
       return None


def extract_meter_point_json(data, account_id, k):
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
    # df_meterpoints1.to_csv('meter_points_' + str(account_id) + '_.csv')
    df_meter_points_string = df_meterpoints1.to_csv(None, index=False)
    file_name_meterpoints = 'meter_points_' + str(account_id) + '.csv'
    k.key = 'ensek-meterpoints/MeterPoints/' + file_name_meterpoints
    k.set_contents_from_string(df_meter_points_string)
    
    meta_meters = ['meterId', 'meterSerialNumber', 'installedDate', 'removedDate', 'meter_point_id']
    df_meters = json_normalize(data, record_path=['meters'], meta=['id'], meta_prefix='meter_point_')
    df_meters['account_id'] = account_id
    df_meters1 = df_meters[meta_meters + ['account_id']]
    # df_meters1.to_csv('meters_'  + str(account_id) + '.csv')
    df_meters_string = df_meters1.to_csv(None, index=False)
    filename_meters = 'meters_' + str(account_id) + '.csv'
    k.key = 'ensek-meterpoints/Meters/' + filename_meters
    k.set_contents_from_string(df_meters_string)

    df_attributes = json_normalize(data, record_path=['attributes'], record_prefix='attributes_', meta=['id'], meta_prefix='meter_point_')
    df_attributes['account_id'] = account_id
    # df_attributes.to_csv('attributes_'  + str(account_id) + '.csv')
    df_attributes_string = df_attributes.to_csv(None, index=False)
    filename_attributes =  'attributes_'  + str(account_id) + '.csv'
    k.key = 'ensek-meterpoints/Attributes/' + filename_attributes
    k.set_contents_from_string(df_attributes_string)

    df_registers = json_normalize(data, record_path=['meters','registers'],meta=['id'], 
    meta_prefix='meter_point_', record_prefix='registers_')
    df_registers['account_id'] = account_id
    # df_registers.to_csv('registers_' + str(account_id) + '.csv')
    df_registers_string = df_registers.to_csv(None, index=False)
    filename_registers =  'registers_'  + str(account_id) + '.csv'
    k.key = 'ensek-meterpoints/Registers/' + filename_registers
    k.set_contents_from_string(df_registers_string)

    return df_meterpoints1['meter_point_id']

def extract_meter_readings_json(data, account_id, meter_point_id,k):
    meta_readings = ['id', 'readingType', 'meterPointId', 'dateTime', 'createdDate', 'meterReadingSource']
    df_meter_readings = json_normalize(data, record_path=['readings'], meta=meta_readings, record_prefix='reading_')
    df_meter_readings['account_id'] = account_id
    df_meter_readings['meter_point_id'] = meter_point_id
    # df_meter_readings.to_csv('meter_point_readings_' + str(account_id) + '_' + str(meter_point_id) + '_' + '.csv')
    # df_meter_readings_string = df_meter_readings.applymap(str)
    # [x.encode('utf-8') for x in df_meter_readings_string]
    filename_readings = 'meter_point_readings_' + str(account_id) + '_' + str(meter_point_id) + '.csv'
    df_meter_readings_string = df_meter_readings.to_csv(None, index=False)
    k.key = 'ensek-meterpoints/Readings/' + filename_readings
    k.set_contents_from_string(df_meter_readings_string)

    # csv_buffer = BytesIO()
    # print(type(csv_buffer))
    # s3.put_object(
    # Body=csv_buffer.getvalue(),
    # ContentType='application/vnd.ms-excel',
    # Bucket='igloo-uat-bucket',
    # Key='ensek-meterpoints/Readings/meterpointreadings.csv'
    # )
    # r = s3.put_object(Key='ensek-meterpoints/Readings/meterpointreadings.csv', Body=toBinary(df_meter_readings_string),Bucket='igloo-uat-bucket',)
    # print(r)
    # df_meter_readings_string = df_meter_readings.to_string
    # df_meter_readings_string
    # print(df_meter_readings)

def get_S3_Connections():
    access_key = con.s3_config['access_key']
    secret_key = con.s3_config['secret_key']
    print(access_key)
    print(secret_key)

    s3 = boto.connect_s3(aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    bucket = s3.get_bucket('igloo-uat-bucket')
    k = Key(bucket)
    return k

def get_Users(k):
    filename_Users = 'users.csv'
    k.key = 'ensek-meterpoints/Users/' + filename_Users
    k.open()
    l = k.read()
    s = l.decode('utf-8')
    p = s.splitlines()
    # print(len(p))
    return p


def main():
    # account_ids = con.api_config['account_ids']
    t = con.api_config['total_no_of_calls']

    k = get_S3_Connections()

    account_ids = get_Users(k)
    # run for configured account ids
    for account_id in account_ids[:t]:
        api_url,token,head = get_meter_point_api_info(account_id)
        meter_info_response = get_api_response(api_url,token,head)
        if(meter_info_response):
            meter_points = extract_meter_point_json(meter_info_response, account_id, k)
            print('ac:' + str(account_id))
            for each_meter_point in meter_points:
                api_url,token,head = get_meter_readings_api_info(account_id, each_meter_point)
                meter_reading_response = get_api_response(api_url,token,head)
                if(meter_reading_response):
                    extract_meter_readings_json(meter_reading_response, account_id, each_meter_point,k)
                    print('mp:' + str(each_meter_point))
                else:
                    print('mp:' + account_id + ' has no data')
                    
        else:
            print('ac:' + account_id + ' has no data')

if __name__ == "__main__":
    main()