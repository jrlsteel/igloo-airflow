import requests
import json
from pandas.io.json import json_normalize 
from ratelimit import limits, sleep_and_retry
import ig_config as con
import boto
from boto.s3.key import Key
import time
import datetime
from requests import ConnectionError
import csv
import pymysql
import multiprocessing
from multiprocessing import Pool, freeze_support, Process


max_calls = con.api_config['max_api_calls']
rate = con.api_config['allowed_period_in_secs']
# k = Key()

# get meter point api info
def get_estimates_internal_api_info(account_id):
    #UAT
    # api_url = 'https://api.uat.igloo.ignition.ensek.co.uk/Accounts/{0}/MeterPoints'.format(account_id)
    # token = 'QUtYcjkhJXkmVmVlUEJwNnAxJm1Md1kjU2RaTkRKcnZGVzROdHRiI0deS0EzYVpFS3ZYdCFQSEs0elNrMmxDdQ=='

    #prod
    api_url = 'https://igloo.ignition.ensek.co.uk/api/accounts/{0}/estimatedusage'.format(account_id)
    token = 'QBToqAkACJpjAyABQNNGdoMh9gY59iRDSSu9W3AYE9wxAtlcAgx7ZDJU5misU0GWe4jOS1CL4kBlM1Qzxc37-H2hMXXZmuQPuuFDPFqHzZIpnMyHYKHAaR4iFEqIgoFaWGdlXYQZLMqxOqxxwdLrjwKspviiKuaF2z6qvY_h3YV2Yzh0vFjaFFfEHjAmmMon4sSB0PfGqQW_SQITFZJ2L5x67NBbfmUbqjGt1KNWRCXHKihH3RWp0UBrvtn7mdP2q-gvylqFjBUs0SSGwYT5ZvFdwhvwaGWMjFBQCZ-kvfx3b91dcOPIo5mEy-dkgdOEJykGImcRLXPcuMLunwJNyCjN-ORSo6Bh4fBnhrVyURb_QCgESkEyK454rChUf5KIOWQVIF7U-0vxiLU9rzcj97g8SMCom2fnZhflIR8nmwM6tjpkov9ehIXYLs8GF5dsOZYOyZofqw7pHcWWwDmydflCjDlt8tEZPuonBC9eCeVxusgRosanK3m41oxGTZ53Cpv_JIlSj8oN2KncLCAGUFF4G4mDydCoNCL5x9q9YB89W7GjroS33oXOxLX3skym1W-jz7A8aw_iRkDyv42k-nRP2gBawzhgbdmw8yF2nTiJM_3MO_PLbYVVn0SzIPC14VuwrkZE5cmCu5JEwcdnoACCIRHrG76AofIFmBdkf_uY0AavwDVyBkMv1y-N6yoCsk-8yekfqojwugrQyUM_4p4-fHfZhpnWUSjvoYmQK_NG6jge7AQyDPxEa65s7uNlkE2L6fzJSssrj8633gSTsVzf1IyTTKkYFSZKL-9nC-yJfDqucpnEnRiYAOw-5ecPpBPp-w'
    head = {'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json',
            'Referrer':'https://igloo.ignition.ensek.co.uk',
           'Authorization': 'Bearer {0}'.format(token)}
    return api_url,token,head

@sleep_and_retry
@limits(calls=max_calls, period=rate)
def get_api_response(api_url,token,head):
   ''' 
        get the response for the respective url that is passed as part of this function
   '''
   session = requests.Session()
   start_time = time.time()
   timeout = con.api_config['connection_timeout']
   retry_in_secs = con.api_config['retry_in_secs']
   i=0
   while True:
        try:
            response = session.get(api_url, headers=head)

            if response.status_code == 200:
                response_json = json.loads(response.content.decode('utf-8'))
                response_items_elec = response_json['Electricity']
                response_items_gas = response_json['Gas']
                return response_items_elec, response_items_gas
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

   

def extract_internal_data_response_elec(data, account_id,k):

   
    ''' Processing meter points data'''
    # meta_meters = ['associationStartDate', 'associationEndDate', 'supplyStartDate', 'supplyEndDate', 'isSmart', 'isSmartCommunicating', 'id', 'meterPointNumber', 'meterPointType']
    df_internal_readings = json_normalize(data)
    # print(df_internal_readings)
    if(df_internal_readings.empty):
        print(" - has no readings data")
    else:
        df_internal_readings['account_id'] = account_id
        df_internal_readings_string = df_internal_readings.to_csv(None, index=False)
        file_name_internal_readings = 'internal_estimates_elec_' + str(account_id) + '.csv'
        k.key = 'ensek-meterpoints/EstimatesElecInternal/' + file_name_internal_readings
        k.set_contents_from_string(df_internal_readings_string)


def extract_internal_data_response_gas(data, account_id, k):
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
        k.key = 'ensek-meterpoints/EstimatesGasInternal/' + file_name_internal_readings
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
    data_str = json.dumps(data, indent=4).replace('null','""')
    data_json = json.loads(data_str)
    return data_json

def log_error(error_msg, error_code=''):
    with open('estimates_logs_' + time.strftime('%m%d%Y') + '.csv' , mode='a') as errorlog:
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
        api_url,token,head = get_estimates_internal_api_info(account_id)
        # print('ac:' + str(account_id) + str(multiprocessing.current_process()))
        print('ac:' + str(account_id))
        msg_ac = 'ac:' + str(account_id)
        log_error(msg_ac, '')
        internal_data_response_elec, internal_data_response_gas = get_api_response(api_url,token,head)
        # print(json.dumps(internal_data_response, indent=4))

        formatted_internal_data_elec = format_json_response(internal_data_response_elec)
        extract_internal_data_response_elec(formatted_internal_data_elec, account_id,k)

        formatted_internal_data_gas = format_json_response(internal_data_response_gas)
        extract_internal_data_response_gas(formatted_internal_data_gas, account_id, k)


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
    print(int(len(account_ids)/12))
    p = int(len(account_ids)/12)

    # print(account_ids)

    start_time = datetime.datetime.now()
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