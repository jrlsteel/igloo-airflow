import requests
import json
from pandas.io.json import json_normalize
from ratelimit import limits, sleep_and_retry
import time
from time import sleep
import timeit
from requests import ConnectionError
import csv
import multiprocessing
from multiprocessing import freeze_support
import datetime
import pandas

import sys
import os

sys.path.append('..')

from common import utils as util
from conf import config as con
from connections.connect_db import get_boto_S3_Connections as s3_con


class MeterPoints:
    max_calls = con.api_config['max_api_calls']
    rate = con.api_config['allowed_period_in_secs']

    def __init__(self, process_num):
        logs_dir_path = sys.path[0] + '/logs/'
        if not os.path.exists(logs_dir_path):
            os.makedirs(logs_dir_path)
        self.pnum = process_num
        self.generic_log_file = sys.path[0] + '/logs/' + 'meterpoint_generic_logs_' + time.strftime('%Y%m%d') + '.csv'

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
                    self.log_msg(
                        '{0}, {1}, {2}, {3}, {4}'.format(account_id, 'success', total_api_time,
                                                         attempt_num, method_time), msg_type='timing')
                    response = json.loads(response.content.decode('utf-8'))
                    return response
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
        self.log_msg(
            '{0}, {1}, {2}, {3}, {4}'.format(account_id, exit_type, total_api_time, attempt_num,
                                             method_time), msg_type='timing')

    def extract_meter_point_json(self, data, account_id, k, dir_s3):
        '''
        Extracting meterpoints, registers, meters, attributes data
        Writing into meter_points.csv , registers.csv, meteres.csv, attributes.csv
        key = meter_point_id
        '''
        meter_point_ids = []
        # global k

        ''' Processing meter points data'''
        meta_meters = ['associationStartDate', 'associationEndDate', 'supplyStartDate', 'supplyEndDate', 'isSmart',
                       'isSmartCommunicating', 'id', 'meterPointNumber', 'meterPointType']
        df_meterpoints = json_normalize(data)
        if df_meterpoints.empty:
            print(" - has no meters points data")
        else:
            df_meterpoints['account_id'] = account_id
            df_meterpoints1 = df_meterpoints[meta_meters + ['account_id']]
            df_meterpoints1.rename(columns={'id': 'meter_point_id'}, inplace=True)
            meter_point_ids = df_meterpoints1['meter_point_id']
            df_meter_points_string = df_meterpoints1.to_csv(None, index=False)
            # print(df_meter_points_string)
            file_name_meterpoints = 'meter_points_' + str(account_id) + '.csv'
            # k.key = 'ensek-meterpoints/MeterPoints/' + file_name_meterpoints
            k.key = dir_s3['s3_key']['MeterPoints'] + file_name_meterpoints
            k.set_contents_from_string(df_meter_points_string)

        ''' Processing meters data'''
        meta_meters = ['meterSerialNumber', 'installedDate', 'removedDate', 'meterId', 'meter_point_id']
        df_meters = json_normalize(data, record_path=['meters'], meta=['id'], meta_prefix='meter_point_')
        if df_meters.empty:
            print(" - has no meters data")
            self.log_error(" - has no meters data")
        else:
            df_meters['account_id'] = account_id
            df_meters1 = df_meters[meta_meters + ['account_id']]
            # df_meters1.to_csv('meters_'  + str(account_id) + '.csv')
            df_meters_string = df_meters1.to_csv(None, index=False)
            filename_meters = 'meters_' + str(account_id) + '.csv'
            # k.key = 'ensek-meterpoints/Meters/' + filename_meters
            k.key = dir_s3['s3_key']['Meters'] + filename_meters
            k.set_contents_from_string(df_meters_string)
            # print(df_meters_string)

        ''' Processing attributes data'''
        df_attributes = json_normalize(data, record_path=['attributes'], record_prefix='attributes_', meta=['id'],
                                       meta_prefix='meter_point_')
        if df_attributes.empty:
            print(" - has no attributes data")
            self.log_error(" - has no attributes data")

        else:
            df_attributes['account_id'] = account_id
            df_attributes['attributes_attributeValue'] = df_attributes[
                'attributes_attributeValue'].str.replace(",", " ")
            # df_attributes.to_csv('attributes_'  + str(account_id) + '.csv')
            df_attributes_string = df_attributes.to_csv(None, index=False)
            filename_attributes = 'mp_attributes_' + str(account_id) + '.csv'
            # k.key = 'ensek-meterpoints/Attributes/' + filename_attributes
            k.key = dir_s3['s3_key']['MeterPointsAttributes'] + filename_attributes
            k.set_contents_from_string(df_attributes_string)
            # print(df_attributes_string)

        ''' Processing registers data'''
        ordered_columns = ['registers_eacAq', 'registers_registerReference', 'registers_sourceIdType',
                           'registers_tariffComponent', 'registers_tpr', 'registers_tprPeriodDescription',
                           'meter_point_meters_meterId', 'registers_id', 'meter_point_id']
        df_registers = json_normalize(data, record_path=['meters', 'registers'], meta=['id', ['meters', 'meterId']],
                                      meta_prefix='meter_point_', record_prefix='registers_', sep='_')
        if df_registers.empty:
            print(" - has no registers data")
            self.log_error(" - has no registers data")

        else:
            df_registers1 = df_registers[ordered_columns]
            df_registers1.rename(columns={'meter_point_meters_meterId': 'meter_id', 'registers_id': 'register_id'},
                                 inplace=True)
            # df_registers.rename(columns={'registers_id' : 'register_id'}, inplace=True)
            df_registers1['account_id'] = account_id
            df_registers1['registers_sourceIdType'] = df_registers1['registers_sourceIdType'].str.replace("\t",
                                                                                                          "").str.strip()
            # df_registers.to_csv('registers_' + str(account_id) + '.csv')
            df_registers_string = df_registers1.to_csv(None, index=False)
            filename_registers = 'registers_' + str(account_id) + '.csv'
            # k.key = 'ensek-meterpoints/Registers/' + filename_registers
            k.key = dir_s3['s3_key']['Registers'] + filename_registers
            k.set_contents_from_string(df_registers_string)
            # print(df_registers_string)

        ''' Prcessing registers -> attributes data '''
        df_registersAttributes = json_normalize(data, record_path=['meters', 'registers', 'attributes'],
                                                meta=[['meters', 'meterId'], ['meters', 'registers', 'id'], 'id'],
                                                meta_prefix='meter_point_', record_prefix='registersAttributes_',
                                                sep='_')
        if df_registersAttributes.empty:
            print(" - has no registers data")
            self.log_error(" - has no registers data")

        else:
            df_registersAttributes.rename(columns={'meter_point_meters_meterId': 'meter_id'}, inplace=True)
            df_registersAttributes.rename(columns={'meter_point_meters_registers_id': 'register_id'}, inplace=True)
            df_registersAttributes['account_id'] = account_id
            # df_registersAttributes.to_csv('registers_' + str(account_id) + '.csv')
            df_registersAttributes_string = df_registersAttributes.to_csv(None, index=False)
            filename_registersAttributes = 'registersAttributes_' + str(account_id) + '.csv'
            # k.key = 'ensek-meterpoints/RegistersAttributes/' + filename_registersAttributes
            k.key = dir_s3['s3_key']['RegistersAttributes'] + filename_registersAttributes
            k.set_contents_from_string(df_registersAttributes_string)
            # print(df_registersAttributes_string)

        ''' Prcessing Meters -> attributes data'''
        df_metersAttributes = json_normalize(data, record_path=['meters', 'attributes'],
                                             meta=[['meters', 'meterId'], 'id'],
                                             meta_prefix='meter_point_', record_prefix='metersAttributes_', sep='_')
        if df_metersAttributes.empty:
            print(" - has no registers data")
            self.log_error(" - has no registers data")

        else:
            df_metersAttributes.rename(columns={'meter_point_meters_meterId': 'meter_id'}, inplace=True)
            df_metersAttributes['account_id'] = account_id
            df_metersAttributes['metersAttributes_attributeValue'] = df_metersAttributes[
                'metersAttributes_attributeValue'].str.replace(",", " ")
            # df_metersAttributes.to_csv('metersAttributes_' + str(account_id) + '.csv')
            df_metersAttributes_string = df_metersAttributes.to_csv(None, index=False)
            filename_metersAttributes = 'metersAttributes_' + str(account_id) + '.csv'
            # k.key = 'ensek-meterpoints/MetersAttributes/' + filename_metersAttributes
            k.key = dir_s3['s3_key']['MetersAttributes'] + filename_metersAttributes
            k.set_contents_from_string(df_metersAttributes_string)
            # print(df_metersAttributes_string)

        return meter_point_ids

    '''Format Json to handle null values'''

    def format_json_response(self, data):
        data_str = json.dumps(data, indent=4).replace('null', '""')
        data_json = json.loads(data_str)
        return data_json

    def log_msg(self, msg, msg_type='diagnostic'):
        with open(self.generic_log_file, mode='a+') as log_msg_file:
            log_msg_file.write(
                '{0}, {1}, {2}, {3}\n'.format(self.pnum, datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), msg, msg_type))

    def log_error(self, error_msg, error_code=''):
        logs_dir_path = sys.path[0] + '/logs/'
        if not os.path.exists(logs_dir_path):
            os.makedirs(logs_dir_path)
        with open(sys.path[0] + '/logs/' + 'meterpoint_logs_' + time.strftime('%d%m%Y') + '.csv', mode='a') as errorlog:
            employee_writer = csv.writer(errorlog, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            employee_writer.writerow([error_msg, error_code])

    def processAccounts(self, account_ids, S3, dir_s3):

        api_url_mp, head_mp = util.get_ensek_api_info1('meterpoints')
        api_url_mpr, head_mpr = util.get_ensek_api_info1('meterpoints_readings')
        api_url_mprb, head_mprb = util.get_ensek_api_info1('meterpoints_readings_billeable')

        for account_id in account_ids:
            t = con.api_config['total_no_of_calls']
            print('ac:' + str(account_id))
            msg_ac = 'ac:' + str(account_id)
            self.log_error(msg_ac, '')
            api_url_mp1 = api_url_mp.format(account_id)
            meter_info_response = self.get_api_response(api_url_mp1, head_mp, account_id)
            # print(type(meter_info_response))
            if meter_info_response:
                formatted_meter_info = self.format_json_response(meter_info_response)
                meter_points = self.extract_meter_point_json(formatted_meter_info, account_id, S3, dir_s3)
                for each_meter_point in meter_points:
                    print('mp:' + str(each_meter_point))
                    msg_mp = 'mp:' + str(each_meter_point)
                    self.log_error(msg_mp, '')
            else:
                print('ac:' + str(account_id) + ' has no data')
                msg_ac = 'ac:' + str(account_id) + ' has no data'
                self.log_error(msg_ac, '')

    def callApisNoProcessing(self, account_ids):
        api_url_mp, head_mp = util.get_ensek_api_info1('meterpoints')

        for account_id in account_ids:
            t = con.api_config['total_no_of_calls']
            print('ac:' + str(account_id))
            msg_ac = 'ac:' + str(account_id) + ' no processing'
            self.log_error(msg_ac, '')
            api_url_mp1 = api_url_mp.format(account_id)
            self.get_api_response(api_url_mp1, head_mp, account_id)


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

    # Enable to test without multiprocessing.
    # p = MeterPoints()
    # p.processAccounts(account_ids, s3, dir_s3)

    weekday = datetime.date.today().weekday()
    if weekday == 5 or weekday == 6:  # Saturday or Sunday
        print("Starting no-processing api call run-through")
        n_processes = 12
        k = int(len(account_ids) / n_processes)

        print(len(account_ids))
        print(k)
        processes = []
        lv = 0

        start = timeit.default_timer()

        for i in range(n_processes + 1):
            p1 = MeterPoints(i)
            print(i)
            uv = i * k
            if i == n_processes:
                # print(d18_keys_s3[l:])
                t = multiprocessing.Process(target=p1.callApisNoProcessing, args=([account_ids[lv:]]))
            else:
                # print(d18_keys_s3[l:u])
                t = multiprocessing.Process(target=p1.callApisNoProcessing, args=([account_ids[lv:uv]]))
            lv = uv

            processes.append(t)

        for p in processes:
            p.start()
            sleep(2)

        for process in processes:
            process.join()

    ####### Multiprocessing Starts #########
    env = util.get_env()
    total_processes = util.get_multiprocess('total_ensek_processes')

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
        p1 = MeterPoints(i)
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
        sleep(2)

    for process in processes:
        process.join()
    ####### Multiprocessing Ends #########

    print("Process completed in " + str(timeit.default_timer() - start) + ' seconds')
