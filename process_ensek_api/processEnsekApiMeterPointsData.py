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

import sys
import os

sys.path.append('..')

from common import utils as util
from conf import config as con
from connections.connect_db import get_boto_S3_Connections as s3_con


class MeterPoints:
    max_calls = con.api_config['max_api_calls']
    rate = con.api_config['allowed_period_in_secs']

    def __init__(self):
        pass

    @sleep_and_retry
    @limits(calls=max_calls, period=rate)
    def get_api_response(self, api_url, head):
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
                    response = json.loads(response.content.decode('utf-8'))
                    return response
                else:
                    print('Problem Grabbing Data: ', response.status_code)
                    self.log_error('Response Error: Problem grabbing data', response.status_code)
                    break

            except ConnectionError:
                if time.time() > start_time + timeout:
                    print('Unable to Connect after {} seconds of ConnectionErrors'.format(timeout))
                    self.log_error('Unable to Connect after {} seconds of ConnectionErrors'.format(timeout))

                    break
                else:
                    print('Retrying connection in ' + str(retry_in_secs) + ' seconds' + str(i))
                    self.log_error('Retrying connection in ' + str(retry_in_secs) + ' seconds' + str(i))

                    time.sleep(retry_in_secs)
            i = i + retry_in_secs


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
                                                meta_prefix='meter_point_', record_prefix='registersAttributes_', sep='_')
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
        df_metersAttributes = json_normalize(data, record_path=['meters', 'attributes'], meta=[['meters', 'meterId'], 'id'],
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


    ''' Processing meter readings data'''


    def extract_meter_readings_json(self, data, account_id, meter_point_id, k, _dir_s3):
        # global k
        meta_readings = ['id', 'readingType', 'meterPointId', 'dateTime', 'createdDate', 'meterReadingSource']
        df_meter_readings = json_normalize(data, record_path=['readings'], meta=meta_readings, record_prefix='reading_')
        df_meter_readings['account_id'] = account_id
        df_meter_readings['meter_point_id'] = meter_point_id
        filename_readings = 'mp_readings_' + str(account_id) + '_' + str(meter_point_id) + '.csv'
        df_meter_readings['readings_filename'] = filename_readings
        # df_meter_readings.to_csv('meter_point_readings_' + str(account_id) + '_' + str(meter_point_id) + '_' + '.csv')

        df_meter_readings_string = df_meter_readings.to_csv(None, index=False)
        # k.key = 'ensek-meterpoints/Readings/' + filename_readings
        k.key = _dir_s3['s3_key']['Readings'] + filename_readings
        k.set_contents_from_string(df_meter_readings_string)
        # print(df_meter_readings_string)
        # print(filename_readings)


    ''' Processing meter readings data billeable'''


    def extract_meter_readings_billeable_json(self, data, account_id, meter_point_id, k, _dir_s3):
        # global k
        meta_readings = ['id', 'readingType', 'meterPointId', 'dateTime', 'createdDate', 'meterReadingSource']
        df_meter_readings = json_normalize(data, record_path=['readings'], meta=meta_readings, record_prefix='reading_')
        df_meter_readings['account_id'] = account_id
        df_meter_readings['meter_point_id'] = meter_point_id
        # df_meter_readings.to_csv('meter_point_readings_' + str(account_id) + '_' + str(meter_point_id) + '_' + '.csv')
        filename_readings = 'mp_readings_billeable_' + str(account_id) + '_' + str(meter_point_id) + '.csv'
        df_meter_readings_string = df_meter_readings.to_csv(None, index=False)
        # k.key = 'ensek-meterpoints/ReadingsBilleable/' + filename_readings
        k.key = _dir_s3['s3_key']['ReadingsBilleable'] + filename_readings
        k.set_contents_from_string(df_meter_readings_string)
        # print(df_meter_readings_string)
        # print(filename_readings)

    '''Format Json to handle null values'''

    def format_json_response(self, data):
        data_str = json.dumps(data, indent=4).replace('null', '""')
        data_json = json.loads(data_str)
        return data_json

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
            meter_info_response = self.get_api_response(api_url_mp1, head_mp)
            # print(type(meter_info_response))
            if meter_info_response:
                formatted_meter_info = self.format_json_response(meter_info_response)
                meter_points = self.extract_meter_point_json(formatted_meter_info, account_id, S3, dir_s3)
                for each_meter_point in meter_points:
                    print('mp:' + str(each_meter_point))
                    msg_mp = 'mp:' + str(each_meter_point)
                    self.log_error(msg_mp, '')

                    # api_url, head = get_meter_readings_api_info(account_id, each_meter_point)
                    api_url_mpr1 = api_url_mpr.format(account_id, each_meter_point)
                    meter_reading_response = self.get_api_response(api_url_mpr1, head_mpr)

                    # api_url, head = get_meter_readings_billeable_api_info(account_id, each_meter_point)
                    api_url_mprb1 = api_url_mprb.format(account_id, each_meter_point)
                    meter_reading_billeable_response = self.get_api_response(api_url_mprb1, head_mprb)

                    # print(json.dumps(meter_reading_response, indent=4))
                    if meter_reading_response:
                        formatted_meter_reading = self.format_json_response(meter_reading_response)
                        self.extract_meter_readings_json(formatted_meter_reading, account_id, each_meter_point, S3, dir_s3)
                    else:
                        print('mp:' + str(each_meter_point) + ' has no data')
                        msg_mp = 'mp:' + str(each_meter_point) + ' has no data'
                        self.log_error(msg_mp, '')

                    if meter_reading_billeable_response:
                        formatted_meter_reading = self.format_json_response(meter_reading_billeable_response)
                        self.extract_meter_readings_billeable_json(formatted_meter_reading, account_id, each_meter_point, S3, dir_s3)
                    else:
                        print('mp:' + str(each_meter_point) + ' has no data')
                        msg_mp = 'mp:' + str(each_meter_point) + ' has no data'
                        self.log_error(msg_mp, '')
            else:
                print('ac:' + str(account_id) + ' has no data')
                msg_ac = 'ac:' + str(account_id) + ' has no data'
                self.log_error(msg_ac, '')


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

    ####### Multiprocessing Starts #########
    env = util.get_env()
    if env == 'uat':
        n = 12  # number of process to run in parallel
    else:
        n = 24

    k = int(len(account_ids) / n)  # get equal no of files for each process

    print(len(account_ids))
    print(k)
    processes = []
    lv = 0

    start = timeit.default_timer()

    for i in range(n + 1):
        p1 = MeterPoints()
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
