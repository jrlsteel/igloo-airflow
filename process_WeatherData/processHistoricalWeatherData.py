import timeit
import requests
import json
from pandas.io.json import json_normalize
from ratelimit import limits, sleep_and_retry
import time
from requests import ConnectionError
import csv
import multiprocessing
from multiprocessing import freeze_support
import datetime

import sys
import os

sys.path.append('..')

from conf import config as con
from common import utils as util
from connections.connect_db import get_boto_S3_Connections as s3_con
from connections import connect_db as db


class HistoricalWeather:
    max_calls = con.api_config['max_api_calls']
    rate = con.api_config['allowed_period_in_secs']

    def __init__(self):
        self.start_date = '01/01/2018'
        # self.start_date = datetime.date()

    @sleep_and_retry
    @limits(calls=max_calls, period=rate)
    def get_api_response(self, api_url, head):
        """
            get the response for the respective url that is passed as part of this function
        """
        session = requests.Session()
        start_time = time.time()
        timeout = con.api_config['connection_timeout']
        retry_in_secs = con.api_config['retry_in_secs']
        i = 0
        while True:
            try:
                response = session.get(api_url, headers=head)
                if response.status_code == 200:
                    if response.content.decode('utf-8') != '':
                        response_json = json.loads(response.content.decode('utf-8'))
                        return response_json
                else:
                    print('Problem Grabbing Data: ', response.status_code)
                    self.log_error('Response Error: Problem grabbing data', response.status_code)
                    return None
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

    def extract_epc_data(self, data, postcode_sector, k, dir_s3):
        epc_rows_df = json_normalize(data, 'rows')
        if (epc_rows_df.empty):
            print(" - has no EPC data")
        else:
            epc_rows_df = epc_rows_df.replace(',', '-', regex=True)
            epc_rows_df = epc_rows_df.replace('"', '', regex=True)
            epc_rows_df_string = epc_rows_df.to_csv(None, index=False)
            file_name_epc = 'igloo_epc_certificates' + '_' + postcode_sector.replace(' ', '') + '.csv'
            k.key = dir_s3['s3_epc_key']['EPCCertificatesRaw'] + file_name_epc
            # print(epc_rows_df_string)
            k.set_contents_from_string(epc_rows_df_string)

    '''Format Json to handle null values'''

    def format_json_response(self, data):
        data_str = json.dumps(data, indent=4).replace('null', '""')
        data_json = json.loads(data_str)
        return data_json

    def log_error(self, error_msg, error_code=''):
        logs_dir_path = sys.path[0] + '/logs/'
        if not os.path.exists(logs_dir_path):
            os.makedirs(logs_dir_path)
        with open(sys.path[0] + '/logs/' + 'igloo_epc_certificates_log_' + time.strftime('%d%m%Y') + '.csv',
                  mode='a') as errorlog:
            employee_writer = csv.writer(errorlog, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            employee_writer.writerow([error_msg, error_code])

    def processData(self, postcodes, k, _dir_s3):

        api_url, head = util.get_epc_api_info('historical_weather_api')

        for postcode in postcodes:
            t = con.api_config['total_no_of_calls']
            print('postcode:' + str(postcode))
            msg_ac = 'ac:' + str(postcode)
            self.log_error(msg_ac, '')
            # # api_url1 = api_url.format(postcode)
            # #   print(api_url1)
            # epc_data_response = self.get_api_response(api_url1, head)
            #
            # if epc_data_response:
            #     formatted_json = self.format_json_response(epc_data_response)
            #     self.extract_epc_data(formatted_json, postcode, k, _dir_s3)

    def get_weather_postcode(self, config_sql):
        pr = db.get_redshift_connection()
        postcodes_df = pr.redshift_to_pandas(config_sql)
        db.close_redshift_connection()
        postcodes_list = postcodes_df['postcode'].values.tolist()

        return postcodes_list


if __name__ == "__main__":

    freeze_support()

    p = HistoricalWeather()

    dir_s3 = util.get_dir()
    bucket_name = dir_s3['s3_bucket']

    s3 = s3_con(bucket_name)

    weather_postcode_sql = con.test_config['weather_sql']
    weather_postcodes = p.get_weather_postcode(weather_postcode_sql)

    print(weather_postcodes)
    p.processData(weather_postcodes, s3, dir_s3)

    ####### Multiprocessing Starts #########
    # n = 24  # number of process to run in parallel
    # k = int(len(weather_postcodes) / n)  # get equal no of files for each process
    #
    # print(len(weather_postcodes))
    # print(k)
    #
    # processes = []
    # lv = 0
    # start = timeit.default_timer()
    #
    # for i in range(n + 1):
    #     p1 = HistoricalWeather()
    #     print(i)
    #     uv = i * k
    #     if i == n:
    #         t = multiprocessing.Process(target=p1.processData, args=(weather_postcodes[lv:], s3, dir_s3))
    #     else:
    #         t = multiprocessing.Process(target=p1.processData, args=(weather_postcodes[lv:uv], s3, dir_s3))
    #     lv = uv
    #
    #     processes.append(t)
    #
    # for p in processes:
    #     p.start()
    #     time.sleep(2)
    #
    # for process in processes:
    #     process.join()
    # ####### Multiprocessing Ends #########
    #
    # print("Process completed in " + str(timeit.default_timer() - start) + ' seconds')
