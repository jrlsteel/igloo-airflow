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

import sys
import os

sys.path.append('..')

from conf import config as con
from common import utils as util
from connections.connect_db import get_boto_S3_Connections as s3_con
from connections import connect_db as db
# import pandas_redshift as pr

class IglooEPCCertificates:
    max_calls = con.api_config['max_api_calls']
    rate = con.api_config['allowed_period_in_secs']

    def __init__(self):
        pass


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
        if epc_rows_df.empty:
            print(" - has no EPC data")
        else:
            epc_rows_df.columns = epc_rows_df.columns.str.replace('-', '_')
            epc_rows_df = epc_rows_df.replace(',', '-', regex=True)
            epc_rows_df = epc_rows_df.replace('"', '', regex=True)
            column_list = util.get_common_info('epc_column_order', 'epc_certificates')
            epc_rows_df_string = epc_rows_df.to_csv(None, columns=column_list, index=False)
            file_name_epc = 'igloo_epc_certificates' + '_' + postcode_sector.replace(' ', '') + '.csv'
            k.key = dir_s3['s3_epc_key']['EPCCertificates'] + file_name_epc
            # print(epc_rows_df_string)
            k.set_contents_from_string(epc_rows_df_string)

    '''Format Json to handle null values'''

    def format_json_response(self, data):
        data_null = json.dumps(data, indent=4).replace('(null)', '')
        # data_str = json.dumps(data, indent=4).replace('null', '""')
        data_str = data_null.replace('null', '""')
        data_json = json.loads(data_str)
        return data_json

    def log_error(self, error_msg, error_code=''):
        logs_dir_path = sys.path[0] + '/logs/'
        if not os.path.exists(logs_dir_path):
            os.makedirs(logs_dir_path)
        with open(sys.path[0] + '/logs/' + 'igloo_epc_certificates_log_' + time.strftime('%d%m%Y') + '.csv', mode='a') as errorlog:
            employee_writer = csv.writer(errorlog, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            employee_writer.writerow([error_msg, error_code])

    def processAccounts(self, postcode_sectors, k, _dir_s3):

        try:
            api_url, head = util.get_epc_api_info('igloo_epc_certificates')
            for postcode_sector in postcode_sectors:
                t = con.api_config['total_no_of_calls']
                print('postcode:' + str(postcode_sector))
                msg_ac = 'ac:' + str(postcode_sector)
                self.log_error(msg_ac, '')
                api_url1 = api_url.format(postcode_sector)
                # print(api_url1)
                epc_data_response = self.get_api_response(api_url1, head)
                # print(epc_data_response)
                if epc_data_response:
                    formatted_json = self.format_json_response(epc_data_response)
                    self.extract_epc_data(formatted_json, postcode_sector, k, _dir_s3)
        except Exception as e:
            print("Exception in EPC Certificates" + str(e))

    def get_epc_postcode(self, config_sql):
        pr = db.get_redshift_connection()
        postcode_sectors_df = pr.redshift_to_pandas(config_sql)
        db.close_redshift_connection()
        postcode_sectors_list = postcode_sectors_df['postcode'].values.tolist()

        return postcode_sectors_list


if __name__ == "__main__":

    freeze_support()

    p = IglooEPCCertificates()

    dir_s3 = util.get_dir()
    bucket_name = dir_s3['s3_bucket']

    s3 = s3_con(bucket_name)

    postcode_sector_sql = con.test_config['epc_certificates_postcode_sql']
    postcode_sectors = p.get_epc_postcode(postcode_sector_sql)

    # p.processAccounts(postcode_sectors, s3, dir_s3)
    ####### Multiprocessing Starts #########

    env = util.get_env()
    if env == 'uat':
        n = 12  # number of process to run in parallel
    else:
        n = 12

    k = int(len(postcode_sectors) / n)  # get equal no of files for each process

    print(len(postcode_sectors))
    print(k)

    processes = []
    lv = 0
    start = timeit.default_timer()

    for i in range(n + 1):
        p1 = IglooEPCCertificates()
        print(i)
        uv = i * k
        if i == n:
            t = multiprocessing.Process(target=p1.processAccounts, args=(postcode_sectors[lv:], s3_con(bucket_name), dir_s3))
        else:
            t = multiprocessing.Process(target=p1.processAccounts, args=(postcode_sectors[lv:uv], s3_con(bucket_name), dir_s3))
        lv = uv

        processes.append(t)

    for p in processes:
        p.start()
        time.sleep(2)

    for process in processes:
        process.join()
    ####### Multiprocessing Ends #########

    print("Process completed in " + str(timeit.default_timer() - start) + ' seconds')

