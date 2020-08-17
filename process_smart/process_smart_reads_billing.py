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
from pathlib import Path

import sys
import os

sys.path.append('..')

from conf import config as con
from common import utils as util
from connections.connect_db import get_boto_S3_Connections as s3_con
from connections import connect_db as db
from common import api_filters as apif


class SmartReadsBillings:
    max_calls = con.api_config['max_api_calls']
    rate = con.api_config['allowed_period_in_secs']

    def __init__(self):
        self.start_date = datetime.datetime.strptime('2018-01-01', '%Y-%m-%d').date()
        self.end_date = datetime.datetime.today().date()
        self.api_url, self.head, self.key = util.get_api_info(api="smart_reads_billing", header_type="json")
        self.num_days_per_api_calls = 7
        self.sql = apif.smart_reads_billing['daily']  # there is no need for a weekly run here


    def format_json_response(self, data):
        """
        This function replaces the null values in the json data to empty string.
        :param data: The json response returned from api
        :return: json data
        """
        data_str = json.dumps(data, indent=4).replace('null', '""')
        data_json = json.loads(data_str)
        return data_json

    def log_error(self, error_msg, error_code=''):
        logs_dir_path = sys.path[0] + '/logs/'
        if not os.path.exists(logs_dir_path):
            os.makedirs(logs_dir_path)
        with open(logs_dir_path + 'smart_reads_billing' + time.strftime('%d%m%Y') + '.csv',
                  mode='a') as errorlog:
            employee_writer = csv.writer(errorlog, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            employee_writer.writerow([error_msg, error_code])

    def post_api_response(self, api_url, body, head, query_string, auth):
        session = requests.Session()
        status_code = 0
        response_json = json.loads('{}')

        try:
            response = session.post(api_url, data=body, params=query_string, headers=head, auth=auth)
            response_json = json.loads(response.content.decode('utf-8'))
            status_code = response.status_code
        except ConnectionError:
            self.log_error('Unable to Connect')
            response_json = json.loads('{message: "Connection Error"}')

        return response_json, status_code

    @staticmethod
    def extract_data_response(data, filename, _param):
        data['setup'] = _param
        df = json_normalize(data)
        if df.empty:
            print(" - No Data")
        else:
            csv_filename = Path('results/' + filename + datetime.datetime.today().strftime("%y%m%d") + '.csv')
            if csv_filename.exists():
                df.to_csv(csv_filename, mode='a', index=False, header=False)
            else:
                df.to_csv(csv_filename, mode='w', index=False)

            print("df_string: {0}".format(df))


    def processAccounts(self, _df, k, _dir_s3):
        api_url_smart_reads, head_smart_reads = util.get_smart_read_billing_api_info('smart_reads_billing')

        for index, df in _df.iterrows():
            # Get SMart Reads Billing
            body = json.dumps({
                "manualMeterReadingId": 0,
                "accountId": df["accountid"],
                "meterReadingDateTime": df["meterreadingdatetime"],
                "meterType": df["metertype"],
                "meterPointNumber": df["meterpointnumber"],
                "meter": df["meter"],
                "register": df["register"],
                "reading": df["reading"],
                "source": df["source"],
                "createdBy": df["createdby"],
                "dateCreated": str(datetime.datetime.now())
            })

            response_smart_reads = self.post_api_response(api_url_smart_reads, body, head_smart_reads)
            # print(account_elec_response)

            if response_smart_reads:
                formated_response_smart_reads = self.format_json_response(response_smart_reads)
                print(formated_response_smart_reads)
            else:
                print('ac:' + str(df['account_id']) + ' has no data for Elec status')
                msg_ac = 'ac:' + str(df['account_id']) + ' has no data for Elec status'
                # self.log_error(msg_ac, '')
                # self.log_error(msg_ac, '')

    def smart_reads_billing_details(self, config_sql):
        pr = db.get_redshift_connection()
        smart_reads_get_df = pr.redshift_to_pandas(config_sql)
        db.close_redshift_connection()
        # addresses_list = addresses_df.values.tolist()

        return smart_reads_get_df


if __name__ == "__main__":

    start = timeit.default_timer()
    freeze_support()
    p = SmartReadsBillings()

    dir_s3 = util.get_dir()
    bucket_name = dir_s3['s3_bucket']

    s3 = s3_con(bucket_name)

    smart_reads_billing_sql = p.sql
    smart_reads_billing_df = p.smart_reads_billing_details(smart_reads_billing_sql)

    p.processAccounts(smart_reads_billing_df, s3, dir_s3)

    print(len(smart_reads_billing_df))

    print("Process completed in " + str(timeit.default_timer() - start) + ' seconds')
