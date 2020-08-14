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
from common import api_filters as apif


class SmartReadsBillings:
    max_calls = con.api_config['max_api_calls']
    rate = con.api_config['allowed_period_in_secs']

    def __init__(self):
        self.start_date = datetime.datetime.strptime('2018-01-01', '%Y-%m-%d').date()
        self.end_date = datetime.datetime.today().date()
        self.api_url, self.head, self.key = util.get_api_info(api="land_registry", header_type="json")
        self.num_days_per_api_calls = 7
        self.sql = apif.smart_reads_billing['daily']  # there is no need for a weekly run here

    def get_params(self, address):

        params = {}
        street = address['thoroughfare']

        # add a space into the postcode
        # ARE ALL POSTCODES X + 3 digits?
        postcode = address['postcode'][:-3] + ' ' + address['postcode'][-3:]

        # building number
        paon = address['building_name_number']

        # get any sub number e.g. address
        saon = address['sub_building_name_number']

        if street:
            params['propertyAddress.street'] = street.upper()

        if postcode:
            params['propertyAddress.postcode'] = postcode

        if paon:
            params['propertyAddress.paon'] = paon

        if saon:
            params['propertyAddress.saon'] = saon

        return params


    def get_api_response(self, api_url, head, query_string, auth):
        session = requests.Session()
        status_code = 0
        response_json = json.loads('{}')

        try:
            response = session.get(api_url, params=query_string, headers=head, auth=auth)
            response_json = json.loads(response.content.decode('utf-8'))
            status_code = response.status_code
        except ConnectionError:
            self.log_error('Unable to Connect')
            response_json = json.loads('{message: "Connection Error"}')

        return response_json, status_code

    def extract_land_registry_data(self, data, address, k, dir_s3):
        meta_landreg = ['transactionDate', 'newBuild', 'pricePaid', 'transactionId']
        land_registry_df = json_normalize(data)

        if land_registry_df.empty:
            print(" - has no land registry data")
        else:
            land_registry_df1 = land_registry_df[meta_landreg]
            land_registry_df1._is_copy = False
            land_registry_df1['propertyType'] = land_registry_df['propertyType.prefLabel'][0][0]['_value']
            land_registry_df1['recordStatus'] = land_registry_df['recordStatus.prefLabel'][0][0]['_value']
            land_registry_df1['transactionCategory'] = land_registry_df['transactionCategory.prefLabel'][0][0]['_value']

            land_registry_df1['uprn'] = address['uprn']
            land_registry_df1['id'] = address['id']

            land_registry_string = land_registry_df1.to_csv(None, index=False)
            file_name_landreg = 'land_registry_' + str(address['id']).strip() + '.csv'
            k.key = dir_s3['s3_land_reg_key']['LandRegistry'] + file_name_landreg
            k.set_contents_from_string(land_registry_string)

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
        with open(logs_dir_path + 'land_registry_log' + time.strftime('%d%m%Y') + '.csv',
                  mode='a') as errorlog:
            employee_writer = csv.writer(errorlog, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            employee_writer.writerow([error_msg, error_code])

    def processData(self, addresses, k, _dir_s3):

        for index, address in addresses.iterrows():
            print('addres_id:' + str(address['id']))
            msg_ac = 'ac:' + str(address['id'])
            self.log_error(msg_ac, '')

            api_response = self.get_api_response(address)

            if api_response:
                formatted_json = self.format_json_response(api_response)
                # print(formatted_json)
                self.extract_land_registry_data(formatted_json, address, k, _dir_s3)

    def smart_reads_billing_details(self, config_sql):
        pr = db.get_redshift_connection()
        addresses_df = pr.redshift_to_pandas(config_sql)
        db.close_redshift_connection()
        # addresses_list = addresses_df.values.tolist()

        return addresses_df


if __name__ == "__main__":

    freeze_support()

    p = SmartReadsBillings()

    dir_s3 = util.get_dir()
    bucket_name = dir_s3['s3_bucket']

    s3 = s3_con(bucket_name)

    smart_reads_billing_sql = p.sql
    smart_reads_billing_df = p.smart_reads_billing_details(smart_reads_billing_sql)

    # print(weather_postcodes)
    # if False:
    #     p.processData(addresses_df, s3, dir_s3)

    print(len(smart_reads_billing_df))

    start = timeit.default_timer()

    print("Process completed in " + str(timeit.default_timer() - start) + ' seconds')
