import timeit
import pandas as pd
import requests
import json
from pandas.io.json import json_normalize
import time
from requests import ConnectionError
import csv
from multiprocessing import freeze_support
import pandas_redshift as pr
from aws_requests_auth.aws_auth import AWSRequestsAuth
import urllib.parse
import datetime

import sys
import os
from pathlib import Path

#sys.path.append('..')

from process_Ensek.processEnsekPriorityServiceRegister.conf import config as con
from process_Ensek.processEnsekPriorityServiceRegister.connections import connect_db as db


class TstApi:

    #def __init__(self):
    #    pass

    def get_connection_pr(self, env):
        try:
            pr.connect_to_redshift(host=env['redshift_config']['host'], port=env['redshift_config']['port'],
                                   user=env['redshift_config']['user'], password=env['redshift_config']['pwd'],
                                   dbname=env['redshift_config']['db'])
            print("Connected to Redshift")

            pr.connect_to_s3(aws_access_key_id=env['s3_config']['access_key'],
                             aws_secret_access_key=env['s3_config']['secret_key'],
                             bucket=env['s3_config']['bucket_name'],
                             subdirectory='aws-glue-tempdir/')
        except Exception as e:
            raise e

    def get_api_response(self, api_url, head, query_string, auth):
        '''
            get the response for the respective url that is passed as part of this function
        '''
        session = requests.Session()
        start_time = time.time()
        timeout = 5
        retry_in_secs = 2
        i = 0
        # print("trying now...")
        while True:
            try:
                response = session.get(api_url, params=query_string, headers=head, auth=auth, )
                # print(response.content)
                # print(response.encoding)
                # print(response.text)
                if response.status_code == 200:
                    response_json = json.loads(response.content.decode('utf-8'))
                    return response_json
                else:
                    # print(response.text)
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

    def extract_data_response(self, data, filename, k, _param):
        ''' Processing meter points data'''
        df = json_normalize(data)
        # print(df_internal_readings)
        if df.empty:
            print(" - No Data")
        else:
            df.insert(0, "account_id", k)
            csv_filename = Path(filename + datetime.datetime.today().strftime("%y%m%d") + '.csv')
            if csv_filename.exists():
                df_string = df.to_csv(csv_filename, mode='a', index=False, header=False)
            else:
                df_string = df.to_csv(csv_filename, mode='w', index=False)

            # print(df_string)

    def format_json_response(self, data):
        data_str = json.dumps(data, indent=4).replace('null', '""')
        data_json = json.loads(data_str)
        return data_json

    def log_error(self, error_msg, error_code=''):
        logs_dir_path = sys.path[0] + '/logs/'
        if not os.path.exists(logs_dir_path):
            os.makedirs(logs_dir_path)
        with open(sys.path[0] + '/logs/' + 'test' + time.strftime('%d%m%Y') + '.csv',
                  mode='a') as errorlog:
            employee_writer = csv.writer(errorlog, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            employee_writer.writerow([error_msg, error_code])

    def test_tado(self, account_ids, k, env, compare_demand_batch):
        api_url = "https://6rsh2pqob0.execute-api.eu-west-1.amazonaws.com/prod/Accounts/{0}/tado"

        # last_updated = "2019-04-20 14:10:10"

        headers = {
            'Content-Type': "application/json",
            'x-api-key': env['aws_api_gateway']['x-api-key']
        }

        auth = AWSRequestsAuth(aws_access_key=env['aws_api_gateway']['access_key'],
                               aws_secret_access_key=env['aws_api_gateway']['secret_key'],
                               aws_token="",
                               aws_host='6rsh2pqob0.execute-api.eu-west-1.amazonaws.com',
                               aws_region='eu-west-1',
                               aws_service='execute-api'
                               )
        # to check if ondemand and batch returns same value.
        params = ["true", "false"]
        for account_id in account_ids:
            for param in params:
                # encoded_qs = urllib.parse.quote(param)
                querystring = ''
                if compare_demand_batch:
                    querystring = {"demand": param}

                print('ac:' + str(account_id))
                msg_ac = 'ac:' + str(account_id)
                self.log_error(msg_ac, '')
                api_url1 = api_url.format(account_id)
                api_response = self.get_api_response(api_url1, headers, querystring, auth)
                # print(json.dumps(internal_data_response, indent=4))
                if api_response:
                    # formatted_api_response = self.format_json_response(api_response)
                    # print(formatted_api_response)
                    self.extract_data_response(api_response, 'tado', k, param)
                else:
                    print(api_response)

    def test_eac_aq(self, account_ids, k, env, compare_demand_batch):
        api_url = "https://6rsh2pqob0.execute-api.eu-west-1.amazonaws.com/prod/Accounts/{0}/consumption/gas"

        # last_updated = "2019-04-20 14:10:10"

        headers = {
            'Content-Type': "application/json",
            'x-api-key': env['aws_api_gateway']['x-api-key']
        }

        auth = AWSRequestsAuth(aws_access_key=env['aws_api_gateway']['access_key'],
                               aws_secret_access_key=env['aws_api_gateway']['secret_key'],
                               aws_token="",
                               aws_host='6rsh2pqob0.execute-api.eu-west-1.amazonaws.com',
                               aws_region='eu-west-1',
                               aws_service='execute-api'
                               )

        # to check if ondemand and batch returns same value.
        params = ["false"]
        for account_id in account_ids:
            for param in params:
                # encoded_qs = urllib.parse.quote(param)
                querystring = ''
                if compare_demand_batch:
                    querystring = {"demand": param}

                print('ac:' + str(account_id))
                msg_ac = 'ac:' + str(account_id)
                self.log_error(msg_ac, '')
                api_url1 = api_url.format(account_id)
                api_response = self.get_api_response(api_url1, headers, querystring, auth)
                # print(json.dumps(internal_data_response, indent=4))
                if api_response:
                    formatted_api_response = self.format_json_response(api_response)
                    print(formatted_api_response)
                    self.extract_data_response(formatted_api_response, account_id, k, param)
                else:
                    print(api_response)

    def test_psr(self, account_ids_local, k, env, compare_demand_batch):
        api_url = "https://igloo.ignition.ensek.co.uk/api/GetPSRByAccount/{0}"

        # last_updated = "2019-04-20 14:10:10"

        headers = {
            'Accept': "application/json",
            'Authorization': "Bearer SfmnUF7k2HVSbFdbExYEgzmVcurepYsrQ4oV766GJVGQiB_fXstV8kbt2-smH3xuqWpTvvMPYnHDNjNb_Rfk88xL9V2ujLACQwsPZfaVaBw5vaVAZGNFja53zw4jnfqAaZchvFKk0iqNn_ph3hOKSzCCQFUwOsPqOBlEoebI3xmXNTTUgCM43pR20fotgkqUFBC_wwntW7G-X8PWcp2KZGGRrN-yx0in5xJx213CBApb2BqFh6rCqfhLwfFJi0Fk_JJ033nlHPRXX7DAidpUfpb_scao_l-4rkInBPW6xfqNuUT1phExJhlnJXMRNc8x1XKyOD_ge0V_xwG-YEYUWOp000P0T08o4b_5A7ME_YrU2g72ROmGAOoPHKT1XhniB6NQmLXYKS6wBd1pkp48uHhkRRYVI3MV-lOS_LKNzORFrh7jzSa3CfNBorEbaSeSRVq8TaNQR9NCFed-_pY8uzhnvSaLz6klERhiwrZ2aORhvvWEX-o-ZJ11E39Cs6hgLwvVZmQkatib0nvzgPNagWtQizwaHs0qfS3P6ZHsBCC-90P-JeF-mCNT3lpOxiURGjcMLfCu7-Navm--u9DvhXYDpgJJT6nAZuEX5XJhKZ66qGZBSnybZzzTOFeM4joLv5spdu0FkbY6HwmFYFzKgMpMZHoA0AaiXm4DhJq3TPDiaNVMYiPYiDGayK8FaL0uxC88bA",
            'Content-Type': "application/json"
        }

        # res = pd.DataFrame(columns=['account_id', 'psr_api_response'])

        for account_id in account_ids_local:
            if account_id > 43053:
                # logging
                print('ac:' + str(account_id))
                # perform api call
                api_url_full = api_url.format(account_id)
                api_response = self.get_api_response(api_url_full, headers, '', '')
                #print(json.dumps(internal_data_response, indent=4))
                if api_response:
                    formatted_api_response = self.format_json_response(api_response)
                    str_api_response = json.dumps(api_response, indent=4).replace('null', '""')
                    # res.loc[len(res)] = [account_id, formatted_api_response]
                    print("ac: " + str(account_id) + ", response: " + str_api_response)
                    self.extract_data_response(formatted_api_response, "psr", account_id, 0)

        # return res

    def get_account_ids(self):
        #sql = "select external_id from ref_cdb_supply_contracts where external_id in (1831,4601,38081,18159,18908,30087,15899,33000,22526,36211,41701,45996,43682,30407,31005,42503,41906,32096)"
        # sql = "select external_id from ref_cdb_supply_contracts where external_id in (1831,4601)"
        sql = "select external_id from ref_cdb_supply_contracts"
        account_id_df = pr.redshift_to_pandas(sql)
        pr.close_up_shop()
        account_id_list = account_id_df.values.tolist()
        account_id_list1 = [row[0] for row in account_id_list]
        return account_id_list1


if __name__ == "__main__":
    freeze_support()

    # env = con.prod

    # get redshift and rds connection
    # bucket_name = env['s3_config']['bucket_name']
    # s3 = db.get_boto_S3_Connections(env, bucket_name)

    p1 = TstApi()

    # p1.get_connection_pr(env)

    # account_ids = p1.get_account_ids()

    acc_id_df = pd.read_csv('live_ids_190718.csv')
    account_ids = acc_id_df['account_id']

    # account_ids = range(8334, 52361)

    print(len(account_ids))
    print(int(len(account_ids) / 12))
    p = int(len(account_ids) / 12)

    compare_demand_batch = True

    # Enable to test eac_aq
    # p1.test_eac_aq(account_ids, s3, env, compare_demand_batch)

    # Enable to test tado
    p1.test_psr(account_ids, [], [], [])

    # ####### Multiprocessing Starts #########
    # env = util.get_env()
    # if env == 'uat':
    #     n = 12  # number of process to run in parallel
    # else:
    #     n = 24
    #
    # k = int(len(account_ids) / n)  # get equal no of files for each process
    #
    # print(len(account_ids))
    # print(k)
    #
    # processes = []
    # lv = 0
    # start = timeit.default_timer()
    #
    # for i in range(n + 1):
    #     p1 = TestApi()
    #     print(i)
    #     uv = i * k
    #     if i == n:
    #         # print(d18_keys_s3[l:])
    #         t = multiprocessing.Process(target=p1.processAccounts, args=(account_ids[lv:], s3, dir_s3))
    #     else:
    #         # print(d18_keys_s3[l:u])
    #         t = multiprocessing.Process(target=p1.processAccounts, args=(account_ids[lv:uv], s3, dir_s3))
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