import csv
import requests
import json
from json_schema import json_schema
import pymysql
import time
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from conf import config as con


def get_api_info(account_id, api):

    global env
    env_conf = con.environment_config['environment']
    if env_conf == 'uat':
        env = con.uat
    if env_conf == 'prod':
        env = con.prod

    env_api = env[api]
    api_url = env_api['api_url'].format(account_id)

    if api in ['internal_estimates', 'internal_readings']:
        token = get_auth_code()
    else:
        token = env['token']

    head = {'Content-Type': 'application/json',
            'Authorization': 'Bearer {0}'.format(token)}
    return api_url, head


def get_auth_code():
    oauth_url = 'https://igloo.ignition.ensek.co.uk/api/Token'
    data = {
            'username': con.internalapi_config['username'],
            'password': con.internalapi_config['password'],
            'grant_type': con.internalapi_config['grant_type']
    }

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'application/json',
        'Referrer': 'https: // igloo.ignition.ensek.co.uk'
    }
    response = requests.post(oauth_url, data=data, headers=headers)
    response = response.json()
    return response.get('access_token')


def get_api_response(api_url, head):
    """
        get the response for the respective url that is passed as part of this function
    """

    response = requests.get(api_url, headers=head)

    if response.status_code == 200:
        response = json.loads(response.content.decode('utf-8'))
        return response
    else:
        print('Problem Grabbing Data: ', response.status_code)
        # log_error('Response Error: Problem grabbing data', response.status_code)


def get_api_response_pages(api_url, head):
    '''
        get the response for the respective url that is passed as part of this function
    '''
    session = requests.Session()

    response = session.post(api_url, headers=head, params={'page': 1})

    if response.status_code == 200:
        response_json = json.loads(response.content.decode('utf-8'))
        total_pages = response_json['totalPages']
        response_items = response_json['items']

        for page in range(2, total_pages + 1):
            response_next_page = session.post(api_url, headers=head, params={'page': page})
            response_next_page_json = json.loads(response_next_page.content.decode('utf-8'))['items']
            response_items.extend(response_next_page_json)
        return response_items
    else:
        print('Problem Grabbing Data: ', response.status_code)


def format_json_response(data, api):

    data_str = json.dumps(data, indent=4).replace('null', '""')

    if api == 'tariff_history':
        for k in data:
            if k['Gas'] is None:
                # print(data_str.replace('"Gas": ""', '"Gas": {"unitRates": [], "standingChargeRates": []}'))
                data_str = data_str.replace('"Gas": ""', '"Gas": {"unitRates": [], "standingChargeRates": []}')

    data_json = json.loads(data_str)
    return data_json


def log_error(error_msg, error_code=''):
    with open(sys.path[0] + '/logs/' + 'ensek_schema_error_' + time.strftime('%d%m%Y') + '.csv', mode='a') as errorlog:
        employee_writer = csv.writer(errorlog, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        employee_writer.writerow([error_msg, error_code])


def get_accountID_fromDB():
    conn = pymysql.connect(host=con.rds_config['host'], port=con.rds_config['port'], user=con.rds_config['user'],
                           passwd=con.rds_config['pwd'], db=con.rds_config['db'])

    cur = conn.cursor()

    cur.execute(con.test_config['schema_account_ids_sql'])

    account_ids = [row[0] for row in cur]
    cur.close()
    conn.close()

    return account_ids


def validateSchema(response_json, api, account_id):
    meterpoints_string = json.dumps(response_json, indent=4)
    filename = sys.path[0] + '/files/ensek_schema/' + api + '_schema' + '.json'
    with open(filename, 'r') as schemafile:
        meterpoints_schema = schemafile.read()

    js = json_schema.loads(meterpoints_schema)
    # print(json_schema.match(meterpoints_string, meterpoints_schema))

    if js == meterpoints_string:
        print('true')
        schema_valid = True
    else:
        print('false')
        schema_valid = False
        error_json = full_check(meterpoints_string, js)
        print(error_json)
        print(meterpoints_string)

        msg_error = time.strftime('%d-%m-%Y-%H:%M:%S') + " - " + api + ' api has invalid schema for account id ' + str(account_id) + "\n" + error_json
        log_error(msg_error, '')

    return schema_valid


def full_check(json_string, json_schema):
    my_json = json.loads(json_string)
    e = json_schema._comparar(my_json, json_schema.schema_dict)
    t = json.dumps(e, indent=4)
    t = t.replace("\\u001b[91m", "\033[91m").replace("\\u001b[92m", "\033[92m")

    error_json = "\033[92m%s\033[0m" % t

    return error_json


def processAccounts(account_id_s):
    apis = ['meterpoints', 'direct_debits', 'internal_estimates', 'internal_readings', 'account_status', 'elec_status', 'gas_status', 'tariff_history']
    schema_valid_response1 = []
    for api in apis:
        print(api)
        for account_id in account_id_s:
            api_url, head = get_api_info(account_id, api)

            if api in ['internal_readings']:
                api_response = get_api_response_pages(api_url, head)
            else:
                api_response = get_api_response(api_url, head)

            if api_response:
                print(account_id)
                # print(api_response)
                formatted_json_response = format_json_response(api_response, api)
                schema_valid_response = validateSchema(formatted_json_response, api, account_id)
                schema_valid_response1 += [{'api': api, 'account_id': account_id, 'valid': schema_valid_response}]
    return schema_valid_response1


if __name__ == "__main__":
    account_ids = []
    '''Enable this to test for 1 account id'''
    if con.test_config['enable_manual'] == 'Y':
        account_ids = con.test_config['account_ids']

    if con.test_config['enable_db'] == 'Y':
        account_ids = get_accountID_fromDB()

    schema_valid_response = processAccounts(account_ids)
    # processAccounts(account_ids)




