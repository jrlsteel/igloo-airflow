import requests

import sys
from connections import connect_db as db

sys.path.append('..')

from conf import config as con
from common import directories as dirs3


def get_environment():

    env = ''
    env_conf = con.environment_config['environment']
    if env_conf == 'uat':
        env = dirs3.uat
    if env_conf == 'prod':
        env = dirs3.prod

    return env


def get_accountID_fromDB(get_max):

    conn = db.get_rds_connection()
    cur = conn.cursor()
    cur.execute(con.test_config['account_ids_sql'])
    account_ids = [row[0] for row in cur]

    # logic to get max external id and process all the id within them
    if get_max:
        account_ids = list(range(1, max(account_ids)+1))
        # account_ids = list(range(max(account_ids)-10, max(account_ids)+1))

    db.close_rds_connection(cur, conn)

    return account_ids


def get_ensek_api_info(api, account_id):

    env = get_environment()

    env_api = env['apis'][api]
    api_url = env_api['api_url'].format(account_id)

    if api in ['internal_estimates', 'internal_readings']:
        token = get_auth_code()
    else:
        token = env['apis']['token']

    head = {'Content-Type': 'application/json',
            'Authorization': 'Bearer {0}'.format(token)}
    return api_url, head


def get_ensek_api_info1(api):

    env = get_environment()

    env_api = env['apis'][api]
    api_url = env_api['api_url']

    if api in ['internal_estimates', 'internal_readings']:
        token = get_auth_code()
    else:
        token = env['apis']['token']

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

