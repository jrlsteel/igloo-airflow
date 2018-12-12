import requests
import platform
import sys
from connections import connect_db as db

sys.path.append('..')

from conf import config as con
from common import directories as dirs3


def get_env():
    env_conf = con.environment_config['environment']
    return env_conf


def get_dir():

    dir = ''
    env_conf = get_env()
    if env_conf == 'uat':
        dir = dirs3.uat
    if env_conf == 'prod':
        dir = dirs3.prod

    return dir


def get_Users_from_s3(k):
    # global k
    filename_Users = 'users.csv'
    k.key = 'ensek-meterpoints/Users/' + filename_Users
    k.open()
    l = k.read()
    s = l.decode('utf-8')
    p = s.splitlines()
    # print(len(p))
    return p


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

    env = get_dir()
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

    dir = get_dir()

    env_api = dir['apis'][api]
    api_url = env_api['api_url']

    if api in ['internal_estimates', 'internal_readings']:
        token = get_auth_code()
    else:
        token = dir['apis']['token']

    head = {'Content-Type': 'application/json',
            'Authorization': 'Bearer {0}'.format(token)}
    return api_url, head


def get_epc_api_info(api):

    dir = get_dir()

    env_api = dir['apis'][api]
    api_url = env_api['api_url']

    token = env_api['token']

    head = {'Content-Type': 'application/json',
            'authorization': 'Basic {0}'.format(token),
            'accept': 'application/json'}
    return api_url, head


def get_weather_url_token(api):
    dir = get_dir()

    env_api = dir['apis'][api]
    api_url = env_api['api_url']

    token = env_api['token']

    return api_url, token


def get_api_info(api=None, auth_type=None, token_required=False, header_type=None):
    dir = get_dir()
    env_api = dir['apis'][api]
    api_url = env_api['api_url']

    token = ''
    if token_required:
        token = env_api['token']

    head = {}
    if auth_type == 'basic':
        head['authorization'] = 'Basic {0}'.format(token)

    if auth_type == 'bearer':
        head['authorization'] = 'Bearer {0}'.format(token)

    if header_type == 'json':
        head['accept'] = 'application/json'

    return api_url, head, token


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


def get_pythonAlias():
    if platform.system() == 'Windows':
        pythonAlias = 'python'
    else:
        pythonAlias = 'python3'

    return pythonAlias
