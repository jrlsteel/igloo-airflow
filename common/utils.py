import requests
import platform
import sys
import pandas as pd
import datetime
from connections import connect_db as db
import uuid
import os

sys.path.append('..')

from conf import config as con
from common import directories as dirs3
from common import api_filters as apif


def get_env():
    env_conf = con.environment_config['environment']
    return env_conf


def get_multiprocess(source):
    env_total_processes = con.multi_process[source]
    return env_total_processes

def get_dir():
    dir = ''
    env_conf = get_env()
    if env_conf == 'dev':
        dir = dirs3.dev
    if env_conf == 'uat':
        dir = dirs3.uat
    if env_conf == 'prod':
        dir = dirs3.prod
    if env_conf == 'preprod':
        dir = dirs3.preprod

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


def batch_logging_insert(id, job_id, job_name, job_script_name):
    '''
    Batch Logging Function
    '''
    job_start = datetime.datetime.now()
    job_end = None
    job_status = 'Running'
    job_df = pd.DataFrame(data=[[id, job_id, job_name, job_script_name, job_start, job_end, '', job_status]],
                          columns=['id', 'job_id', 'job_name', 'job_script_name', 'job_start', 'job_end',
                                   'job_error_message', 'job_status'])
    batch_logging = redshift_upsert(df=job_df, crud_type='i')
    return batch_logging


def batch_logging_update(id, update_type=None, error_message=None):
    """
    :param id:
    :param update_type: 's' - Start time, 'e' - End time, 'f' - Job Failed
    :param error_message
    :return: None
    """
    time = datetime.datetime.now()
    job_updates = {
        's': {'time': time,
              'status': 'Running'
              },
        'e': {
            'time': time,
            'status': 'Done'
        },
        'f': {
            'time': time,
            'status': 'Failed'
        },
    }

    job_time = job_updates[update_type]['time']
    job_status = job_updates[update_type]['status']

    sql_update_f = ''
    if update_type == 's':
        sql_update = """update dwh_job_logs set job_start = '{0}', job_status = '{1}', job_error_message = '{2}' where id = '{3}'"""
        sql_update_f = sql_update.format(job_time, job_status, error_message, id)
        redshift_upsert(sql_update_f, crud_type='u')

    if update_type in ('e', 'f'):
        sql_update = """update dwh_job_logs set job_end = '{0}', job_status = '{1}', job_error_message = '{2}' where id = '{3}'"""
        sql_update_f = sql_update.format(job_time, job_status, error_message, id)
        redshift_upsert(sql_update_f, crud_type='u')


def redshift_upsert(sql=None, df=None, crud_type=None):
    '''
    :param sql: the sql to run
    '''
    try:
        table_name = 'dwh_job_logs'
        pr = db.get_redshift_connection()
        if crud_type == 'i':
            pr.pandas_to_redshift(df, table_name, index=None, append=True)

        if crud_type in ('u', 'd'):
            pr.exec_commit(sql)
        pr.close_up_shop()

    except Exception as e:
        return e


def execute_query(sql, return_as='d'):
    '''
    :param sql: The query to execute
    :param return_as: User can mention the return type as list (l) or dataframe (d - default)
    :return:
    '''
    env_conf = get_env()

    # Limit for UAT environments
    if env_conf == 'prod':
        sql = sql
    else:
        sql = sql + ' limit 200'

    pr = db.get_redshift_connection()
    df = pr.redshift_to_pandas(sql)
    db.close_redshift_connection()

    df_list = []
    if return_as == 'l':
        df_list = df.values.tolist()
        return df_list

    return df


def get_accountID_fromDB(get_max, filter='live'):
    env_conf = get_env()

    if filter == 'live':
        sql_group = apif.account_ids
    elif filter == 'pre-live':
        sql_group = apif.pending_acc_ids
    elif filter == 'tariff-diffs':
        sql_group = apif.tariff_diff_acc_ids
    elif filter == 'land-registry':
        sql_group = apif.land_registry_postcodes
    elif filter == 'historical-weather':
        sql_group = apif.weather_postcodes
    else:
        sql_group = {}

    # Sunday chosen as the major reports are utilised on monday and should be near up to date as possible.
    if datetime.date.today().weekday() == 6:  # 6 == Sunday
        if env_conf == 'prod':
            config_sql = sql_group['weekly']
        else:
            config_sql = sql_group['weekly'] + ' limit 200'
    else:
        if env_conf == 'prod':
            config_sql = sql_group['daily']
        else:
            config_sql = sql_group['daily'] + ' limit 200'

    account_ids = []
    if env_conf == 'prod':
        rd_conn = db.get_redshift_connection_prod()
        # config_sql = con.test_config['account_ids_sql_prod']
        account_id_df = rd_conn.redshift_to_pandas(config_sql)
        db.close_redshift_connection()
        account_id_list = account_id_df.values.tolist()
        account_id_list1 = [row[0] for row in account_id_list]
        # logic to get max external id and process all the id within them
        if get_max:
            account_ids = list(range(1, max(account_id_list1) + 1))
            # account_ids = list(range(max(account_ids)-10, max(account_ids)+1))
        else:
            account_ids = account_id_list1
    else:
        rd_conn = db.get_redshift_connection_prod()
        # config_sql = con.test_config['account_ids_sql_prod']
        account_id_df = rd_conn.redshift_to_pandas(config_sql)
        db.close_redshift_connection()
        account_id_list = account_id_df.values.tolist()
        #print(account_id_df.values[0])
        account_id_list1 = [row[0] for row in account_id_list]
        #print(account_id_list[0])
        # logic to get max external id and process all the id within them
        if get_max:
            account_ids = list(range(1, max(account_id_list1) + 1))
            # account_ids = list(range(max(account_ids)-10, max(account_ids)+1))
        else:
            account_ids = account_id_list1
    return account_ids


def get_ensek_api_info(api, account_id):
    env = get_dir()
    env_api = env['apis'][api]
    api_url = env_api['api_url'].format(account_id)

    if api in ['internal_estimates', 'internal_readings', 'internal_psr']:
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

    if api in ['internal_estimates', 'internal_readings', 'occupier_accounts']:
        token = get_auth_code()
    else:
        token = dir['apis']['token']

    head = {'Content-Type': 'application/json',
            'Authorization': 'Bearer {0}'.format(token)}
    return api_url, head


def get_smart_read_billing_api_info(api):
    dir = get_dir()

    env_api = dir['apis'][api]
    api_url = env_api['api_url']

    api_key = dir['apis']['api_key']
    host = dir['apis']['host']

    head = {'Content-Type': 'application/json',
            'Host':'{0}'.format(host),
            'x-api-key':'{0}'.format(api_key)}
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


def get_epc_api_info_full(api):
    dir = get_dir()

    env_api = dir['apis'][api]
    api_url = env_api['api_url']

    file_url = env_api['file_url']

    return api_url, file_url


def get_gas_historical_wcf_api_info(api):
    dir = get_dir()

    env_api = dir['apis'][api]
    api_url = env_api['api_url']
    head = {'Content-Type': 'application/soap+xml',
            'charset': 'utf-8'}

    return api_url, head


def get_gas_historical_cv_api_info(api):
    dir = get_dir()

    env_api = dir['apis'][api]
    api_url = env_api['api_url']
    head = {'Content-Type': 'application/soap+xml',
            'charset': 'utf-8'}

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


def get_jobID():
    jobid = uuid.uuid4()
    return jobid


def get_files_from_sftp(sftp_path, search_string=''):
    """
    :return: files from sftp for the specified directory
    """
    sftp_conn = None
    sftp_files = []
    try:
        sftp_conn = db.get_ensek_sftp_connection()  # get sftp connection

        if sftp_conn.exists(sftp_path):

            sftp_files = sftp_conn.listdir(sftp_path)
            if search_string:
                print(type(sftp_files))
                sftp_files = [x for x in sftp_files if search_string in x]
        else:
            print('Error : SFTP path does not exists')

    except Exception as e:
        print('Error :' + str(e))
        sys.exit(1)

    finally:
        sftp_conn.close()

    return sftp_files


def archive_files_on_sftp(files, sftp_path, archive_path):
    sftp_conn = None
    try:
        sftp_conn = db.get_ensek_sftp_connection()  # get sftp connection

        if sftp_conn.exists(sftp_path):
            for file in files:
                if file != 'Archive':
                    src_file = sftp_path + '/' + file
                    tgt_file = archive_path + '/' + file
                    sftp_conn.rename(src_file, tgt_file)

    except Exception as e:
        print('Error : ' + str(e))

    finally:
        sftp_conn.close()


def get_keys_from_s3(s3, bucket_name, prefix, suffix):
    """
    This function gets all the nrl_keys that needs to be processed.
    :param s3: holds the s3 connection
    :param bucket_name: s3 bucket name
    :param suffix: suffix of file ex: .csv, .flw
    :param prefix: directory path of the file exists ex: bucket/stage1/nrl/
    :return: list of filenames under the s3 path
    """

    s3_keys = []
    for obj in s3.list_objects(Bucket=bucket_name, Prefix=prefix)['Contents']:
        if obj['Key'].endswith(suffix):
            s3_keys.append(obj['Key'])
    return s3_keys
