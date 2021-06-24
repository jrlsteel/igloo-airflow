import requests
import json
from json_schema import json_schema
import pymysql

import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from conf import config as con
from common import directories as dir

import boto3
from common.secrets_manager import get_secret

client = boto3.client("secretsmanager")


def get_meter_point_api_info(account_id, api):
    global env
    env_conf = con.environment_config["environment"]
    if env_conf == "uat":
        env = dir.uat
    if env_conf == "prod":
        env = dir.prod

    env_api = env["apis"][api]
    api_url = env_api["api_url"].format(account_id)

    if api in ["internal_estimates", "internal_readings"]:
        token = get_auth_code()
    else:
        token = env["apis"]["token"]

    head = {"Content-Type": "application/json", "Authorization": "Bearer {0}".format(token)}

    return api_url, head


def get_auth_code():
    internalapi_config = get_secret(client, con.internalapi_config["secret_id"])
    oauth_url = "https://igloo.ignition.ensek.co.uk/api/Token"
    data = {
        "username": internalapi_config["username"],
        "password": internalapi_config["password"],
        "grant_type": con.internalapi_config["grant_type"],
    }

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
        "Referrer": "https: // igloo.ignition.ensek.co.uk",
    }
    response = requests.post(oauth_url, data=data, headers=headers)
    response = response.json()
    return response.get("access_token")


def get_api_response(api_url, head):
    """
    get the response for the respective url that is passed as part of this function
    """

    response = requests.get(api_url, headers=head)

    if response.status_code == 200:
        response = json.loads(response.content.decode("utf-8"))
        return response
    else:
        print("Problem Grabbing Data: ", response.status_code)
        # log_error('Response Error: Problem grabbing data', response.status_code)


def get_api_response_pages(api_url, head):
    """
    get the response for the respective url that is passed as part of this function
    """
    session = requests.Session()

    response = session.post(api_url, headers=head, params={"page": 1})

    if response.status_code == 200:
        response_json = json.loads(response.content.decode("utf-8"))
        total_pages = response_json["totalPages"]
        response_items = response_json["items"]

        for page in range(2, total_pages + 1):
            response_next_page = session.post(api_url, headers=head, params={"page": page})
            response_next_page_json = json.loads(response_next_page.content.decode("utf-8"))["items"]
            response_items.extend(response_next_page_json)
        return response_items
    else:
        print("Problem Grabbing Data: ", response.status_code)


def get_accountID_fromDB():
    rds_config = get_secret(client, con.rds_config["secret_id"])
    conn = pymysql.connect(
        host=rds_config["host"],
        port=rds_config["port"],
        user=rds_config["username"],
        passwd=rds_config["password"],
        db=con.rds_config["db"],
    )

    cur = conn.cursor()

    cur.execute(con.test_config["schema_account_ids_sql_rds"])

    account_ids = [row[0] for row in cur]
    cur.close()
    conn.close()

    return account_ids


def generateSchema(response_json, api):
    meterpoints_string = json.dumps(response_json, indent=4)
    meterpoints_schema = json_schema.dumps(meterpoints_string, indent=4)
    sampleschema_dir_path = sys.path[0] + "/files/sample_schema/"
    if not os.path.exists(sampleschema_dir_path):
        os.makedirs(sampleschema_dir_path)
    filename = sampleschema_dir_path + "_sample_" + api + "_schema" + ".json"
    with open(filename, "w") as schemafile:
        schemafile.write(meterpoints_schema)


def processAccounts(account_id_s):
    apis = [
        "meterpoints",
        "direct_debits",
        "internal_estimates",
        "internal_readings",
        "account_status",
        "elec_status",
        "gas_status",
        "tariff_history",
    ]
    for api in apis:
        print(api)
        for account_id in account_id_s:
            api_url, head = get_meter_point_api_info(account_id, api)
            # msg_ac = 'ac:' + str(account_id)
            # log_error(msg_ac, '')

            if api in ["internal_readings"]:
                api_response = get_api_response_pages(api_url, head)
            else:
                api_response = get_api_response(api_url, head)

            if api_response:
                print(account_id)
                generateSchema(api_response, api)


if __name__ == "__main__":
    account_ids = []

    """Enable this to test for 1 account id"""
    if con.test_config["enable_manual"] == "Y":
        account_ids = con.test_config["account_ids"]

    # if con.test_config['enable_db'] == 'Y':
    account_ids = get_accountID_fromDB()

    processAccounts(account_ids)
