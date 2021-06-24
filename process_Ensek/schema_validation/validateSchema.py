import csv
import requests
import json
from json_schema import json_schema
import pymysql
import time
import sys
import os
import psycopg2
import timeit
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from conf import config as con
from common import directories as dir

import boto3
from common.secrets_manager import get_secret

client = boto3.client("secretsmanager")


def get_api_info(account_id, api):

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


def format_json_response(data, api):

    data_str = json.dumps(data, indent=4)

    if api == "tariff_history":
        for k in data:
            if k["Gas"] is None:
                # print(data_str.replace('"Gas": null', '"Gas": {"unitRates": [], "standingChargeRates": []}'))
                data_str = data_str.replace('"Gas": null', '"Gas": {"unitRates": [], "standingChargeRates": []}')

    data_json = json.loads(data_str)
    return data_json


def log_error(error_msg, error_code=""):
    with open(sys.path[0] + "/logs/" + "ensek_schema_error_" + time.strftime("%d%m%Y") + ".csv", mode="a") as errorlog:
        employee_writer = csv.writer(errorlog, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL)
        employee_writer.writerow([error_msg, error_code])


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
    print("connected to rds..")

    cur.execute(con.test_config["schema_account_ids_sql_rds"])

    account_ids = [row[0] for row in cur]
    cur.close()
    conn.close()
    print("connection to rds closed..")

    return account_ids


def get_accountID_from_Redshift():
    redshift_config = get_secret(client, con.refshift_config["secret_id"])
    conn = psycopg2.connect(
        host=redshift_config["host"],
        port=redshift_config["port"],
        user=redshift_config["username"],
        password=redshift_config["password"],
        dbname=con.redshift_config["db"],
    )

    cur = conn.cursor()
    print("connected to redshift..")

    cur.execute(con.test_config["schema_account_ids_sql_redshift"])

    account_ids = [row[0] for row in cur]
    cur.close()
    conn.close()
    print("connection to redshift closed..")

    # server.stop()
    return account_ids


def validateSchema(response_json, api, account_id):
    schema_valid = False
    error_json = ""

    meterpoints_string = json.dumps(response_json, indent=4)
    filename = sys.path[0] + "/files/ensek_schema/" + api + "_schema" + ".json"
    with open(filename, "r") as schemafile:
        meterpoints_schema = schemafile.read()

    js = json_schema.loads(meterpoints_schema)
    # print(json_schema.match(meterpoints_string, meterpoints_schema))

    if js == meterpoints_string:
        schema_valid = True
    else:
        schema_valid = False
        error_json = full_check(meterpoints_string, js)

    schema_validation_details = {
        "api": api,
        "account_id": account_id,
        "valid": schema_valid,
        "error": error_json,
        "error_json": meterpoints_string,
    }
    return schema_validation_details


def full_check(json_string, json_schema):
    my_json = json.loads(json_string)
    e = json_schema._comparar(my_json, json_schema.schema_dict)
    t = json.dumps(e, indent=4)
    t = t.replace("\\u001b[91m", "\033[91m").replace("\\u001b[92m", "\033[92m")

    error_json = "\033[92m%s\033[0m" % t

    return error_json


def processAccounts():
    try:
        start = timeit.default_timer()
        account_ids = []
        """Enable this to test for 1 account id"""
        if con.test_config["enable_manual"] == "Y":
            account_ids = con.test_config["account_ids"]

        # if con.test_config['enable_db'] == 'Y':
        # server = redshift_connection()
        # account_ids = get_accountID_from_Redshift()
        account_ids = get_accountID_fromDB()

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
            start_api = timeit.default_timer()
            print("{0}: >>>>>> {1} <<<<<<<".format(datetime.now().strftime("%H:%M:%S"), api.upper()))
            for account_id in account_ids:
                api_url, head = get_api_info(account_id, api)

                if api in ["internal_readings"]:
                    api_response = get_api_response_pages(api_url, head)
                else:
                    api_response = get_api_response(api_url, head)

                if api_response:
                    formatted_json_response = format_json_response(api_response, api)
                    validate_schema_response = validateSchema(formatted_json_response, api, account_id)
                    if validate_schema_response["valid"]:
                        # print(str(account_id) + ': ' + str(validate_schema_response['valid']))
                        pass
                    else:
                        # print(str(account_id) + ': ' + str(validate_schema_response['valid']))
                        msg_error = (
                            time.strftime("%d-%m-%Y-%H:%M:%S")
                            + " - "
                            + api
                            + " api has invalid schema for account id "
                            + str(account_id)
                            + "\n"
                            + validate_schema_response["error"]
                            + validate_schema_response["error_json"]
                        )
                        log_error(msg_error, "")
                        # print(msg_error)
                        raise Exception(" schema Error : {0}".format(msg_error))
            print(
                "{0}: Completed in {1:.2f} seconds".format(
                    datetime.now().strftime("%H:%M:%S"), float(timeit.default_timer() - start_api)
                )
            )
        print(
            "{0}: Schema Validation completed in {1:.2f} seconds".format(
                datetime.now().strftime("%H:%M:%S"), float(timeit.default_timer() - start)
            )
        )

        return True

    except:
        raise


if __name__ == "__main__":

    schema_valid_response_main = processAccounts()
