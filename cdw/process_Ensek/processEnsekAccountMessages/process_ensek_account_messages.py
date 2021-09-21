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


from cdw.common import utils as util
from cdw.conf import config as con
from cdw.connections.connect_db import get_boto_S3_Connections as s3_con


max_calls = con.api_config["max_api_calls"]
rate = con.api_config["allowed_period_in_secs"]


@sleep_and_retry
@limits(calls=max_calls, period=rate)
def get_api_response(api_url, head):
    """
    get the response for the respective url that is passed as part of this function
    """
    start_time = time.time()
    timeout = con.api_config["connection_timeout"]
    retry_in_secs = con.api_config["retry_in_secs"]
    i = 0
    while True:
        try:
            response = requests.get(api_url, headers=head)
            if response.status_code == 200:
                return json.loads(response.content.decode("utf-8"))
            else:
                print("Problem Grabbing Data: ", response.status_code)
                break

        except ConnectionError:
            if time.time() > start_time + timeout:
                print("Unable to Connect after {} seconds of ConnectionErrors".format(timeout))

                break
            else:
                print("Retrying connection in " + str(retry_in_secs) + " seconds" + str(i))

                time.sleep(retry_in_secs)
        i = i + retry_in_secs


""" Extracting direct debit data"""


def extract_tariff_history_json(data, account_id, k, dir_s3):
    # global k,
    meta_tariff_history = [
        "tariffName",
        "startDate",
        "endDate",
        "discounts",
        "tariffType",
        "exitFees",
        "id",
        "account_id",
    ]

    df_tariff_history = json_normalize(data)
    df_tariff_history["account_id"] = account_id
    df_tariff_history1 = df_tariff_history[meta_tariff_history]

    filename_tariff_history = "df_tariff_history_" + str(account_id) + ".csv"
    df_tariff_history_string = df_tariff_history1.to_csv(None, index=False)
    # k.key = 'ensek-meterpoints/TariffHistory/' + filename_tariff_history
    k.key = dir_s3["s3_key"]["TariffHistory"] + filename_tariff_history
    k.set_contents_from_string(df_tariff_history_string)


"""Read Users from S3"""


def get_Users(k):
    # global k
    filename_Users = "users.csv"
    k.key = "ensek-meterpoints/Users/" + filename_Users
    k.open()
    l = k.read()
    s = l.decode("utf-8")
    p = s.splitlines()
    # print(len(p))
    return p


"""Format Json to handle null values"""


def format_json_response(data):
    data_str = json.dumps(data, indent=4).replace("null", '""')
    data_json = json.loads(data_str)
    return data_json


def processAccounts(account_ids, k, dir_s3):
    api_url, head = util.get_ensek_api_info1("account_messages")
    for account_id in account_ids:
        t = con.api_config["total_no_of_calls"]
        # Get Account Staus
        print("ac: " + str(account_id))
        msg_ac = "ac:" + str(account_id)
        api_url1 = api_url.format(account_id)
        th_response = get_api_response(api_url1, head)
        if th_response:
            formatted_tariff_history = format_json_response(th_response)
            # print(json.dumps(formatted_tariff_history))
            extract_tariff_history_json(formatted_tariff_history, account_id, k, dir_s3)
        else:
            print("ac:" + str(account_id) + " has no data")
            msg_ac = "ac:" + str(account_id) + " has no data"


if __name__ == "__main__":
    freeze_support()

    dir_s3 = util.get_environment()
    bucket_name = dir_s3["s3_bucket"]

    k = s3_con(bucket_name)

    """Enable this to test for 1 account id"""
    if con.test_config["enable_manual"] == "Y":
        account_ids = con.test_config["account_ids"]

    if con.test_config["enable_file"] == "Y":
        account_ids = get_Users(k)

    if con.test_config["enable_db"] == "Y":
        account_ids = util.get_accountID_fromDB(False)

    if con.test_config["enable_db_max"] == "Y":
        account_ids = util.get_accountID_fromDB(True)

    # threads = 5
    # chunksize = 100

    # with Pool(threads) as pool:
    #     pool.starmap(processAccounts, zip(account_ids), chunksize)

    print(len(account_ids))
    print(int(len(account_ids) / 12))
    p = int(len(account_ids) / 12)

    # print(account_ids)
    ######### Multiprocessing starts  ##########
    p1 = multiprocessing.Process(target=processAccounts, args=(account_ids[0:p], s3_con(bucket_name), dir_s3))
    p2 = multiprocessing.Process(target=processAccounts, args=(account_ids[p : 2 * p], s3_con(bucket_name), dir_s3))
    p3 = multiprocessing.Process(target=processAccounts, args=(account_ids[2 * p : 3 * p], s3_con(bucket_name), dir_s3))
    p4 = multiprocessing.Process(target=processAccounts, args=(account_ids[3 * p : 4 * p], s3_con(bucket_name), dir_s3))
    p5 = multiprocessing.Process(target=processAccounts, args=(account_ids[4 * p : 5 * p], s3_con(bucket_name), dir_s3))
    p6 = multiprocessing.Process(target=processAccounts, args=(account_ids[5 * p : 6 * p], s3_con(bucket_name), dir_s3))
    p7 = multiprocessing.Process(target=processAccounts, args=(account_ids[6 * p : 7 * p], s3_con(bucket_name), dir_s3))
    p8 = multiprocessing.Process(target=processAccounts, args=(account_ids[7 * p : 8 * p], s3_con(bucket_name), dir_s3))
    p9 = multiprocessing.Process(target=processAccounts, args=(account_ids[8 * p : 9 * p], s3_con(bucket_name), dir_s3))
    p10 = multiprocessing.Process(
        target=processAccounts, args=(account_ids[9 * p : 10 * p], s3_con(bucket_name), dir_s3)
    )
    p11 = multiprocessing.Process(
        target=processAccounts, args=(account_ids[10 * p : 11 * p], s3_con(bucket_name), dir_s3)
    )
    p12 = multiprocessing.Process(target=processAccounts, args=(account_ids[11 * p :], s3_con(bucket_name), dir_s3))

    p1.start()
    p2.start()
    p3.start()
    p4.start()
    p5.start()
    p6.start()
    p7.start()
    p8.start()
    p9.start()
    p10.start()
    p11.start()
    p12.start()

    p1.join()
    p2.join()
    p3.join()
    p4.join()
    p5.join()
    p6.join()
    p7.join()
    p8.join()
    p9.join()
    p10.join()
    p11.join()
    p12.join()
    ####### Multiprocessing Ends ########
