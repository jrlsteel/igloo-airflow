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
from datetime import datetime, timedelta

import sys
import os


from cdw.conf import config as con
from cdw.common import utils as util
from cdw.connections.connect_db import get_boto_S3_Connections as s3_con
from cdw.connections import connect_db as db
from cdw.common import api_filters as apif

environment = util.get_env()

logger = util.IglooLogger()


class HistoricalWeather:
    max_calls = con.api_config["max_api_calls"]
    rate = con.api_config["allowed_period_in_secs"]
    end_date_offset_days = con.weather_historical["end_date_offset_days"]
    start_date_offset_days = con.weather_historical["start_date_offset_days"]

    def __init__(self):

        self.day_of_week = datetime.today().isoweekday()
        self.end_date = (
            datetime.today().date() + timedelta(days=(7 - self.day_of_week)) + timedelta(days=self.end_date_offset_days)
        )
        self.start_date = (datetime.today().date() + timedelta(days=(7 - self.day_of_week))) + timedelta(
            days=self.start_date_offset_days
        )
        self.api_url, self.key = util.get_weather_url_token("historical_weather")
        self.num_days_per_api_calls = 7
        self.sql = apif.weather_postcodes["daily"]

    @sleep_and_retry
    @limits(calls=max_calls, period=rate)
    def get_api_response(self, api_url):
        """
        get the response for the respective url that is passed as part of this function
        """
        session = requests.Session()
        start_time = time.time()
        timeout = con.api_config["connection_timeout"]
        retry_in_secs = con.api_config["retry_in_secs"]
        i = 0
        while True:
            try:
                response = session.get(api_url)
                if response.status_code == 200:
                    if response.content.decode("utf-8") != "":
                        response_json = json.loads(response.content.decode("utf-8"))
                        return response_json
                else:
                    print("Problem Grabbing Data: ", response.status_code)
                    return None
                    break

            except ConnectionError:
                if time.time() > start_time + timeout:
                    print("Unable to Connect after {} seconds of ConnectionErrors".format(timeout))
                    break
                else:
                    print("Retrying connection in " + str(retry_in_secs) + " seconds" + str(i))

                    time.sleep(retry_in_secs)
            i = i + retry_in_secs

    def extract_weather_data(self, data, postcode, k, dir_s3, start_date, end_date):
        meta_weather = ["timezone", "state_code", "country_code", "lat", "lon", "city_name", "station_id", "city_id"]
        weather_df = json_normalize(data, record_path="data", meta=meta_weather, max_level=0)
        weather_df["sources"] = " ".join(data["sources"])
        weather_df["postcode"] = postcode

        weather_df1 = weather_df.drop(columns=["app_temp"])
        if weather_df1.empty:
            print(" - has no Weather data")
        else:
            week_number_iso = start_date.strftime("%V")
            year = start_date.strftime("%Y")

            column_list = util.get_common_info("weather_column_order", "historical_weather")
            weather_df_string = weather_df1.to_csv(None, columns=column_list, index=False)
            file_name_weather = (
                "historical_weather"
                + "_"
                + postcode.strip()
                + "_"
                + year.strip()
                + "_"
                + week_number_iso.strip()
                + ".csv"
            )
            k.key = dir_s3["s3_weather_key"]["HistoricalWeather"] + file_name_weather

            # print(weather_df_string)
            k.set_contents_from_string(weather_df_string)

    """Format Json to handle null values"""

    def format_json_response(self, data):
        data_str = json.dumps(data, indent=4).replace("null", '""')
        data_json = json.loads(data_str)
        return data_json

    def processData(self, postcodes, k, _dir_s3):
        logger.in_prod_env(
            "Fetching data for {} postcodes, start_date={}, end_data={}".format(
                len(postcodes), self.start_date, self.end_date
            )
        )

        for postcode in postcodes:

            _start_date = self.start_date
            while _start_date < self.end_date:

                # Logic to fetch date for only 7 days for each call
                _end_date = _start_date + timedelta(days=7)
                if _end_date > self.end_date:
                    _end_date = self.end_date

                api_url1 = self.api_url.format(postcode, _start_date, _end_date, self.key)
                logger.in_test_env("GET {}".format(api_url1.replace(self.key, "x")))

                api_response = self.get_api_response(api_url1)

                if api_response:
                    formatted_json = self.format_json_response(api_response)
                    # print(formatted_json)
                    self.extract_weather_data(formatted_json, postcode, k, _dir_s3, _start_date, _end_date)
                _start_date = _end_date

    def get_weather_postcode(self, config_sql):
        pr = db.get_redshift_connection()
        postcodes_df = pr.redshift_to_pandas(config_sql)
        db.close_redshift_connection()
        postcodes_list = postcodes_df["postcode"].values.tolist()

        return postcodes_list


if __name__ == "__main__":

    freeze_support()

    p = HistoricalWeather()

    dir_s3 = util.get_dir()
    bucket_name = dir_s3["s3_bucket"]

    s3 = s3_con(bucket_name)
    # weather_sql = "SELECT left(postcode, len(postcode) - 3) postcode FROM aws_s3_stage1_extracts.stage1_postcodesuk where  left(postcode, len(postcode) - 3) in ('SL6') group by left(postcode, len(postcode) - 3)"

    weather_postcode_sql = p.sql
    weather_postcodes = p.get_weather_postcode(weather_postcode_sql)
    if environment in ["preprod", "dev"]:
        weather_postcodes = weather_postcodes[
            :24
        ]  # only get 24 postcodes if environment is dev or preprod, as we do not want to process all of them.

    ##### Multiprocessing Starts #########
    env = util.get_env()

    if env == "uat":
        n = 12  # number of process to run in parallel
    else:
        n = 12

    k = int(len(weather_postcodes) / n)  # get equal no of files for each process

    print(len(weather_postcodes))
    print(k)

    processes = []
    lv = 0
    start = timeit.default_timer()

    for i in range(n + 1):
        p1 = HistoricalWeather()
        print(i)
        uv = i * k
        if i == n:
            t = multiprocessing.Process(
                target=p1.processData, args=(weather_postcodes[lv:], s3_con(bucket_name), dir_s3)
            )
        else:
            t = multiprocessing.Process(
                target=p1.processData, args=(weather_postcodes[lv:uv], s3_con(bucket_name), dir_s3)
            )
        lv = uv

        processes.append(t)

    for p in processes:
        p.start()
        time.sleep(2)

    for process in processes:
        process.join()

    print("Process completed in " + str(timeit.default_timer() - start) + " seconds")
    ####### Multiprocessing Ends #########
