import io
import timeit
import json
import time
import csv
import sys
import os
from datetime import datetime, timedelta
import requests
import pandas as pd
from ratelimit import limits, sleep_and_retry
from requests import ConnectionError

from cdw.conf import config as con
from cdw.common import utils as util
from cdw.common import api_filters as apif
from cdw.connections.connect_db import get_boto_S3_Connections as s3_con
from cdw.connections import connect_db as db


class ForecastWeather:

    max_calls = con.api_config["max_api_calls"]
    rate = con.api_config["allowed_period_in_secs"]

    def __init__(self, duration, forecast_resolution="hourly"):

        hours, days = 0, 0

        if forecast_resolution == "hourly":
            self.units = "hours"
            hours = duration
        elif forecast_resolution == "daily":
            self.units = "days"
            days = duration
        else:
            raise ValueError("Invalid forcast resolution")

        dirs = util.get_dir()
        api_dir = dirs["apis"]["forecast_weather"][forecast_resolution]
        s3_dir = dirs["s3_weather_key"]["forecast_weather"][forecast_resolution]
        bucket_name = dirs["s3_bucket"]

        self.extract_date = datetime.today().strftime("%Y-%m-%d")
        self.start_date = self.extract_date
        self.end_date = (datetime.today().date() + timedelta(days=days, hours=hours)).strftime("%Y-%m-%d")
        self.forecast_resolution = forecast_resolution
        self.duration = duration
        self.weather_sql = apif.weather_forecast[forecast_resolution]["stage1"]
        self.api_url = api_dir["api_url"]
        self.key = api_dir["token"]
        self.data_keys = api_dir["data_keys"]
        self.dtypes = api_dir["dtypes"]
        self.parquet_file_name = "{}.parquet".format(util.get_jobID())
        self.stage1_dir_s3 = s3_dir["stage1"]
        self.stage2_dir_s3 = s3_dir["stage2"]
        self.s3 = s3_con(bucket_name)

    @sleep_and_retry
    @limits(calls=max_calls, period=rate)
    def get_api_response(self, postcode):
        """
        get the response for the respective url that is passed as part of this function
        """
        session = requests.Session()
        start_time = time.time()
        timeout = con.api_config["connection_timeout"]
        retry_in_secs = con.api_config["retry_in_secs"]
        i = 0
        api_url = self.api_url.format(postcode.strip(), self.duration, self.key)
        while True:
            try:
                response = session.get(api_url)
                if response.status_code == 200:
                    if response.content.decode("utf-8") != "":
                        response = json.loads(response.content.decode("utf-8"))
                        response["outcode"] = postcode

                        # print(self.format_json_response(response))

                        return self.format_json_response(response)
                else:
                    print(f"Problem grabbing data for URL {api_url}:", response.status_code)
                    self.log_error(f"Response Error: Problem grabbing data for URL {api_url}:", response.status_code)
                    return None

            except ConnectionError:
                if time.time() > start_time + timeout:
                    print("Unable to Connect after {} seconds of ConnectionErrors".format(timeout))
                    self.log_error("Unable to Connect after {} seconds of ConnectionErrors".format(timeout))
                    break
                else:
                    print("Retrying connection in " + str(retry_in_secs) + " seconds" + str(i))
                    self.log_error("Retrying connection in " + str(retry_in_secs) + " seconds" + str(i))

                    time.sleep(retry_in_secs)
            i = i + retry_in_secs

    def flatten(self, weather_json):

        postcode_weather = json.loads(weather_json)

        def get_row(interval_forecast):

            row = {key: interval_forecast[key] for key in self.data_keys}

            row["icon"] = interval_forecast["weather"]["icon"]
            row["code"] = interval_forecast["weather"]["code"]
            row["description"] = interval_forecast["weather"]["description"]

            repeated_cols = ["city_name", "lon", "timezone", "lat", "country_code", "state_code", "outcode"]
            repeated_data = {repeated_col: postcode_weather[repeated_col] for repeated_col in repeated_cols}

            row.update(repeated_data)

            row["etlchange"] = datetime.utcnow().isoformat()

            return row

        interval_forecasts = postcode_weather["data"]
        rows = [get_row(interval_forecast) for interval_forecast in interval_forecasts]

        return rows

    def add_to_dataframe(self, rows):

        self.dataframe = self.dataframe.append(rows)

    def create_parquet_file(self, dataframe):

        parquet_file = io.BytesIO()
        dataframe.to_parquet(parquet_file, index=False)

        return parquet_file

    def post_stage1_to_s3(self, data, postcode):

        file_name = f"{postcode.strip()}_{self.forecast_resolution}_{self.start_date}_to_{self.end_date}.json"
        self.s3.key = self.stage1_dir_s3.format(self.extract_date) + file_name
        self.s3.set_contents_from_string(data)

        return self.flatten(data)

    def post_stage2_to_s3(self, parquet_file):

        self.s3.key = self.stage2_dir_s3.format(self.extract_date) + self.parquet_file_name
        self.s3.set_contents_from_file(parquet_file, rewind=True)

    def format_json_response(self, data):
        return json.dumps(data, indent=4).replace("null", '""')

    def log_error(self, error_msg, error_code=""):

        logs_dir_path = sys.path[0] + "/logs/"

        os.makedirs(logs_dir_path, exist_ok=True)

        with open(
            logs_dir_path + self.forecast_resolution + "_weather_log" + time.strftime("%d%m%Y") + ".csv", mode="a"
        ) as errorlog:
            employee_writer = csv.writer(errorlog, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL)
            employee_writer.writerow([error_msg, error_code])

    def processData(self):

        postcodes = self.get_weather_postcode(self.weather_sql)

        # list of reponses from the api for each
        api_response_list = [(self.get_api_response(postcode), postcode) for postcode in postcodes]

        # 2D array containing array of rows for each postcode
        flattened_data_list = [
            self.post_stage1_to_s3(api_response, postcode)
            for api_response, postcode in api_response_list
            if api_response
        ]

        # the 2D array unpacked into a 1D array
        unpacked_rows = [row for postcode_data in flattened_data_list for row in postcode_data]

        dataframe = pd.DataFrame(unpacked_rows).astype(dtype=self.dtypes)

        parquet_file = self.create_parquet_file(dataframe)

        self.post_stage2_to_s3(parquet_file)

    def get_weather_postcode(self, config_sql):

        pr = db.get_redshift_connection()
        postcodes_df = pr.redshift_to_pandas(config_sql)
        db.close_redshift_connection()
        postcodes_list = postcodes_df["postcode"].values.tolist()

        return postcodes_list
