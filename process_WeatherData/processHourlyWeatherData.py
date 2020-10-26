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

sys.path.append('..')
from conf import config as con
from common import utils as util
from common import api_filters as apif
from connections.connect_db import get_boto_S3_Connections as s3_con
from connections import connect_db as db


class HourlyWeather:
    max_calls = con.api_config['max_api_calls']
    rate = con.api_config['allowed_period_in_secs']

    def __init__(self):

        self.extract_date = datetime.today().strftime('%Y-%m-%d')
        self.start_date = self.extract_date
        self.end_date = (datetime.today().date() + timedelta(hours=48)).strftime('%Y-%m-%d')
        self.api_url, self.key = util.get_weather_url_token('hourly_weather')
        self.hours = 48
        self.weather_sql = apif.weather_forecast['hourly']['stage1']
        self.dataframe = pd.DataFrame()
        self.parquet_file_name = '{}.parquet'.format(util.get_jobID())

    @sleep_and_retry
    @limits(calls=max_calls, period=rate)
    def get_api_response(self, api_url):
        """
            get the response for the respective url that is passed as part of this function
        """
        session = requests.Session()
        start_time = time.time()
        timeout = con.api_config['connection_timeout']
        retry_in_secs = con.api_config['retry_in_secs']
        i = 0
        while True:
            try:
                response = session.get(api_url)
                if response.status_code == 200:
                    if response.content.decode('utf-8') != '':
                        response_json = json.loads(response.content.decode('utf-8'))
                        return response_json
                else:
                    print(f'Problem grabbing data for URL {api_url}:', response.status_code)
                    self.log_error(f'Response Error: Problem grabbing data for URL {api_url}:', response.status_code)
                    return None

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

    def flatten(self, weather_json):

        postcode_weather = json.loads(weather_json)

        def get_row(hourly_forecast):

            row = dict()

            row['wind_cdir'] = hourly_forecast['wind_cdir']
            row['rh'] = hourly_forecast['rh']
            row['pod'] = hourly_forecast['pod']
            row['timestamp_utc'] = hourly_forecast['timestamp_utc']
            row['pres'] = hourly_forecast['pres']
            row['solar_rad'] = hourly_forecast['solar_rad']
            row['ozone'] = hourly_forecast['ozone']
            row['icon'] = hourly_forecast['weather']['icon']
            row['code'] = hourly_forecast['weather']['code']
            row['description'] = hourly_forecast['weather']['description']
            row['wind_gust_spd'] = hourly_forecast['wind_gust_spd']
            row['timestamp_local'] = hourly_forecast['timestamp_local']
            row['snow_depth'] = hourly_forecast['snow_depth']
            row['clouds'] = hourly_forecast['clouds']
            row['ts'] = hourly_forecast['ts']
            row['wind_spd'] = hourly_forecast['wind_spd']
            row['pop'] = hourly_forecast['pop']
            row['wind_cdir_full'] = hourly_forecast['wind_cdir_full']
            row['slp'] = hourly_forecast['slp']
            row['dni'] = hourly_forecast['dni']
            row['dewpt'] = hourly_forecast['dewpt']
            row['snow'] = hourly_forecast['snow']
            row['uv'] = hourly_forecast['uv']
            row['wind_dir'] = hourly_forecast['wind_dir']
            row['clouds_hi'] = hourly_forecast['clouds_hi']
            row['precip'] = hourly_forecast['precip']
            row['vis'] = hourly_forecast['vis']
            row['dhi'] = hourly_forecast['dhi']
            row['app_temp'] = hourly_forecast['app_temp']
            row['datetime'] = hourly_forecast['datetime']
            row['temp'] = hourly_forecast['temp']
            row['ghi'] = hourly_forecast['ghi']
            row['clouds_mid'] = hourly_forecast['clouds_mid']
            row['clouds_low'] = hourly_forecast['clouds_low']
            row['city_name'] = postcode_weather['city_name']
            row['lon'] = postcode_weather['lon']
            row['timezone'] = postcode_weather['timezone']
            row['lat'] = postcode_weather['lat']
            row['country_code'] = postcode_weather['country_code']
            row['state_code'] = postcode_weather['state_code']
            row['outcode'] = postcode_weather['outcode']
            row['forecast_issued'] = postcode_weather['forecast_issued']
            row['etlchange'] = datetime.utcnow()

            return row

        hourly_forecasts = postcode_weather['data']
        rows = [ get_row(hourly_forecast) for hourly_forecast in hourly_forecasts ]

        return rows

    def add_to_dataframe(self, rows):

        self.dataframe = self.dataframe.append(rows)

    def create_parquet_file(self):

        parquet_file = io.BytesIO()
        # Set allow_truncated_timestamp to avoid a pyarrow error that is seen in unit tests:
        # pyarrow.lib.ArrowInvalid: Casting from timestamp[ns] to timestamp[ms] would lose data: 1603725177571636000
        self.dataframe.to_parquet(parquet_file, index=False, allow_truncated_timestamps=True)

        return parquet_file

    def post_stage1_to_s3(self, data, postcode, k, stage1_dir_s3):

        if data:
            file_name = f'{postcode.strip()}_hourly_{self.start_date}_to_{self.end_date}.json'
            k.key = stage1_dir_s3.format(self.extract_date) + file_name
            k.set_contents_from_string(data)
        else:
            print(f'{postcode.strip()} has no weather')

    def post_stage2_to_s3(self, parquet_file, k, stage2_dir_s3):

        k.key = stage2_dir_s3.format(self.extract_date) + self.parquet_file_name
        k.set_contents_from_file(parquet_file, rewind=True)

    def format_json_response(self, data):
        return json.dumps(data, indent=4).replace('null', '""')

    def log_error(self, error_msg, error_code=''):

        logs_dir_path = sys.path[0] + '/logs/'

        if not os.path.exists(logs_dir_path):
            os.makedirs(logs_dir_path)
        with open(logs_dir_path + 'hourly_weather_log' + time.strftime('%d%m%Y') + '.csv',
                  mode='a') as errorlog:
            employee_writer = csv.writer(errorlog, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            employee_writer.writerow([error_msg, error_code])

    def processData(self, postcodes, k, stage1_dir_s3, stage2_dir_s3):

        for postcode in postcodes:

            api_url = self.api_url.format(postcode.strip(), self.hours, self.key)
            api_response = self.get_api_response(api_url)

            if api_response:
                api_response['outcode'] = postcode
                api_response['forecast_issued'] = self.extract_date
                formatted_json = self.format_json_response(api_response)

                self.post_stage1_to_s3(formatted_json, postcode, k, stage1_dir_s3)

                flattened_data = self.flatten(formatted_json)

                self.add_to_dataframe(flattened_data)

        parquet_file = self.create_parquet_file()
        self.post_stage2_to_s3(parquet_file, k, stage2_dir_s3)

    def get_weather_postcode(self, config_sql):

        pr = db.get_redshift_connection()
        postcodes_df = pr.redshift_to_pandas(config_sql)
        db.close_redshift_connection()
        postcodes_list = postcodes_df['postcode'].values.tolist()

        return postcodes_list

if __name__ == "__main__":

    p = HourlyWeather()

    dir_s3 = util.get_dir()
    bucket_name = dir_s3['s3_bucket']
    stage1_dir_s3 = dir_s3['s3_weather_key']['HourlyWeather']['stage1']
    stage2_dir_s3 = dir_s3['s3_weather_key']['HourlyWeather']['stage2']

    s3 = s3_con(bucket_name)
    weather_postcodes = p.get_weather_postcode(p.weather_sql)

    start = timeit.default_timer()

    p.processData(weather_postcodes, s3, stage1_dir_s3, stage2_dir_s3)

    print("Process completed in " + str(timeit.default_timer() - start) + ' seconds')
