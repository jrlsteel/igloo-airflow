import sys
import io
import json
import uuid
import unittest
from unittest.mock import MagicMock, patch
from moto import mock_s3_deprecated

sys.path.append('..')

from process_forecast_weather_data import ForecastWeather
import process_forecast_weather_data as weather_module

api_returned_data_hourly_json = open('fixtures/api_returned_data_hourly.json', 'r').read()
api_returned_data_daily_json = open('fixtures/api_returned_data_daily.json', 'r').read()

api_returned_data_hourly = json.loads(api_returned_data_hourly_json)
api_returned_data_daily = json.loads(api_returned_data_daily_json)

postcodes=['GU14']
dir_s3 = weather_module.util.get_dir()
bucket_name = dir_s3['s3_bucket']

print(bucket_name)

TEST_UUIDS_COUNT = 0

def mock_uuid():
    global TEST_UUIDS_COUNT
    TEST_UUIDS_COUNT += 1
    return uuid.UUID(int=TEST_UUIDS_COUNT)

class TestProcessHourlyWeatherData(unittest.TestCase):

    @mock_s3_deprecated
    @patch('uuid.uuid4', mock_uuid)
    def test_process_hourly_single_postcode(self):

        stage1_dir_s3 = dir_s3['s3_weather_key']['forecast_weather']['hourly']['stage1']
        stage2_dir_s3 = dir_s3['s3_weather_key']['forecast_weather']['hourly']['stage2']


        boto = weather_module.db.boto
        s3 = boto.connect_s3(aws_access_key_id='XXXX', aws_secret_access_key='XXXX')

        bucket = s3.create_bucket(bucket_name)
        s3_key = boto.s3.key.Key(bucket)

        hw = ForecastWeather(duration=48, forecast_resolution='hourly')
        hw.get_weather_postcode = MagicMock(return_value=postcodes)
        hw.get_api_response = MagicMock(return_value=api_returned_data_hourly_json)

        hw.processData()

        stage1_file_name = f'{postcodes[0].strip()}_hourly_{hw.start_date}_to_{hw.end_date}.json'

        s3_key.key = stage1_dir_s3.format(hw.extract_date) + stage1_file_name
        s3_stage1_object = json.loads(s3_key.get_contents_as_string().decode('utf-8'))


        stage2_file_name = '{}.parquet'.format('00000000-0000-0000-0000-000000000002')
        s3_key.key = stage2_dir_s3.format(hw.extract_date) + stage2_file_name
        try:
            s3_key.get_contents_as_string()
        except Exception:

            self.fail('Stage 2 parquet file was not found')

        self.assertDictEqual(s3_stage1_object, api_returned_data_hourly)

    @mock_s3_deprecated
    @patch('uuid.uuid4', mock_uuid)
    def test_process_daily_single_postcode(self):
        stage1_dir_s3 = dir_s3['s3_weather_key']['forecast_weather']['daily']['stage1']
        stage2_dir_s3 = dir_s3['s3_weather_key']['forecast_weather']['daily']['stage2']

        boto = weather_module.db.boto
        s3 = boto.connect_s3(aws_access_key_id='XXXX', aws_secret_access_key='XXXX')

        bucket = s3.create_bucket(bucket_name)
        s3_key = boto.s3.key.Key(bucket)

        hw = ForecastWeather(duration=16, forecast_resolution='daily')
        hw.get_weather_postcode = MagicMock(return_value=postcodes)
        hw.get_api_response = MagicMock(return_value=api_returned_data_daily_json)

        hw.processData()

        stage1_file_name = f'{postcodes[0].strip()}_daily_{hw.start_date}_to_{hw.end_date}.json'

        s3_key.key = stage1_dir_s3.format(hw.extract_date) + stage1_file_name
        s3_stage1_object = json.loads(s3_key.get_contents_as_string().decode('utf-8'))

        stage2_file_name = '{}.parquet'.format('00000000-0000-0000-0000-000000000001')
        s3_key.key = stage2_dir_s3.format(hw.extract_date) + stage2_file_name
        try:
            s3_key.get_contents_as_string()
        except Exception:

            self.fail('Stage 2 parquet file was not found')

        self.assertDictEqual(s3_stage1_object, api_returned_data_daily)

if __name__ == '__main__':
    unittest.main()
