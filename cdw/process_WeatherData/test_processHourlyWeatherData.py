import os
import sys
import io
import json
import uuid
import unittest
from unittest.mock import MagicMock, patch
from moto import mock_s3_deprecated
import pandas as pd


from cdw.process_WeatherData.process_forecast_weather_data import ForecastWeather
import cdw.process_WeatherData.process_forecast_weather_data as weather_module

dirname = os.path.dirname(__file__)
api_returned_data_hourly_json = open(os.path.join(dirname, "fixtures/api_returned_data_hourly.json"), "r").read()
api_returned_data_daily_json = open(os.path.join(dirname, "fixtures/api_returned_data_daily.json"), "r").read()

api_returned_data_hourly = json.loads(api_returned_data_hourly_json)
api_returned_data_daily = json.loads(api_returned_data_daily_json)

postcodes = ["GU14"]
dir_s3 = weather_module.util.get_dir()
bucket_name = dir_s3["s3_bucket"]

print(bucket_name)

TEST_UUIDS_COUNT = 0


def mock_uuid():
    global TEST_UUIDS_COUNT
    TEST_UUIDS_COUNT += 1
    return uuid.UUID(int=TEST_UUIDS_COUNT)


hourly_dtypes = {
    "wind_cdir": "object",
    "rh": "int64",
    "pod": "object",
    "timestamp_utc": "object",
    "pres": "float64",
    "solar_rad": "float64",
    "ozone": "float64",
    "wind_gust_spd": "float64",
    "timestamp_local": "object",
    "snow_depth": "float64",
    "clouds": "int64",
    "ts": "int64",
    "wind_spd": "float64",
    "pop": "int64",
    "wind_cdir_full": "object",
    "slp": "float64",
    "dni": "float64",
    "dewpt": "float64",
    "snow": "float64",
    "uv": "float64",
    "wind_dir": "int64",
    "clouds_hi": "int64",
    "precip": "float64",
    "vis": "float64",
    "dhi": "float64",
    "app_temp": "float64",
    "datetime": "object",
    "temp": "float64",
    "ghi": "float64",
    "clouds_mid": "int64",
    "clouds_low": "int64",
    "icon": "object",
    "code": "int64",
    "description": "object",
    "city_name": "object",
    "lon": "object",
    "timezone": "object",
    "lat": "object",
    "country_code": "object",
    "state_code": "object",
    "outcode": "object",
    "etlchange": "object",
}

daily_dtypes = {
    "moonrise_ts": "int64",
    "wind_cdir": "object",
    "rh": "int64",
    "pres": "float64",
    "high_temp": "float64",
    "sunset_ts": "int64",
    "ozone": "float64",
    "moon_phase": "float64",
    "wind_gust_spd": "float64",
    "snow_depth": "float64",
    "clouds": "int64",
    "ts": "int64",
    "sunrise_ts": "int64",
    "app_min_temp": "float64",
    "wind_spd": "float64",
    "pop": "int64",
    "wind_cdir_full": "object",
    "slp": "float64",
    "moon_phase_lunation": "float64",
    "valid_date": "object",
    "app_max_temp": "float64",
    "vis": "float64",
    "dewpt": "float64",
    "snow": "float64",
    "uv": "float64",
    "wind_dir": "int64",
    "max_dhi": "object",
    "clouds_hi": "int64",
    "precip": "float64",
    "low_temp": "float64",
    "max_temp": "float64",
    "moonset_ts": "int64",
    "datetime": "object",
    "temp": "float64",
    "min_temp": "float64",
    "clouds_mid": "int64",
    "clouds_low": "int64",
    "icon": "object",
    "code": "int64",
    "description": "object",
    "city_name": "object",
    "lon": "object",
    "timezone": "object",
    "lat": "object",
    "country_code": "object",
    "state_code": "object",
    "outcode": "object",
    "etlchange": "object",
}


class TestProcessHourlyWeatherData(unittest.TestCase):
    @mock_s3_deprecated
    @patch("uuid.uuid4", mock_uuid)
    def test_process_hourly_single_postcode(self):

        stage1_dir_s3 = dir_s3["s3_weather_key"]["forecast_weather"]["hourly"]["stage1"]
        stage2_dir_s3 = dir_s3["s3_weather_key"]["forecast_weather"]["hourly"]["stage2"]

        boto = weather_module.db.boto
        s3 = boto.connect_s3(aws_access_key_id="XXXX", aws_secret_access_key="XXXX")

        bucket = s3.create_bucket(bucket_name)
        s3_key = boto.s3.key.Key(bucket)

        hw = ForecastWeather(duration=120, forecast_resolution="hourly")
        hw.get_weather_postcode = MagicMock(return_value=postcodes)
        hw.get_api_response = MagicMock(return_value=api_returned_data_hourly_json)

        hw.processData()

        stage1_file_name = f"{postcodes[0].strip()}_hourly_{hw.start_date}_to_{hw.end_date}.json"

        s3_key.key = stage1_dir_s3.format(hw.extract_date) + stage1_file_name
        s3_stage1_object = json.loads(s3_key.get_contents_as_string().decode("utf-8"))

        stage2_file_name = "{}.parquet".format("00000000-0000-0000-0000-000000000002")
        s3_key.key = stage2_dir_s3.format(hw.extract_date) + stage2_file_name
        try:
            buf = io.BytesIO(s3_key.get_contents_as_string())
            df = pd.read_parquet(buf)
            for item in df.dtypes.items():
                actual_dtype = item[1]
                expected_dtype = hourly_dtypes[item[0]]
                assert actual_dtype == expected_dtype
        except Exception as e:
            print(e)
            self.fail("Stage 2 parquet file was not found")

        self.assertDictEqual(s3_stage1_object, api_returned_data_hourly)

    @mock_s3_deprecated
    @patch("uuid.uuid4", mock_uuid)
    def test_process_daily_single_postcode(self):
        stage1_dir_s3 = dir_s3["s3_weather_key"]["forecast_weather"]["daily"]["stage1"]
        stage2_dir_s3 = dir_s3["s3_weather_key"]["forecast_weather"]["daily"]["stage2"]

        boto = weather_module.db.boto
        s3 = boto.connect_s3(aws_access_key_id="XXXX", aws_secret_access_key="XXXX")

        bucket = s3.create_bucket(bucket_name)
        s3_key = boto.s3.key.Key(bucket)

        hw = ForecastWeather(duration=16, forecast_resolution="daily")
        hw.get_weather_postcode = MagicMock(return_value=postcodes)
        hw.get_api_response = MagicMock(return_value=api_returned_data_daily_json)

        hw.processData()

        stage1_file_name = f"{postcodes[0].strip()}_daily_{hw.start_date}_to_{hw.end_date}.json"

        s3_key.key = stage1_dir_s3.format(hw.extract_date) + stage1_file_name
        s3_stage1_object = json.loads(s3_key.get_contents_as_string().decode("utf-8"))

        stage2_file_name = "{}.parquet".format("00000000-0000-0000-0000-000000000001")
        s3_key.key = stage2_dir_s3.format(hw.extract_date) + stage2_file_name
        try:
            s3_key.get_contents_as_string()
            buf = io.BytesIO(s3_key.get_contents_as_string())
            df = pd.read_parquet(buf)
            for item in df.dtypes.items():
                actual_dtype = item[1]
                expected_dtype = daily_dtypes[item[0]]
                assert actual_dtype == expected_dtype
        except Exception:
            self.fail("Stage 2 parquet file was not found")

        self.assertDictEqual(s3_stage1_object, api_returned_data_daily)


if __name__ == "__main__":
    unittest.main()
