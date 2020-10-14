import sys
import json
import unittest
from unittest.mock import MagicMock
from moto import mock_s3_deprecated

sys.path.append('..')

from processHourlyWeatherData import HourlyWeather
import processHourlyWeatherData as weather_module

api_returned_data = {
    "data": [
        {
            "wind_cdir": "N",
            "rh": 77,
            "pod": "d",
            "timestamp_utc": "2020-10-13T14: 00: 00",
            "pres": 1001.15,
            "solar_rad": 94.9475,
            "ozone": 318.88,
            "weather": {
                "icon": "r04d",
                "code": 520,
                "description": "Light shower rain"
            },
            "wind_gust_spd": 8.06146,
            "timestamp_local": "2020-10-13T15: 00: 00",
            "snow_depth": 0,
            "clouds": 100,
            "ts": 1602597600,
            "wind_spd": 2.82779,
            "pop": 35,
            "wind_cdir_full": "north",
            "slp": 1010.11,
            "dni": 730.6,
            "dewpt": 6.8,
            "snow": 0,
            "uv": 0.909345,
            "wind_dir": 4,
            "clouds_hi": 59,
            "precip": 0.5,
            "vis": 15.7569,
            "dhi": 85.58,
            "app_temp": 10.5,
            "datetime": "2020-10-13: 14",
            "temp": 10.5,
            "ghi": 379.79,
            "clouds_mid": 98,
            "clouds_low": 62
        }
    ],
    "city_name": "Hampshire",
    "lon": "-0.7908",
    "timezone": "Europe/London",
    "lat": "51.2955",
    "country_code": "GB",
    "state_code": "ENG"
}

postcodes=['GU14']
dir_s3 = weather_module.util.get_dir()
bucket_name = dir_s3['s3_bucket']

class TestProcessHourlyWeatherData(unittest.TestCase):

    @mock_s3_deprecated
    def test_process_single_postcode(self):

        hw = HourlyWeather()

        boto = weather_module.db.boto
        s3 = boto.connect_s3(aws_access_key_id='XXXX', aws_secret_access_key='XXXX')
        bucket = s3.create_bucket(bucket_name)
        s3_key = boto.s3.key.Key(bucket)
        
        file_name = f'{postcodes[0].strip()}_hourly_{hw.start_date}_to_{hw.end_date}.json'
        s3_key.key = dir_s3['s3_weather_key']['HourlyWeather'].format(hw.extract_date) + file_name

        hw.get_api_response = MagicMock(return_value=api_returned_data)
        
        hw.processData(postcodes, s3_key, dir_s3)

        s3_object = json.loads(s3_key.get_contents_as_string().decode('utf-8'))

        self.assertDictEqual(s3_object, api_returned_data)

if __name__ == '__main__':
    unittest.main()
