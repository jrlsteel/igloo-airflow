from datetime import datetime, timedelta
from lxml import etree
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from moto import mock_s3_deprecated
import csv
import json
import os
import sys
import pandas

from processHistoricalWeatherData import HistoricalWeather

from connections.connect_db import get_boto_S3_Connections as s3_con

fixtures = {
    "historical-weather": json.load(open(os.path.join(os.path.dirname(__file__), 'fixtures', 'historical-weather.json'))),
}


@mock_s3_deprecated
def test_extract_weather_data():
    s3_bucket_name = 'simulated-bucket'

    s3_connection = S3Connection()
    s3_bucket = s3_connection.create_bucket(s3_bucket_name)

    historical_weather = HistoricalWeather()

    data = historical_weather.format_json_response(
        fixtures['historical-weather'])

    postcode = 'SO16'
    start_date = datetime.fromisoformat('2020-11-03')
    end_date = datetime.fromisoformat('2020-11-04')

    k = s3_con(s3_bucket_name)
    dir_s3 = {
        "s3_weather_key": {
            "HistoricalWeather": "stage1/HistoricalWeather/",
        },
    }

    historical_weather.extract_weather_data(
        data, postcode, k, dir_s3, start_date, end_date)

    # Verify that the files created in S3 have the correct contents
    k = Key(s3_bucket)
    k.key = 'stage1/HistoricalWeather/historical_weather_SO16_2020_45.csv'
    csv_lines = k.get_contents_as_string(encoding='utf-8')

    # Construct a CSV reader to parse the file data for us, and verify that the
    # column headers are correct.
    reader = csv.reader(csv_lines.split('\n'), delimiter=',')

    column_headers = next(reader)
    assert(column_headers == ['azimuth', 'clouds', 'datetime', 'dewpt', 'dhi', 'dni', 'elev_angle', 'ghi', 'h_angle', 'pod', 'precip', 'pres', 'rh', 'slp', 'snow', 'solar_rad', 'temp', 'timestamp_local',
                              'timestamp_utc', 'ts', 'uv', 'vis', 'weather', 'wind_dir', 'wind_spd', 'timezone', 'state_code', 'country_code', 'lat', 'lon', 'city_name', 'station_id', 'city_id', 'sources', 'postcode'])
    assert(list(reader) == [
        ['4.54', '0', '2020-11-03:00', '2.7', '0.0', '0.0', '-54.11', '0.0', '-90', 'n', '0', '1012.3', '82', '1014', '0', '0.0', '7.8', '2020-11-03T00:00:00', '2020-11-03T00:00:00', '1604361600', '0.0', '5.0', "{'icon': 'c01n', 'code': 800, 'description': 'Clear Sky'}", '240', '2.18309', 'Europe/London', 'ENG', 'GB', '50.9363', '-1.4329', 'Southampton', '038650-99999', '2637487', '038650-99999 imerg merra2 era5 modis', 'SO16'],
        ['28.18', '12', '2020-11-03:01', '2.7', '0.0', '0.0', '-51.44', '0.0', '-90', 'n', '0', '1012.3', '84', '1014', '0', '0.0', '7.6', '2020-11-03T01:00:00', '2020-11-03T01:00:00', '1604365200', '0.0', '1.5', "{'icon': 'c02n', 'code': 801, 'description': 'Few clouds'}", '240', '1.61078', 'Europe/London', 'ENG', 'GB', '50.9363', '-1.4329', 'Southampton', '038650-99999', '2637487', '038650-99999 imerg merra2 era5 modis', 'SO16'],
        ['48.27', '26', '2020-11-03:02', '2.7', '0.0', '0.0', '-45.59', '0.0', '-90', 'n', '0', '1012.3', '85', '1014', '0', '0.0', '7.7', '2020-11-03T02:00:00', '2020-11-03T02:00:00', '1604368800', '0.0', '1.0', "{'icon': 'c02n', 'code': 802, 'description': 'Scattered clouds'}", '240', '1.51585', 'Europe/London', 'ENG', 'GB', '50.9363', '-1.4329', 'Southampton', '038650-99999', '2637487', '038650-99999 imerg merra2 era5 modis', 'SO16'],
        ['64.61', '100', '2020-11-03:03', '2.7', '0.0', '0.0', '-37.73', '0.0', '-90', 'n', '0', '1012.3', '85', '1014', '0', '0.0', '8.3', '2020-11-03T03:00:00', '2020-11-03T03:00:00', '1604372400', '0.0', '5.0', "{'icon': 'c04n', 'code': 804, 'description': 'Overcast clouds'}", '240', '2.72672', 'Europe/London', 'ENG', 'GB', '50.9363', '-1.4329', 'Southampton', '038650-99999', '2637487', '038650-99999 imerg merra2 era5 modis', 'SO16'],
        ['78.27', '100', '2020-11-03:04', '2.7', '0.0', '0.0', '-28.8', '0.0', '-90', 'n', '2', '1012.3', '88', '1014', '0', '0.0', '8.8', '2020-11-03T04:00:00', '2020-11-03T04:00:00', '1604376000', '0.0', '5.0', "{'icon': 'r01n', 'code': 500, 'description': 'Light rain'}", '240', '3.04498', 'Europe/London', 'ENG', 'GB', '50.9363', '-1.4329', 'Southampton', '038650-99999', '2637487', '038650-99999 imerg merra2 era5 modis', 'SO16'],
        ['90.37', '100', '2020-11-03:05', '2.7', '0.0', '0.0', '-19.42', '0.0', '-90', 'n', '0', '1012.3', '84', '1014', '0', '0.0', '9.9', '2020-11-03T05:00:00', '2020-11-03T05:00:00', '1604379600', '0.0', '5.0', "{'icon': 'c04n', 'code': 804, 'description': 'Overcast clouds'}", '240', '6.72288', 'Europe/London', 'ENG', 'GB', '50.9363', '-1.4329', 'Southampton', '038650-99999', '2637487', '038650-99999 imerg merra2 era5 modis', 'SO16'],
        ['101.79', '84', '2020-11-03:06', '2.7', '0.0', '0.0', '-10.05', '0.0', '-90', 'n', '0', '1012.3', '82', '1014', '0', '0.0', '9.9', '2020-11-03T06:00:00', '2020-11-03T06:00:00', '1604383200', '0.0', '5.0', "{'icon': 'c04n', 'code': 804, 'description': 'Overcast clouds'}", '240', '6.03479', 'Europe/London', 'ENG', 'GB', '50.9363', '-1.4329', 'Southampton', '038650-99999', '2637487', '038650-99999 imerg merra2 era5 modis', 'SO16'],
        ['113.2', '73', '2020-11-03:07', '7.8', '0.0', '0.0', '-1.06', '0.0', '-90', 'n', '0', '1010.3', '74', '1012', '0', '0.0', '10.0', '2020-11-03T07:00:00', '2020-11-03T07:00:00', '1604386800', '0.0', '5.0', "{'icon': 'c04n', 'code': 804, 'description': 'Overcast clouds'}", '240', '7.17377', 'Europe/London', 'ENG', 'GB', '50.9363', '-1.4329', 'Southampton', '038650-99999', '2637487', '038650-99999 imerg merra2 era5 modis', 'SO16'],
        ['125.13', '47', '2020-11-03:08', '6.0', '44.14', '390.69', '7.17', '83.73', '-90', 'd', '0', '1012.3', '74', '1014', '0', '78.9097', '8.0', '2020-11-03T08:00:00', '2020-11-03T08:00:00', '1604390400', '1.8', '0.5', "{'icon': 'c03d', 'code': 803, 'description': 'Broken clouds'}", '260', '7.21299', 'Europe/London', 'ENG', 'GB', '50.9363', '-1.4329', 'Southampton', '038650-99999', '2637487', '038650-99999 imerg merra2 era5 modis', 'SO16'],
        ['137.99', '42', '2020-11-03:09', '5.0', '65.74', '585.45', '14.23', '201.84', '-72', 'd', '0', '1015.3', '75', '1017', '0', '193.913', '7.0', '2020-11-03T09:00:00', '2020-11-03T09:00:00', '1604394000', '2.4', '0.5', "{'icon': 'c03d', 'code': 803, 'description': 'Broken clouds'}", '270', '5.60185', 'Europe/London', 'ENG', 'GB', '50.9363', '-1.4329', 'Southampton', '038650-99999', '2637487', '038650-99999 imerg merra2 era5 modis', 'SO16'],
        ['152.03', '61', '2020-11-03:10', '4.8', '77.97', '678.54', '19.63', '298.92', '-72', 'd', '0', '1017.3', '70', '1019', '0', '257.162', '8.0', '2020-11-03T10:00:00', '2020-11-03T10:00:00', '1604397600', '2.3', '0.5', "{'icon': 'c03d', 'code': 803, 'description': 'Broken clouds'}", '240', '7.01867', 'Europe/London', 'ENG', 'GB', '50.9363', '-1.4329', 'Southampton', '038650-99999', '2637487', '038650-99999 imerg merra2 era5 modis', 'SO16'],
        ['167.16', '70', '2020-11-03:11', '3.7', '84.25', '722.09', '22.92', '358.68', '-54', 'd', '0', '1017.3', '69', '1019', '0', '278.678', '9.0', '2020-11-03T11:00:00', '2020-11-03T11:00:00', '1604401200', '2.2', '0.5', "{'icon': 'c03d', 'code': 803, 'description': 'Broken clouds'}", '250', '6.24443', 'Europe/London', 'ENG', 'GB', '50.9363', '-1.4329', 'Southampton', '038650-99999', '2637487', '038650-99999 imerg merra2 era5 modis', 'SO16'],
        ['182.9', '62', '2020-11-03:12', '5.0', '85.69', '731.67', '23.73', '373.35', '-36', 'd', '0', '1018.3', '63', '1020', '0', '318.23', '10.0', '2020-11-03T12:00:00', '2020-11-03T12:00:00', '1604404800', '2.5', '0.5', "{'icon': 'c03d', 'code': 803, 'description': 'Broken clouds'}", '260', '6.32355', 'Europe/London', 'ENG', 'GB', '50.9363', '-1.4329', 'Southampton', '038650-99999', '2637487', '038650-99999 imerg merra2 era5 modis', 'SO16'],
        ['198.49', '36', '2020-11-03:13', '4.8', '82.49', '710.13', '21.96', '341.17', '0', 'd', '0', '1018.3', '61', '1020', '0', '333.237', '11.0', '2020-11-03T13:00:00', '2020-11-03T13:00:00', '1604408400', '3.2', '1.0', "{'icon': 'c02d', 'code': 802, 'description': 'Scattered clouds'}", '260', '6.64062', 'Europe/London', 'ENG', 'GB', '50.9363', '-1.4329', 'Southampton', '038650-99999', '2637487', '038650-99999 imerg merra2 era5 modis', 'SO16'],
        ['213.24', '56', '2020-11-03:14', '4.8', '74.16', '650.77', '17.82', '266.06', '18', 'd', '0', '1019.3', '63', '1021', '0', '238.271', '11.0', '2020-11-03T14:00:00', '2020-11-03T14:00:00', '1604412000', '2.3', '0.5', "{'icon': 'c03d', 'code': 803, 'description': 'Broken clouds'}", '260', '6.19552', 'Europe/London', 'ENG', 'GB', '50.9363', '-1.4329', 'Southampton', '038650-99999', '2637487', '038650-99999 imerg merra2 era5 modis', 'SO16'],
        ['226.82', '42', '2020-11-03:15', '4.8', '59.03', '529.33', '11.73', '158.42', '18', 'd', '0', '1019.3', '65', '1021', '0', '152.198', '10.7', '2020-11-03T15:00:00', '2020-11-03T15:00:00', '1604415600', '2.2', '0.5', "{'icon': 'c03d', 'code': 803, 'description': 'Broken clouds'}", '260', '5.6014', 'Europe/London', 'ENG', 'GB', '50.9363', '-1.4329', 'Southampton', '038650-99999', '2637487', '038650-99999 imerg merra2 era5 modis', 'SO16'],
        ['239.28', '34', '2020-11-03:16', '3.9', '31.4', '257.31', '4.18', '40.64', '54', 'n', '0', '1020.3', '71', '1022', '0', '39.8619', '10.0', '2020-11-03T16:00:00', '2020-11-03T16:00:00', '1604419200', '1.9', '1.0', "{'icon': 'c02n', 'code': 802, 'description': 'Scattered clouds'}", '250', '4.71481', 'Europe/London', 'ENG', 'GB', '50.9363', '-1.4329', 'Southampton', '038650-99999', '2637487', '038650-99999 imerg merra2 era5 modis', 'SO16'],
        ['250.94', '46', '2020-11-03:17', '5.0', '0.0', '0.0', '-4.4', '0.0', '54', 'n', '0', '1020.3', '74', '1022', '0', '0.0', '10.0', '2020-11-03T17:00:00', '2020-11-03T17:00:00', '1604422800', '0.0', '0.5', "{'icon': 'c03n', 'code': 803, 'description': 'Broken clouds'}", '260', '4.06618', 'Europe/London', 'ENG', 'GB', '50.9363', '-1.4329', 'Southampton', '038650-99999', '2637487', '038650-99999 imerg merra2 era5 modis', 'SO16'],
        ['262.26', '55', '2020-11-03:18', '3.7', '0.0', '0.0', '-13.58', '0.0', '72', 'n', '0', '1021.3', '77', '1023', '0', '0.0', '9.0', '2020-11-03T18:00:00', '2020-11-03T18:00:00', '1604426400', '0.0', '0.5', "{'icon': 'c03n', 'code': 803, 'description': 'Broken clouds'}", '250', '3.48123', 'Europe/London', 'ENG', 'GB', '50.9363', '-1.4329', 'Southampton', '038650-99999', '2637487', '038650-99999 imerg merra2 era5 modis', 'SO16'],
        ['273.82', '48', '2020-11-03:19', '3.7', '0.0', '0.0', '-23.02', '0.0', '-90', 'n', '0', '1022.3', '75', '1024', '0', '0.0', '8.0', '2020-11-03T19:00:00', '2020-11-03T19:00:00', '1604430000', '0.0', '0.5', "{'icon': 'c03n', 'code': 803, 'description': 'Broken clouds'}", '240', '2.75878', 'Europe/London', 'ENG', 'GB', '50.9363', '-1.4329', 'Southampton', '038650-99999', '2637487', '038650-99999 imerg merra2 era5 modis', 'SO16'],
        ['286.36', '43', '2020-11-03:20', '4.0', '0.0', '0.0', '-32.32', '0.0', '-90', 'n', '0', '1022.3', '78', '1024', '0', '0.0', '7.0', '2020-11-03T20:00:00', '2020-11-03T20:00:00', '1604433600', '0.0', '0.5', "{'icon': 'c03n', 'code': 803, 'description': 'Broken clouds'}", '0', '2.10169', 'Europe/London', 'ENG', 'GB', '50.9363', '-1.4329', 'Southampton', '038650-99999', '2637487', '038650-99999 imerg merra2 era5 modis', 'SO16'],
        ['300.84', '35', '2020-11-03:21', '4.0', '0.0', '0.0', '-40.98', '0.0', '-90', 'n', '0', '1023.3', '82', '1025', '0', '0.0', '6.9', '2020-11-03T21:00:00', '2020-11-03T21:00:00', '1604437200', '0.0', '1.0', "{'icon': 'c02n', 'code': 802, 'description': 'Scattered clouds'}", '240', '1.69945', 'Europe/London', 'ENG', 'GB', '50.9363', '-1.4329', 'Southampton', '038650-99999', '2637487', '038650-99999 imerg merra2 era5 modis', 'SO16'],
        ['318.46', '42', '2020-11-03:22', '4.0', '0.0', '0.0', '-48.27', '0.0', '-90', 'n', '0', '1023.3', '84', '1025', '0', '0.0', '6.5', '2020-11-03T22:00:00', '2020-11-03T22:00:00', '1604440800', '0.0', '0.5', "{'icon': 'c03n', 'code': 803, 'description': 'Broken clouds'}", '240', '1.23889', 'Europe/London', 'ENG', 'GB', '50.9363', '-1.4329', 'Southampton', '038650-99999', '2637487', '038650-99999 imerg merra2 era5 modis', 'SO16'],
        ['340.07', '52', '2020-11-03:23', '4.0', '0.0', '0.0', '-53.13', '0.0', '-90', 'n', '0', '1023.3', '87', '1025', '0', '0.0', '5.9', '2020-11-03T23:00:00', '2020-11-03T23:00:00', '1604444400', '0.0', '0.5', "{'icon': 'c03n', 'code': 803, 'description': 'Broken clouds'}", '240', '0.722436', 'Europe/London', 'ENG', 'GB', '50.9363', '-1.4329', 'Southampton', '038650-99999', '2637487', '038650-99999 imerg merra2 era5 modis', 'SO16'],
        []
    ])
