import sys
import os
import timeit
import time
from datetime import datetime, timedelta

sys.path.append('..')

from conf import config as con
from common import api_filters as apif
from common import utils as util
from connections import connect_db as db


class ForecastWeather:

    def __init__(
        self,
        forecast_resolution='hourly'):

        dirs = util.get_dir()

        self.weather_sql = apif.weather_forecast[forecast_resolution]['stage2']
        self.redshift_table = dirs['apis']['forecast_weather'][forecast_resolution]['redshift_table']

    def get_dataframe_from_view(self) :
        pr = db.get_redshift_connection()
        weather_df = pr.redshift_to_pandas(self.weather_sql)
        db.close_redshift_connection()

        return weather_df

    def copy_dataframe_to_redshift(self, dataframe):
        pr = db.get_redshift_connection()

        pr.pandas_to_redshift(
            data_frame = dataframe,
            redshift_table_name = self.redshift_table
        )

        pr.close_up_shop()
