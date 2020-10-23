import sys
import os
import timeit
import time
from datetime import datetime, timedelta

sys.path.append('..')

from conf import config as con
from common import api_filters as apif
from connections import connect_db as db


class HourlyWeather:

    def __init__(self):

        self.weather_sql = apif.weather_forecast['hourly']['stage2']

    def get_dataframe_from_view(self) :
        pr = db.get_redshift_connection()
        weather_df = pr.redshift_to_pandas(self.weather_sql)
        db.close_redshift_connection()

        return weather_df

    def copy_dataframe_to_redshift(self, dataframe):
        pr = db.get_redshift_connection()

        pr.pandas_to_redshift(
            data_frame = dataframe,
            redshift_table_name = 'ref_weather_forecast_hourly'
        )

        pr.close_up_shop()

if __name__ == "__main__":

    p = HourlyWeather()

    start = timeit.default_timer()

    dataframe = p.get_dataframe_from_view()
    p.copy_dataframe_to_redshift(dataframe)

    print("Process completed in " + str(timeit.default_timer() - start) + ' seconds')
