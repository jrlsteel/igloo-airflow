import sys
import timeit

sys.path.append('..')

from conf import config as con
from common import api_filters as apif
from connections import connect_db as db
from store_forecast_weather_data import ForecastWeather


p = ForecastWeather(forecast_resolution='hourly')

start = timeit.default_timer()

dataframe = p.get_dataframe_from_view()
p.copy_dataframe_to_redshift(dataframe)

print("Process completed in " + str(timeit.default_timer() - start) + ' seconds')
