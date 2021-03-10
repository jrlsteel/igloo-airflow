import io
import timeit
import json
import time
import csv
import sys
import timeit

sys.path.append('..')
from process_forecast_weather_data import ForecastWeather

p = ForecastWeather(duration=120, forecast_resolution='hourly')

start = timeit.default_timer()

p.processData()

print("Process completed in " + str(timeit.default_timer() - start) + ' seconds')
