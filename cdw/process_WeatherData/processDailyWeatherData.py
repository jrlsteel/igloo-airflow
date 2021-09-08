import timeit

from cdw.process_WeatherData.process_forecast_weather_data import ForecastWeather

p = ForecastWeather(duration=16, forecast_resolution="daily")

start = timeit.default_timer()

p.processData()

print("Process completed in " + str(timeit.default_timer() - start) + " seconds")
