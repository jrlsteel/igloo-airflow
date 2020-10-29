import os
import sys
import timeit
import subprocess
from datetime import datetime

sys.path.append('..')
from common import utils as util
from common import Refresh_UAT as refresh
from common.directories import prod as prod_dir

script_name = os.path.basename(__file__)

class Weather:
    def __init__(
        self,
        all_jobid,
        process_name,
        process_job_script,
        store_job_script,
        process_weather_job_num,
        store_weather_job_num):

        self.process_name = process_name
        self.process_weather_job_script = process_job_script
        self.store_weather_job_script = store_job_script
        self.process_weather_job_num = process_weather_job_num
        self.store_weather_job_num = store_weather_job_num

        self.pythonAlias = util.get_pythonAlias()
        self.env = util.get_env()
        self.dir = util.get_dir()

        self.all_jobid = all_jobid
        self.process_weather_jobid = util.get_jobID()
        self.store_weather_jobid = util.get_jobID()

    def process(self):

        print("{0}: {1} job is running...".format(datetime.now().strftime('%H:%M:%S'), self.process_name))

        self.submit_process_weather_job()
        self.submit_store_weather_job()

        print("{0}: All {1} completed successfully".format(datetime.now().strftime('%H:%M:%S'), self.process_name))

    def submit_process_weather_job(self):
        """
        Calls the weather script defined by self.process_weather_job_script
        :return: None
        """

        print("{0}: >>>> Process {1}<<<<".format(datetime.now().strftime('%H:%M:%S'), self.process_name))
        try:
            util.batch_logging_insert(self.process_weather_jobid, self.process_weather_job_num, 'weather_forecast_extract_pyscript', script_name)
            start = timeit.default_timer()
            subprocess.run([self.pythonAlias, self.process_weather_job_script], check=True)
            util.batch_logging_update(self.process_weather_jobid, 'e')
            print("{0}: Processing of {2} Data completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'),
                                                                               float(timeit.default_timer() - start), self.process_name))
        except Exception as e:
            util.batch_logging_update(self.process_weather_jobid, 'f', str(e))
            util.batch_logging_update(self.all_jobid, 'f', str(e))
            print("Error in process :- " + str(e))
            sys.exit(1)

    def submit_store_weather_job(self):
        """
        Calls the weather script defined by self.store_weather_job_script
        :return: None
        """

        print("{0}: >>>> Process {1}<<<<".format(datetime.now().strftime('%H:%M:%S'), self.process_name))
        try:
            util.batch_logging_insert(self.store_weather_jobid, self.store_weather_job_num, 'weather_forecast_store_pyscript', script_name)
            start = timeit.default_timer()
            subprocess.run([self.pythonAlias, self.store_weather_job_script], check=True)
            util.batch_logging_update(self.store_weather_jobid, 'e')
            print("{0}: Processing of {2} Data completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'),
                                                                               float(timeit.default_timer() - start), self.process_name))
        except Exception as e:
            util.batch_logging_update(self.store_weather_jobid, 'f', str(e))
            util.batch_logging_update(self.all_jobid, 'f', str(e))
            print("Error in process :- " + str(e))
            sys.exit(1)

if __name__ == '__main__':

    all_jobid = util.get_jobID()

    util.batch_logging_insert(all_jobid, 109, 'all_weather_forecast_jobs', script_name)

    hourly_weather = Weather(
        all_jobid = all_jobid,
        process_name = 'Hourly Weather',
        process_job_script = 'processHourlyWeatherData.py',
        store_job_script = 'storeHourlyWeatherData.py',
        process_weather_job_num = 61,
        store_weather_job_num = 61,
    )

    hourly_weather.process()

    daily_weather = Weather(
        all_jobid = all_jobid,
        process_name = 'Daily Weather',
        process_job_script = 'processDailyWeatherData.py',
        store_job_script = 'storeDailyWeatherData.py',
        process_weather_job_num = 61,
        store_weather_job_num = 61,
    )

    daily_weather.process()

    util.batch_logging_update(all_jobid, 'e')
