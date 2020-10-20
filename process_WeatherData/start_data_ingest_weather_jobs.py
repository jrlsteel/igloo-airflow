import sys
import timeit
import subprocess
from datetime import datetime

sys.path.append('..')
from common import utils as util
from common import Refresh_UAT as refresh
from common.directories import prod as prod_dir

class Weather:
    def __init__(
        self,
        all_jobid,
        process_name,
        job_script,
        s3_key,
        weather_job_num):

        self.process_name = process_name 
        self.weather_job_script = job_script
        self.s3_key = s3_key
        self.weather_job_num = weather_job_num

        self.pythonAlias = util.get_pythonAlias()
        self.env = util.get_env()
        self.dir = util.get_dir()

        self.all_jobid = all_jobid
        self.weather_jobid = util.get_jobID()

    def process(self):

        print("{0}: {1} job is running...".format(datetime.now().strftime('%H:%M:%S'), self.process_name))

        self.submit_process_weather_job()

        print("{0}: All {1} completed successfully".format(datetime.now().strftime('%H:%M:%S'), self.process_name))

    def submit_process_weather_job(self):
        """
        Calls the weather script defined by self.weather_job_script
        :return: None
        """

        print("{0}: >>>> Process {1}<<<<".format(datetime.now().strftime('%H:%M:%S'), self.process_name))
        try:
            util.batch_logging_insert(self.weather_jobid, self.weather_job_num, 'weather_forecast_extract_pyscript','start_data_ingest_weather_jobs.py')
            start = timeit.default_timer()
            subprocess.run([self.pythonAlias, self.weather_job_script], check=True)
            util.batch_logging_update(self.weather_jobid, 'e')
            print("{0}: Processing of {2} Data completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'),
                                                                               float(timeit.default_timer() - start), self.process_name))
        except Exception as e:
            util.batch_logging_update(self.weather_jobid, 'f', str(e))
            util.batch_logging_update(self.all_jobid, 'f', str(e))
            print("Error in process :- " + str(e))
            sys.exit(1)

if __name__ == '__main__':

    all_jobid = util.get_jobID()

    util.batch_logging_insert(all_jobid, 109, 'all_weather_forecast_jobs', 'start_data_ingest_weather_jobs.py')
    
    hourly_weather = Weather(
        all_jobid = all_jobid,
        process_name = 'Hourly Weather',
        job_script = 'processHourlyWeatherData.py',
        s3_key = '/stage1/HourlyWeather/',
        weather_job_num = 61,
    )

    hourly_weather.process()

    util.batch_logging_update(all_jobid, 'e')
