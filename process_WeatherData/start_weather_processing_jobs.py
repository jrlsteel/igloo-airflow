import sys
from datetime import datetime
import timeit
import subprocess

sys.path.append('..')
from common import process_glue_job as glue
from common import utils as util
from common import Refresh_UAT as refresh

class Weather:
    def __init__(self):
        self.process_name = "Historical Weather"
        self.pythonAlias = util.get_pythonAlias()
        self.env = util.get_env()
        self.dir = util.get_dir()

        self.all_jobid = util.get_jobID()
        self.weather_jobid = util.get_jobID()
        self.weather_staging_jobid = util.get_jobID()
        self.weather_ref_jobid = util.get_jobID()
        self.mirror_jobid = util.get_jobID()



    def submit_process_weather_job(self):
        """
        Calls the processHistoricalWeatherData.py script which processes the weather data from Weatherbit.io for the past 1 year
        :return: None
        """

        print("{0}: >>>> Process {1}<<<<".format(datetime.now().strftime('%H:%M:%S'), self.process_name))
        try:
            util.batch_logging_insert(self.weather_jobid, 21, 'weather_extract_pyscript','start_weather_jobs.py')
            start = timeit.default_timer()
            subprocess.run([self.pythonAlias, "processHistoricalWeatherData.py"], check=True)
            util.batch_logging_update(self.weather_jobid, 'e')
            print("{0}: Processing of {2} Data completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'),
                                                                               float(timeit.default_timer() - start), self.process_name))
        except Exception as e:
            util.batch_logging_update(self.weather_jobid, 'f', str(e))
            util.batch_logging_update(self.all_jobid, 'f', str(e))
            print("Error in process :- " + str(e))
            sys.exit(1)


if __name__ == '__main__':

    s = Weather()

    util.batch_logging_insert(s.all_jobid, 107, 'all_weather_jobs', 'start_weather_jobs.py')

    s.submit_process_weather_job()

    util.batch_logging_update(s.all_jobid, 'e')