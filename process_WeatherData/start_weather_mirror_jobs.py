import sys
from datetime import datetime
import timeit
import subprocess

sys.path.append("..")
from common import process_glue_job as glue
from common import utils as util
from common import Refresh_UAT as refresh
from conf import config as con


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

    def submit_process_s3_mirror_job(self, source_input, destination_input):
        """
        Calls the utils/Refresh_UAT.py script which mirrors s3 data from source to destination fdlder
        :return: None
        """

        print("{0}: >>>> Process {1}<<<<".format(datetime.now().strftime("%H:%M:%S"), self.process_name))
        try:
            util.batch_logging_insert(
                self.mirror_jobid,
                21,
                "weather_extract_mirror-" + source_input + "-" + self.env,
                "start_weather_jobs.py",
            )
            start = timeit.default_timer()
            r = refresh.SyncS3(source_input, destination_input)

            r.process_sync(
                env={
                    "AWS_ACCESS_KEY_ID": con.s3_config["access_key"],
                    "AWS_SECRET_ACCESS_KEY": con.s3_config["secret_key"],
                }
            )

            util.batch_logging_update(self.mirror_jobid, "e")
            print(
                "weather_extract_mirror-"
                + source_input
                + "-"
                + self.env
                + " files completed in {1:.2f} seconds".format(
                    datetime.now().strftime("%H:%M:%S"), float(timeit.default_timer() - start)
                )
            )
        except Exception as e:
            util.batch_logging_update(self.mirror_jobid, "f", str(e))
            util.batch_logging_update(self.all_jobid, "f", str(e))
            print("Error in process :- " + str(e))
            sys.exit(1)

    def submit_process_weather_job(self):
        """
        Calls the processHistoricalWeatherData.py script which processes the weather data from Weatherbit.io for the past 1 year
        :return: None
        """

        print("{0}: >>>> Process {1}<<<<".format(datetime.now().strftime("%H:%M:%S"), self.process_name))
        try:
            util.batch_logging_insert(
                self.weather_jobid,
                21,
                "weather_extract_pyscript",
                "start_weather_jobs.py",
            )
            start = timeit.default_timer()
            subprocess.run([self.pythonAlias, "processHistoricalWeatherData.py"], check=True)
            util.batch_logging_update(self.weather_jobid, "e")
            print(
                "{0}: Processing of {2} Data completed in {1:.2f} seconds".format(
                    datetime.now().strftime("%H:%M:%S"),
                    float(timeit.default_timer() - start),
                    self.process_name,
                )
            )
        except Exception as e:
            util.batch_logging_update(self.weather_jobid, "f", str(e))
            util.batch_logging_update(self.all_jobid, "f", str(e))
            print("Error in process :- " + str(e))
            sys.exit(1)


if __name__ == "__main__":

    s = Weather()

    util.batch_logging_insert(s.all_jobid, 107, "all_weather_jobs", "start_weather_jobs.py")

    master_source = util.get_master_source("weather_historical")
    current_env = util.get_env()
    print("Current environment: {0}, Master_Source: {1}".format(current_env, master_source))

    if master_source == current_env:  # current environment is master source, run the data extract script
        # run processing weather python script
        print("{0}: {1} job is running...".format(datetime.now().strftime("%H:%M:%S"), s.process_name))
        s.submit_process_weather_job()
    else:
        # # run processing mirror job
        print("Weather Mirror job is running...".format(datetime.now().strftime("%H:%M:%S"), s.process_name))
        dest_dir = util.get_dir()
        dest_prefix = dest_dir["s3_weather_key"]["HistoricalWeather"]
        destination = "s3://{}/{}".format(dest_dir["s3_bucket"], dest_prefix)

        source_dir = util.get_dir(master_source)
        source_prefix = source_dir["s3_weather_key"]["HistoricalWeather"]
        source = "s3://{}/{}".format(source_dir["s3_bucket"], source_prefix)

        s.submit_process_s3_mirror_job(source, destination)

    print("{0}: All {1} completed successfully".format(datetime.now().strftime("%H:%M:%S"), s.process_name))

    util.batch_logging_update(s.all_jobid, "e")
