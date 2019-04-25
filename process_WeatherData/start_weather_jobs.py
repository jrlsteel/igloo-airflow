import sys
from datetime import datetime
import timeit
import subprocess

sys.path.append('..')
from common import process_glue_job as glue
from common import utils as util


class Weather:
    def __init__(self):
        self.process_name = "Historical Weather"
        self.pythonAlias = util.get_pythonAlias()
        self.env = util.get_env()
        self.dir = util.get_dir()
        self.weather_jobid = util.get_jobID()
        self.weather_staging_jobid = util.get_jobID()
        self.weather_ref_jobid = util.get_jobID()

    def submit_process_weather_job(self):
        """
        Calls the processHistoricalWeatherData.py script which processes the weather data from Weatherbit.io for the past 1 year
        :return: None
        """

        print("{0}: >>>> Process {1}<<<<".format(datetime.now().strftime('%H:%M:%S'), self.process_name))
        try:
            util.batch_logging_insert(self.weather_jobid, 21, 'weather_extract_pyscript','start_weather_jobs.py')
            start = timeit.default_timer()
            subprocess.run([self.pythonAlias, "processHistoricalWeatherData.py"])
            util.batch_logging_update(self.weather_jobid, 'e')
            print("{0}: Processing of {2} Data completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'),
                                                                               float(timeit.default_timer() - start), self.process_name))
        except Exception as e:
            util.batch_logging_update(self.weather_jobid, 'f', str(e))
            print("Error in process :- " + str(e))
            sys.exit(1)

    def submit_staging_gluejob(self):
        try:
            util.batch_logging_insert(self.weather_staging_jobid, 22, 'weather_staging_glue_job','start_weather_jobs.py')
            jobName = self.dir['glue_staging_job_name']
            s3_bucket = self.dir['s3_bucket']
            environment = self.env
            obj_stage = glue.ProcessGlueJob(job_name=jobName, s3_bucket=s3_bucket, environment=environment, processJob='historicalweather')
            staging_job_response = obj_stage.run_glue_job()
            if staging_job_response:
                util.batch_logging_update(self.weather_staging_jobid, 'e')
                print("{0}: Staging Job Completed successfully for {1}".format(datetime.now().strftime('%H:%M:%S'), self.process_name))
                # return staging_job_response
            else:
                print("Error occurred in {0} Staging Job".format(self.process_name))
                # return staging_job_response
                raise Exception
        except Exception as e:
            util.batch_logging_update(self.weather_staging_jobid, 'f', str(e))
            print("Error in Staging Job :- " + str(e))
            sys.exit(1)

    def submit_ref_gluejob(self):
        try:
            util.batch_logging_insert(self.weather_ref_jobid, 23, 'weather_ref_glue_job','start_weather_jobs.py')
            jobName = self.dir['glue_historicalweather_jobname']
            s3_bucket = self.dir['s3_bucket']
            environment = self.env
            obj = glue.ProcessGlueJob(job_name=jobName, s3_bucket=s3_bucket, environment=environment, processJob='historicalweather')
            job_response = obj.run_glue_job()
            if job_response:
                util.batch_logging_update(self.weather_ref_jobid, 'e')
                print("{0}: Ref Glue Job Completed successfully for {1}".format(datetime.now().strftime('%H:%M:%S'), self.process_name))
                # return staging_job_response
            else:
                print("Error occurred in {0} Job".format(self.process_name))
                # return staging_job_response
                raise Exception
        except Exception as e:
            util.batch_logging_update(self.weather_ref_jobid, 'f', str(e))
            print("Error in Ref Glue Job :- " + str(e))
            sys.exit(1)


if __name__ == '__main__':

    s = Weather()

    # run processing weather python script
    print("{0}: {1} job is running...".format(datetime.now().strftime('%H:%M:%S'), s.process_name))
    s.submit_process_weather_job()

    # run staging glue job
    print("{0}: Staging Job running for {1}...".format(datetime.now().strftime('%H:%M:%S'), s.process_name))
    s.submit_staging_gluejob()

    # run ref glue job
    print("{0}: Glue Job running for {1}...".format(datetime.now().strftime('%H:%M:%S'), s.process_name))
    s.submit_ref_gluejob()

    print("{0}: All {1} completed successfully".format(datetime.now().strftime('%H:%M:%S'), s.process_name))

