import sys
from datetime import datetime
import timeit
import subprocess

sys.path.append('..')
from common import process_glue_job as glue
from common import utils as util
from common import Refresh_UAT as refresh
from common.directories import prod as prod_dir

class Weather:
    def __init__(
        self,
        all_jobid,
        process_name,
        job_script,
        glue_job,
        s3_key,
        mirror_job_num,
        weather_job_num,
        staging_glue_job_num,
        ref_glue_job_num):

        self.process_name = process_name 
        self.weather_job_script = job_script
        self.glue_job = glue_job
        self.s3_key = s3_key
        self.mirror_job_num = mirror_job_num
        self.weather_job_num = weather_job_num
        self.staging_job_num = staging_glue_job_num
        self.ref_job_num = ref_glue_job_num


        self.pythonAlias = util.get_pythonAlias()
        self.env = util.get_env()
        self.dir = util.get_dir()

        self.all_jobid = all_jobid
        self.weather_jobid = util.get_jobID()
        self.weather_staging_jobid = util.get_jobID()
        self.weather_ref_jobid = util.get_jobID()
        self.mirror_jobid = util.get_jobID()

    def process(self):

        if self.env == 'prod':
            # run processing weather python script
            print("{0}: {1} job is running...".format(datetime.now().strftime('%H:%M:%S'), self.process_name))
            self.submit_process_weather_job()

        else:
            # # run processing mirror job
            print("{0}: Weather Mirror job {1} is running...".format(datetime.now().strftime('%H:%M:%S'), self.process_name))
            source_input = f"{prod_dir['s3_bucket']}{self.s3_key}"
            destination_input = f"{self.dir['s3_bucket']}{self.s3_key}"
            self.submit_process_s3_mirror_job(source_input, destination_input)


        # run staging glue job
        print("{0}: Staging Job running for {1}...".format(datetime.now().strftime('%H:%M:%S'), self.process_name))
        self.submit_staging_gluejob()

        # run ref glue job
        print("{0}: Glue Job running for {1}...".format(datetime.now().strftime('%H:%M:%S'), self.process_name))
        self.submit_ref_gluejob()

        print("{0}: All {1} completed successfully".format(datetime.now().strftime('%H:%M:%S'), self.process_name))

    def submit_process_s3_mirror_job(self, source_input, destination_input):
            """
            Calls the utils/Refresh_UAT.py script which mirrors s3 data from source to destination fdlder
            :return: None
            """

            print("{0}: >>>> Process {1}<<<<".format(datetime.now().strftime('%H:%M:%S'), self.process_name))
            try:
                util.batch_logging_insert(self.mirror_jobid, self.mirror_job_num, 'weather_extract_mirror-' + source_input + '-' + self.env,
                                          'start_weather_jobs.py')
                start = timeit.default_timer()
                r = refresh.SyncS3(source_input, destination_input)
                r.process_sync()

                util.batch_logging_update(self.mirror_jobid, 'e')
                print(
                    "{time}: weather_extract_mirror-" + source_input + "-" + self.env + " files completed in {duration} seconds".format(
                        time=datetime.now().strftime('%H:%M:%S'), duration=float(timeit.default_timer() - start)))
            except Exception as e:
                util.batch_logging_update(self.mirror_jobid, 'f', str(e))
                util.batch_logging_update(self.all_jobid, 'f', str(e))
                print("Error in process :- " + str(e))
                sys.exit(1)

    def submit_process_weather_job(self):
        """
        Calls the weather script defined by self.weather_job_script
        :return: None
        """

        print("{0}: >>>> Process {1}<<<<".format(datetime.now().strftime('%H:%M:%S'), self.process_name))
        try:
            util.batch_logging_insert(self.weather_jobid, self.mirror_job_num, 'weather_extract_pyscript','start_weather_jobs.py')
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

    def submit_staging_gluejob(self):
        try:
            util.batch_logging_insert(self.weather_staging_jobid, self.staging_job_num, 'weather_staging_glue_job','start_weather_jobs.py')
            jobName = self.dir['glue_staging_job_name']
            s3_bucket = self.dir['s3_bucket']
            environment = self.env
            obj_stage = glue.ProcessGlueJob(job_name=jobName, s3_bucket=s3_bucket, environment=environment, processJob=self.glue_job)
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
            util.batch_logging_update(self.all_jobid, 'f', str(e))
            print("Error in Staging Job :- " + str(e))
            sys.exit(1)

    def submit_ref_gluejob(self):
        try:
            util.batch_logging_insert(self.weather_ref_jobid, self.ref_job_num, 'weather_ref_glue_job','start_weather_jobs.py')
            jobName = self.dir['glue_weather_jobname']
            s3_bucket = self.dir['s3_bucket']
            environment = self.env
            obj = glue.ProcessGlueJob(job_name=jobName, s3_bucket=s3_bucket, environment=environment, processJob=self.glue_job)
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
            util.batch_logging_update(self.all_jobid, 'f', str(e))
            print("Error in Ref Glue Job :- " + str(e))
            sys.exit(1)


if __name__ == '__main__':

    all_jobid = util.get_jobID()

    util.batch_logging_insert(all_jobid, 107, 'all_weather_jobs', 'start_weather_jobs.py')
    
    # historical_weather = Weather(
    #     all_jobid = all_jobid,
    #     process_name = 'Historical Weather',
    #     job_script = 'processHistoricalWeatherData.py',
    #     glue_job = 'historicalweather',
    #     s3_key = '/stage1/HistoricalWeather/',
    #     mirror_job_num = 21,
    #     weather_job_num = 21,
    #     staging_glue_job_num = 22,
    #     ref_glue_job_num = 23
    # )

    # historical_weather.process()
    
    hourly_weather = Weather(
        all_jobid = all_jobid,
        process_name = 'Hourly Weather',
        job_script = 'processHourlyWeatherData.py',
        glue_job = 'hourlyweather',
        s3_key = '/stage1/HourlyWeather/',
        mirror_job_num = 61,
        weather_job_num = 61,
        staging_glue_job_num = 61,
        ref_glue_job_num = 61
    )

    hourly_weather.process()

    util.batch_logging_update(all_jobid, 'e')