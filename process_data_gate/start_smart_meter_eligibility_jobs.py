import sys

from datetime import datetime
import timeit
import subprocess

sys.path.append('..')
from common import process_glue_job as glue
from common import utils as util


class SmartMeterEligibilityJobs:
    def __init__(self):
        self.pythonAlias = util.get_pythonAlias()
        self.env = util.get_env()
        self.dir = util.get_dir()
        self.all_jobid = util.get_jobID()
        self.smart_meter_eligibility_jobid = util.get_jobID()

    def submit_smart_meter_eligibility_gluejob(self):
        try:
            jobName = self.dir['glue_smart_meter_eligibility_job_name']
            s3_bucket = self.dir['s3_bucket']
            environment = self.env

            #Batch Logging
            util.batch_logging_insert(self.smart_meter_eligibility_jobid, 1, 'smart_meter_eligibility_gluejob', 'start_smart_meter_eligibility_jobs.py')

            obj_smart_meter_eligibility = glue.ProcessGlueJob(job_name=jobName, s3_bucket=s3_bucket, environment=environment, processJob='smart_eli')
            smart_meter_eligibility_job_response = obj_smart_meter_eligibility.run_glue_job()

            if smart_meter_eligibility_job_response:

                print("{0}: Smart Meter Eligibility job completed successfully".format(datetime.now().strftime('%H:%M:%S')))

                # Batch Logging
                util.batch_logging_update(self.smart_meter_eligibility_jobid, 'e')

            else:

                print("Error occurred in Smart Meter Eligibility job")
                # return staging_job_response
                raise Exception
        except Exception as e:
            print("Error in Smart Meter Eligibility glue job Job :- " + str(e))

            # Batch Logging
            util.batch_logging_update(self.smart_meter_eligibility_jobid, 'f',  str(e))

            # write
            sys.exit(1)


if __name__ == '__main__':

    s = SmartMeterEligibilityJobs()

    util.batch_logging_insert(s.all_jobid, 1, 'all_smart_meter_eligibility_jobs', 'start_smart_meter_eligibility_jobs.py')

    # run reference smart meter eligibility glue job
    print("{0}: Smart Meter Eligibility glue job running...".format(datetime.now().strftime('%H:%M:%S')))
    s.submit_smart_meter_eligibility_gluejob()

    util.batch_logging_update(s.all_jobid, 'e')
