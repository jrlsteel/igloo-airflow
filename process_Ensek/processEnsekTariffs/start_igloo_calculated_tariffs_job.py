import sys

from datetime import datetime
import timeit
import subprocess

sys.path.append('..')
from common import process_glue_job as glue
from common import utils as util


class IglooCalculatedTariffsJobs:
    def __init__(self):
        self.pythonAlias = util.get_pythonAlias()
        self.env = util.get_env()
        self.dir = util.get_dir()
        self.all_jobid = util.get_jobID()
        self.igloo_calculated_tariffs_jobid = util.get_jobID()

    def submit_igloo_calculated_tariffs_gluejob(self):
        try:
            jobName = self.dir['glue_igloo_calculated_tariffs_job_name']
            s3_bucket = self.dir['s3_bucket']
            environment = self.env

            #Batch Logging
            util.batch_logging_insert(self.igloo_calculated_tariffs_jobid, 52, 'igloo_calculated_tariffs_gluejob', 'start_igloo_calculated_tariffs_jobs.py')

            obj_igloo_calculated_tariffs = glue.ProcessGlueJob(job_name=jobName, s3_bucket=s3_bucket, environment=environment, processJob='tariff_calc')
            igloo_calculated_tariffs_job_response = obj_igloo_calculated_tariffs.run_glue_job()

            if igloo_calculated_tariffs_job_response:

                print("{0}: Smart Igloo Calculated Tariffs job completed successfully".format(datetime.now().strftime('%H:%M:%S')))

                # Batch Logging
                util.batch_logging_update(self.igloo_calculated_tariffs_jobid, 'e')

            else:

                print("Error occurred in Igloo Calculated Tariffs job")
                # return staging_job_response
                raise Exception
        except Exception as e:
            print("Error in Igloo Calculated Tariffs glue job Job :- " + str(e))

            # Batch Logging
            util.batch_logging_update(self.igloo_calculated_tariffs_jobid, 'f',  str(e))

            # write
            sys.exit(1)


if __name__ == '__main__':

    s = IglooCalculatedTariffsJobs()

    util.batch_logging_insert(s.all_jobid, 52, 'all_igloo_calculated_tariffs_jobs', 'start_igloo_calculated_tariffs_jobs.py')

    # run reference smart meter eligibility glue job
    print("{0}: Igloo Calculated Tariffs glue job running...".format(datetime.now().strftime('%H:%M:%S')))
    s.submit_igloo_calculated_tariffs_gluejob()

    util.batch_logging_update(s.all_jobid, 'e')
