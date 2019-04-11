import sys
import uuid

from datetime import datetime
import timeit
import subprocess

sys.path.append('..')
from common import process_glue_job as glue
from common import utils as util


class TADOEfficiencyJobs:
    def __init__(self):
        self.pythonAlias = util.get_pythonAlias()
        self.env = util.get_env()
        self.dir = util.get_dir()
        self.jobid = uuid.uuid4()


    def submit_tado_efficiency_gluejob(self):
        try:
            jobName = self.dir['glue_tado_efficiency_job_name']
            s3_bucket = self.dir['s3_bucket']
            environment = self.env

            #Batch Logging
            util.batch_logging_insert(self.jobid, 1, 'tado_efficiency_gluejob','start_ensek_internal_estimates_jobs.py')

            obj_tado_efficiency = glue.ProcessGlueJob(job_name=jobName, s3_bucket=s3_bucket, environment=environment, processJob='tado_efficiency')
            tado_efficiency_job_response = obj_tado_efficiency.run_glue_job()

            if tado_efficiency_job_response:

                print("{0}: TADO Efficiency Job Completed successfully".format(datetime.now().strftime('%H:%M:%S')))

                # Batch Logging
                util.batch_logging_update(self.jobid, 'e')

            else:
                print("Error occurred in TADO Efficiency Job")
                # return staging_job_response
                raise Exception
        except Exception as e:
            print("Error in TADO Efficiency Glue Job Job :- " + str(e))

            # Batch Logging
            util.batch_logging_update(self.jobid, 'f',  str(e))

            # write
            sys.exit(1)



if __name__ == '__main__':

    s = TADOEfficiencyJobs()

    # run reference TADO Efficiency glue job
    print("{0}: TADO Efficiency Glue Job running...".format(datetime.now().strftime('%H:%M:%S')))
    s.submit_tado_efficiency_gluejob()

    print("{0}: TADO Efficiency Glue Job running completed successfully".format(datetime.now().strftime('%H:%M:%S')))

