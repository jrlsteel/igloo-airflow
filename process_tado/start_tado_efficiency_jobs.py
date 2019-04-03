import sys
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


    def submit_tado_efficiency_gluejob(self):
        try:
            jobName = self.dir['glue_tado_efficiency_job_name']
            s3_bucket = self.dir['s3_bucket']
            environment = self.env

            obj_tado_efficiency = glue.ProcessGlueJob(job_name=jobName, s3_bucket=s3_bucket, environment=environment, processJob='tado_efficiency')
            tado_efficiency_job_response = obj_tado_efficiency.run_glue_job()
            if tado_efficiency_job_response:
                print("{0}: TADO Efficiency Job Completed successfully".format(datetime.now().strftime('%H:%M:%S')))
                # return staging_job_response
            else:
                print("Error occurred in TADO Efficiency Job")
                # return staging_job_response
                raise Exception
        except Exception as e:
            print("Error in D18 Job :- " + str(e))
            sys.exit(1)


if __name__ == '__main__':

    s = TADOEfficiencyJobs()

    # run reference d18 glue job
    print("{0}: D18 Glue Job running...".format(datetime.now().strftime('%H:%M:%S')))
    s.submit_d18_gluejob()

    print("{0}: All D18 completed successfully".format(datetime.now().strftime('%H:%M:%S')))

