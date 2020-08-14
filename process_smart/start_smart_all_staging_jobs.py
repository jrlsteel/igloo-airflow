import sys
from datetime import datetime
import timeit
import subprocess

sys.path.append('..')

from common import process_glue_job as glue
from common import utils as util
from common import Refresh_UAT as refresh
from process_calculated_steps import start_calculated_steps_jobs as sj


class SmartStagingAll:
    def __init__(self):
        self.process_name = "Smart All Staging "
        self.pythonAlias = util.get_pythonAlias()
        self.env = util.get_env()
        self.dir = util.get_dir()

        self.all_jobid = util.get_jobID()
        self.smart_all_staging_jobid = util.get_jobID()


    def submit_smart_all_staging_gluejob(self):
        try:
            util.batch_logging_insert(self.smart_all_staging_jobid, 601, 'smart_all_staging_glue_job','start_smart_all_staging_jobs.py')
            jobName = self.dir['glue_staging_job_name']
            s3_bucket = self.dir['s3_bucket']
            environment = self.env

            obj_stage = glue.ProcessGlueJob(job_name=jobName, s3_bucket=s3_bucket, environment=environment, processJob='smart-all')
            staging_job_response = obj_stage.run_glue_job()
            if staging_job_response:
                util.batch_logging_update(self.smart_all_staging_jobid, 'e')
                print("{0}: Staging Job Completed successfully for {1}".format(datetime.now().strftime('%H:%M:%S'),self.process_name))
            else:
                print("Error occurred in {0} Staging Job".format(self.process_name))
                # return staging_job_response
                raise Exception

        except Exception as e:
            util.batch_logging_update(self.smart_all_staging_jobid, 'f', str(e))
            util.batch_logging_update(self.all_jobid, 'f', str(e))
            print("Error in Staging Job :- " + str(e))
            sys.exit(1)




if __name__ == '__main__':

    s = SmartStagingAll()

    util.batch_logging_insert(s.all_jobid, 600, 'all_smart_all_jobs', 'start_smart_all_staging_jobs.py')

    #
    # # run alp wcf staging glue job
    print("{0}: Staging Job running for {1}...".format(datetime.now().strftime('%H:%M:%S'), s.process_name))
    s.submit_smart_all_staging_gluejob()
    #

    util.batch_logging_update(s.all_jobid, 'e')

