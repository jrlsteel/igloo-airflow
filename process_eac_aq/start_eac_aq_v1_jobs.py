import sys
from datetime import datetime
import timeit
import subprocess

sys.path.append('..')
from common import process_glue_job as glue
from common import utils as util


class EacAqPa:
    def __init__(self):
        self.process_name = "EAC AQ PA Process"
        self.pythonAlias = util.get_pythonAlias()
        self.env = util.get_env()
        self.dir = util.get_dir()

        self.all_jobid = util.get_jobID()
        self.eac_aq_v1_ref_jobid = util.get_jobID()




    def submit_eac_aq_gluejob(self):
        try:
            util.batch_logging_insert(self.eac_aq_v1_ref_jobid, 38, 'eac_aq_v1_calculated_glue_job', 'start_eac_aq_v1_jobs.py')

            jobName = self.dir['glue_eac_aq_job_name']
            s3_bucket = self.dir['s3_bucket']
            environment = self.env

            obj_eac_aq = glue.ProcessGlueJob(job_name=jobName, s3_bucket=s3_bucket, environment=environment, processJob='eac_aq_v1')
            eac_aq_job_response = obj_eac_aq.run_glue_job()
            if eac_aq_job_response:
                util.batch_logging_update(self.eac_aq_v1_ref_jobid, 'e')
                print("{0}: EAC and AQ Job Completed successfully".format(datetime.now().strftime('%H:%M:%S')))
                # return staging_job_response
            else:
                print("Error occurred in EAC and AQ Job")
                # return staging_job_response
                raise Exception
        except Exception as e:
            util.batch_logging_update(self.eac_aq_v1_ref_jobid, 'f', str(e))
            util.batch_logging_update(self.all_jobid, 'f', str(e))
            print("Error in EAC and AQ Job :- " + str(e))
            sys.exit(1)

if __name__ == '__main__':

    s = EacAqPa()

    util.batch_logging_insert(s.all_jobid, 130, 'all_eac_aq_pa', 'start_eac_aq_pa_jobs.py')

    # run eac and aq calculation job
    print("{0}: EAC and AQ Glue Job running...".format(datetime.now().strftime('%H:%M:%S')))
    s.submit_eac_aq_gluejob()

    print("{0}: All {1} completed successfully".format(datetime.now().strftime('%H:%M:%S'), s.process_name))

    util.batch_logging_update(s.all_jobid, 'e')

