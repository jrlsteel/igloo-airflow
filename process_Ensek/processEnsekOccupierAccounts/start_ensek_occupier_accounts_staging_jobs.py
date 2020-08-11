import sys
from datetime import datetime
import timeit
import subprocess

sys.path.append('../..')
from common import process_glue_job as glue
from common import utils as util


class StartOccupierAccountsJobs:
    def __init__(self):
        self.process_name = "Occupier Accounts"
        self.pythonAlias = util.get_pythonAlias()
        self.env = util.get_env()
        self.dir = util.get_dir()
        self.occupier_accounts_extract_jobid = util.get_jobID()
        self.occupier_accounts_staging_jobid = util.get_jobID()
        self.occupier_accounts_reference_jobid = util.get_jobID()
        self.occupier_accounts_job_id = util.get_jobID()
        self.jobName = self.dir['glue_estimated_advance_job_name']

    def submit_stage2_job(self):
        try:
            util.batch_logging_insert(self.occupier_accounts_staging_jobid, 51, 'Occupier Accounts Staging Processing',
                                      'start_ensek_occupier_accounts_jobs.py')
            jobName = self.dir['glue_staging_job_name']
            s3_bucket = self.dir['s3_bucket']
            environment = self.env
            obj_stage = glue.ProcessGlueJob(job_name=jobName, s3_bucket=s3_bucket, environment=environment,
                                            processJob='occu_acc')
            staging_job_response = obj_stage.run_glue_job()
            if staging_job_response:
                util.batch_logging_update(self.occupier_accounts_staging_jobid, 'e')
                print("{0}: Staging Job Completed successfully for {1}".format(datetime.now().strftime('%H:%M:%S'),
                                                                               self.process_name))
                # return staging_job_response
            else:
                print("Error occurred in {0} Staging Job".format(self.process_name))
                # return staging_job_response
                raise Exception
        except Exception as e:
            util.batch_logging_update(self.occupier_accounts_staging_jobid, 'f', str(e))
            util.batch_logging_update(self.occupier_accounts_job_id, 'f', str(e))
            print("Error in Staging Job :- " + str(e))
            sys.exit(1)


if __name__ == '__main__':


    s = StartOccupierAccountsJobs()

    util.batch_logging_insert(s.occupier_accounts_job_id, 51, 'all_occupier_accounts_jobs',
                              'start_ensek_occupier_accounts_jobs.py')

    # run staging glue job
    print("{0}: Staging Job running for {1}...".format(datetime.now().strftime('%H:%M:%S'), s.process_name))
    s.submit_stage2_job()

    util.batch_logging_update(s.occupier_accounts_job_id, 'e')
