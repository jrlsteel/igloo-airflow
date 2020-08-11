import sys
from datetime import datetime
import timeit
import subprocess

sys.path.append('../..')
from common import process_glue_job as glue
from common import utils as util


class StartOccupierAccountsJobs:
    def __init__(self):
        self.pythonAlias = util.get_pythonAlias()
        self.env = util.get_env()
        self.dir = util.get_dir()
        self.occupier_accounts_extract_jobid = util.get_jobID()
        self.occupier_accounts_reference_jobid = util.get_jobID()
        self.occupier_accounts_job_id = util.get_jobID()
        self.jobName = self.dir['glue_estimated_advance_job_name']


    def submit_occupier_accounts_extract_reference_gluejob(self):
        try:
            util.batch_logging_insert(self.occupier_accounts_reference_jobid, 51, 'Occupier Accounts Ref Processing',
                                      'start_ensek_occupier_accounts_jobs.py')
            jobname = self.dir['glue_occupier_accounts_job_name']
            s3_bucket = self.dir['s3_bucket']
            environment = self.env

            obj_submit_occupier_accounts_status_Gluejob = glue.ProcessGlueJob(job_name=jobname, s3_bucket=s3_bucket, environment=environment,
                                                 processJob='occu_acc')
            job_response = obj_submit_occupier_accounts_status_Gluejob.run_glue_job()
            if job_response:
                util.batch_logging_update(self.occupier_accounts_reference_jobid, 'e')
                print("{0}: Ensek Occupier Accounts Reference Glue Job completed successfully".format(datetime.now().strftime('%H:%M:%S')))
                # returnsubmit_registrations_meterpoints_status_Gluejob
            else:
                print("Error occurred in Occupier Accounts Reference Glue Job")
                # return submit_registrations_meterpoints_status_Gluejob
                raise Exception
        except Exception as e:
            util.batch_logging_update(self.occupier_accounts_reference_jobid, 'f', str(e))
            util.batch_logging_update(self.occupier_accounts_job_id, 'f', str(e))
            print("Error in Ensek Occupier Accounts Reference DB Job :- " + str(e))
            sys.exit(1)


if __name__ == '__main__':


    s = StartOccupierAccountsJobs()

    util.batch_logging_insert(s.occupier_accounts_job_id, 51, 'all_occupier_accounts_jobs',
                              'start_ensek_occupier_accounts_jobs.py')

    # #Ensek Occupier Accounts  Ref Tables Jobs
    print("{0}: Occupier Accounts Ref Jobs Running...".format(datetime.now().strftime('%H:%M:%S')))
    s.submit_occupier_accounts_extract_reference_gluejob()

    print("{0}: AllEnsek Occupier Accounts completed successfully".format(datetime.now().strftime('%H:%M:%S')))

    util.batch_logging_update(s.occupier_accounts_job_id, 'e')
