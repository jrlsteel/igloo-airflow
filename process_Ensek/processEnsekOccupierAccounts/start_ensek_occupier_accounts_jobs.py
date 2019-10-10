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
        self.occupier_accounts_job_id = util.get_jobID()
        self.jobName = self.dir['glue_estimated_advance_job_name']

    def submit_occupier_accounts_extract_job(self):
        """
        Calls the Occupier Accounts process_ensek_occupier_accounts.py script to which processes Occupuier Acocunts
        :return: None
        """

        print("{0}: >>>> Process Ensek Occupier Accounts   <<<<".format(datetime.now().strftime('%H:%M:%S')))
        try:
            start = timeit.default_timer()
            subprocess.run([self.pythonAlias, "process_ensek_occupier_accounts.py"])
            print("{0}: Process Ensek Occupier  completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'),
                                                                               float(timeit.default_timer() - start)))
        except Exception as e:
            print("Error in Process Occupier Accounts  :- " + str(e))
            sys.exit(1)

    # def submit_meterpoints_extract_staging_gluejob(self):
    #     try:
    #         jobName = self.dir['glue_staging_job_name']
    #         s3_bucket = self.dir['s3_bucket']
    #         environment = self.env
    #
    #         obj_stage = glue.ProcessGlueJob(job_name=jobName, s3_bucket=s3_bucket, environment=environment,
    #                                         processJob='meterpoints')
    #         job_response = obj_stage.run_glue_job()
    #         if job_response:
    #             print("{0}: Staging Ensek Meterpoints Job Completed successfully".format(datetime.now().strftime('%H:%M:%S')))
    #             # return staging_job_response
    #         else:
    #             print("Error occurred in Ensek Meterpoints Staging Job")
    #             # return staging_job_response
    #             raise Exception
    #     except Exception as e:
    #         print("Error in Ensek Meterpoints Staging Job :- " + str(e))
    #         sys.exit(1)
    #
    # def submit_meterpoints_extract_reference_gluejob(self):
    #     try:
    #         jobname = self.dir['glue_registrations_meterpoints_status_job_name']
    #         s3_bucket = self.dir['s3_bucket']
    #         environment = self.env
    #
    #         obj_submit_registrations_meterpoints_status_Gluejob = glue.ProcessGlueJob(job_name=jobname, s3_bucket=s3_bucket, environment=environment,
    #                                              processJob='ref_meterpoints')
    #         job_response = obj_submit_registrations_meterpoints_status_Gluejob.run_glue_job()
    #         if job_response:
    #             print("{0}: Ensek Meterpoints Reference Glue Job completed successfully".format(datetime.now().strftime('%H:%M:%S')))
    #             # returnsubmit_registrations_meterpoints_status_Gluejob
    #         else:
    #             print("Error occurred in Ensek Meterpoints Reference Glue Job")
    #             # return submit_registrations_meterpoints_status_Gluejob
    #             raise Exception
    #     except Exception as e:
    #         print("Error in Ensek Meterpoints Reference DB Job :- " + str(e))
    #         sys.exit(1)


if __name__ == '__main__':


    s = StartOccupierAccountsJobs()

    util.batch_logging_insert(s.occupier_accounts_job_id, 51, 'all_occupier_accounts_jobs',
                              'start_ensek_occupier_accounts_jobs.py')


    #Ensek Meterpoints Extract
    print("{0}:  Ensek Meterpoints Status Jobs running...".format(datetime.now().strftime('%H:%M:%S')))
    s.submit_occupier_accounts_extract_job()

    # #Ensek Meterpoints Staging Jobs
    # print("{0}:  Ensek  Meterpoints Status Staging Jobs running...".format(datetime.now().strftime('%H:%M:%S')))
    # s.submit_meterpoints_extract_staging_gluejob()
    #
    # #Ensek Meterpoints ref Tables Jobs
    # print("{0}: Registrations Meterpoints Status Ref Jobs Running...".format(datetime.now().strftime('%H:%M:%S')))
    # s.submit_meterpoints_extract_reference_gluejob()

    print("{0}: AllEnsek Occupier Accounts completed successfully".format(datetime.now().strftime('%H:%M:%S')))

    util.batch_logging_update(s.occupier_accounts_job_id, 'e')
