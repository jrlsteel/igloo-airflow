import sys
from datetime import datetime
import timeit
import subprocess

sys.path.append('..')
from common import process_glue_job as glue
from common import utils as util


class StartAccountSettingsJobs:
    def __init__(self):
        self.pythonAlias = util.get_pythonAlias()
        self.env = util.get_env()
        self.dir = util.get_dir()

    def submit_account_settings_job(self):
        """
        Calls the Ensek Account Settings process_ensek_internal_estimates.py
        :return: None
        """

        print("{0}: >>>> Process Ensek Account Settings  <<<<".format(datetime.now().strftime('%H:%M:%S')))
        try:
            start = timeit.default_timer()
            subprocess.run([self.pythonAlias, "process_ensek_account_settings.py"])
            print("{0}: Process Ensek Account Settings completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'),
                                                                               float(timeit.default_timer() - start)))
        except Exception as e:
            print("Error in Process Ensek Account Settings process :- " + str(e))
            sys.exit(1)

    def submit_account_settings_staging_gluejob(self):
        try:
            jobName = self.dir['glue_staging_job_name']
            s3_bucket = self.dir['s3_bucket']
            environment = self.env

            obj_stage = glue.ProcessGlueJob(job_name=jobName, s3_bucket=s3_bucket, environment=environment,
                                            processJob='ensek-account-settings')
            job_response = obj_stage.run_glue_job()
            if job_response:
                print("{0}: Staging Ensek Account Setttings Job Completed successfully".format(datetime.now().strftime('%H:%M:%S')))
                # return staging_job_response
            else:
                print("Error occurred in Ensek Account Settings Staging Job")
                # return staging_job_response
                raise Exception
        except Exception as e:
            print("Error in Ensek Account Settings Staging Job :- " + str(e))
            sys.exit(1)

    def submit_account_sedttings_gluejob(self):
        try:
            jobname = self.dir['glue_account_settings_job_name']
            s3_bucket = self.dir['s3_bucket']
            environment = self.env

            obj_submit_internal_estimates_Gluejob = glue.ProcessGlueJob(job_name=jobname, s3_bucket=s3_bucket, environment=environment,
                                                 processJob='ensek_ref_account_settings')
            job_response = obj_submit_internal_estimates_Gluejob.run_glue_job()
            if job_response:
                print("{0}: Ensek Account Settings Glue Job completed successfully".format(datetime.now().strftime('%H:%M:%S')))

            else:
                print("Error occurred in Ensek Account Settings Glue Job")

                raise Exception
        except Exception as e:
            print("Error in Ensek Account Settings DB Job :- " + str(e))
            sys.exit(1)


if __name__ == '__main__':

    s = StartInternalEstimatesJobs()

    #Ensek Internal Estimates Ensek Extract
    print("{0}: Ensek Account Settings Jobs running...".format(datetime.now().strftime('%H:%M:%S')))
    s.submit_account_settings_job()

    #Ensek Internal Estimates Staging Jobs
    print("{0}:  Ensek Account Settings Staging Jobs running...".format(datetime.now().strftime('%H:%M:%S')))
    s.submit_account_settings_staging_gluejob()

    #Ensek Internal Estimates ref Tables Jobs
    print("{0}: Ensek Account Settings Ref Jobs Running...".format(datetime.now().strftime('%H:%M:%S')))
    s.submit_account_settings_gluejob()



    print("{0}: All Internal Estimates completed successfully".format(datetime.now().strftime('%H:%M:%S')))

