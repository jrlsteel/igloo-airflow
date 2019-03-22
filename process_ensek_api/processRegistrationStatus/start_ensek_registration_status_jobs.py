import sys
from datetime import datetime
import timeit
import subprocess

sys.path.append('..')
from common import process_glue_job as glue
from common import utils as util


class StartRegistrationsMeterpointsStatusJobs:
    def __init__(self):
        self.pythonAlias = util.get_pythonAlias()
        self.env = util.get_env()
        self.dir = util.get_dir()

    def submit_registrations_meterpoints_status_scripts(self):
        try:
            all_ensek_pa_scripts_response = ae.process_all_ensek_pa_scripts()
            if all_ensek_pa_scripts_response:
                print("{0}: All Ensek Scripts job completed successfully".format(datetime.now().strftime('%H:%M:%S')))
                # return all_ensek_scripts_response
            else:
                print("Error occurred in All Ensek Scripts job")
                # return all_ensek_scripts_response
                raise Exception
        except Exception as e:
            print("Error in Ensek Scripts :- " + str(e))
            sys.exit(1)

    def submit_registrations_meterpoints_status_staging_Gluejob(self):
        try:
            jobName = self.dir['glue_staging_job_name']
            s3_bucket = self.dir['s3_bucket']
            environment = self.env

            obj_stage = glue.ProcessGlueJob(job_name=jobName, s3_bucket=s3_bucket, environment=environment,
                                            processJob='ensek')
            job_response = obj_stage.run_glue_job()
            if job_response:
                print("{0}: Staging Job Completed successfully".format(datetime.now().strftime('%H:%M:%S')))
                # return staging_job_response
            else:
                print("Error occurred in Staging Job")
                # return staging_job_response
                raise Exception
        except Exception as e:
            print("Error in Staging Job :- " + str(e))
            sys.exit(1)

    def submit_registrations_meterpoints_status_Gluejob(self):
        try:
            jobname = self.dir['glue_customerDB_job_name']
            s3_bucket = self.dir['s3_bucket']
            environment = self.env

            obj_customerDB = glue.ProcessGlueJob(job_name=jobname, s3_bucket=s3_bucket, environment=environment,
                                                 processJob='')
            job_response = obj_customerDB.run_glue_job()
            if job_response:
                print("{0}: CustomerDB Glue Job completed successfully".format(datetime.now().strftime('%H:%M:%S')))
                # return staging_job_response
            else:
                print("Error occurred in CustomerDB Glue Job")
                # return staging_job_response
                raise Exception
        except Exception as e:
            print("Error in Customer DB Job :- " + str(e))
            sys.exit(1)


if __name__ == '__main__':

    s = StartRegistrationsMeterpointsStatusJobs()

    # run Registrations Meterpoints Status
    print("{0}:  CustomerDB Jobs running...".format(datetime.now().strftime('%H:%M:%S')))
    s.submit_registrations_meterpoints_status_scripts()

    # run PA Ensek Jobs
    print("{0}:  PA Ensek Jobs running...".format(datetime.now().strftime('%H:%M:%S')))
    s.submit_registrations_meterpoints_status_staging_Gluejob()

    # run staging glue job
    print("{0}: Staging Job running...".format(datetime.now().strftime('%H:%M:%S')))
    s.submit_registrations_meterpoints_status_Gluejob()



    print("{0}: All D18 completed successfully".format(datetime.now().strftime('%H:%M:%S')))

