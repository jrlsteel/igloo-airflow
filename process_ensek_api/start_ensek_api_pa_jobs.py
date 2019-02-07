import sys
from datetime import datetime

sys.path.append('..')

from process_ensek_api import processAllEnsekPAScripts as ae
from process_D18 import start_d18_jobs as d18
from process_ALP import start_alp_historical_jobs as alp
from common import process_glue_job as glue

from common import utils as util


class StartEnsekPAJobs:

    def __init__(self):
        self.env = util.get_env()
        self.dir = util.get_dir()

    def submit_all_ensek_pa_scripts(self):
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

    def submit_all_d18_scripts(self):
        try:
            d18_job = d18.StartD18Jobs()
            d18_job.main(d18_job)
        except Exception as e:
            print("Error in D181 Job :- " + str(e))
            sys.exit(1)

    def submit_all_alp_scripts(self):
        try:
            alp_job = alp.ALP()
            alp_job.main(alp_job)
        except Exception as e:
            print("Error in ALP Job :- " + str(e))
            sys.exit(1)

    def submit_ensek_staging_Gluejob(self):
        try:
            jobName = self.dir['glue_staging_job_name']
            s3_bucket = self.dir['s3_bucket']
            environment = self.env

            obj_stage = glue.ProcessGlueJob(job_name=jobName, s3_bucket=s3_bucket, environment=environment, processJob='ensek')
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

    def submit_customerDB_Gluejob(self):
        try:
            jobname = self.dir['glue_customerDB_job_name']
            s3_bucket = self.dir['s3_bucket']
            environment = self.env

            obj_customerDB = glue.ProcessGlueJob(job_name=jobname, s3_bucket=s3_bucket, environment=environment, processJob='')
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

    def submit_Ensek_Gluejob(self):
        try:
            jobname = self.dir['glue_ensek_job_name']
            s3_bucket = self.dir['s3_bucket']
            environment = self.env

            obj_ensek = glue.ProcessGlueJob(job_name=jobname, s3_bucket=s3_bucket, environment=environment,
                                            processJob='ensek')
            job_response = obj_ensek.run_glue_job()
            if job_response:
                print("{0}: Ensek Glue Job completed successfully".format(datetime.now().strftime('%H:%M:%S')))
                # return staging_job_response
            else:
                print("Error occurred in Ensek Glue Job")
                # return staging_job_response
                raise Exception
        except Exception as e:
            print("Error in Ensek Job :- " + str(e))
            sys.exit(1)

    def submit_eac_aq_gluejob(self):
        try:
            jobName = self.dir['glue_eac_aq_job_name']
            s3_bucket = self.dir['s3_bucket']
            environment = self.env

            obj_d18 = glue.ProcessGlueJob(job_name=jobName, s3_bucket=s3_bucket, environment=environment,
                                          processJob='eac_aq')
            d18_job_response = obj_d18.run_glue_job()
            if d18_job_response:
                print("{0}: EAC and AQ Job Completed successfully".format(datetime.now().strftime('%H:%M:%S')))
                # return staging_job_response
            else:
                print("Error occurred in EAC and AQ Job")
                # return staging_job_response
                raise Exception
        except Exception as e:
            print("Error in EAC and AQ Job :- " + str(e))
            sys.exit(1)



if __name__ == '__main__':
    s = StartEnsekPAJobs()


    # run Customer DB
    print("{0}:  CustomerDB Jobs running...".format(datetime.now().strftime('%H:%M:%S')))
    s.submit_customerDB_Gluejob()

    # run staging glue job
    print("{0}: Staging Job running...".format(datetime.now().strftime('%H:%M:%S')))
    s.submit_ensek_staging_Gluejob()

    print("{0}: Ensek Reference Jobs running...".format(datetime.now().strftime('%H:%M:%S')))
    s.submit_Ensek_Gluejob()

    # run eac and aq calculation job
    print("{0}: EAC and AQ Glue Job running...".format(datetime.now().strftime('%H:%M:%S')))
    s.submit_eac_aq_gluejob()

    print("{0}: All jobs completed successfully".format(datetime.now().strftime('%H:%M:%S')))
