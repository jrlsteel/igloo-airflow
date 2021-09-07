import sys
from datetime import datetime
import timeit


from process_Ensek import processAllEnsekPAScripts as ae
from cdw.common import process_glue_job as glue

from cdw.common import utils as util
from cdw.common import Refresh_UAT as refresh


class StartEnsekPAJobs:
    def __init__(self):
        self.env = util.get_env()
        self.dir = util.get_dir()
        self.all_jobid = util.get_jobID()
        self.staging_jobid = util.get_jobID()
        self.mirror_jobid = util.get_jobID()
        self.customerdb_jobid = util.get_jobID()
        self.ref_jobid = util.get_jobID()
        self.ref_eac_aq_jobid = util.get_jobID()
        self.process_name = "Ensek PA Extract and Process "

    def submit_ensek_staging_Gluejob(self):
        try:
            jobName = self.dir["glue_staging_job_name"]
            s3_bucket = self.dir["s3_bucket"]
            environment = self.env

            util.batch_logging_insert(self.staging_jobid, 9, "ensek_pa_staging_gluejob", "start_ensek_api_pa_jobs.py")
            obj_stage = glue.ProcessGlueJob(
                job_name=jobName, s3_bucket=s3_bucket, environment=environment, processJob="ensek-non-pa"
            )
            job_response = obj_stage.run_glue_job()
            if job_response:
                print("{0}: Staging Job Completed successfully".format(datetime.now().strftime("%H:%M:%S")))
                util.batch_logging_update(self.staging_jobid, "e")
                # return staging_job_response
            else:
                print("Error occurred in Staging Job")
                # return staging_job_response
                raise Exception
        except Exception as e:
            print("Error in Staging Job :- " + str(e))
            util.batch_logging_update(self.staging_jobid, "f", str(e))
            util.batch_logging_update(self.all_jobid, "f", str(e))
            sys.exit(1)

    def submit_customerDB_Gluejob(self):
        try:
            jobname = self.dir["glue_customerDB_job_name"]
            s3_bucket = self.dir["s3_bucket"]
            environment = self.env
            util.batch_logging_insert(
                self.customerdb_jobid, 10, "igloo_customerdb_gluejob", "start_ensek_api_pa_jobs.py"
            )
            obj_customerDB = glue.ProcessGlueJob(
                job_name=jobname, s3_bucket=s3_bucket, environment=environment, processJob="mirror_all_cdb"
            )
            job_response = obj_customerDB.run_glue_job()
            if job_response:
                print("{0}: CustomerDB Glue Job completed successfully".format(datetime.now().strftime("%H:%M:%S")))
                util.batch_logging_update(self.customerdb_jobid, "e")
                # return staging_job_response
            else:
                print("Error occurred in CustomerDB Glue Job")
                # return staging_job_response
                raise Exception
        except Exception as e:
            print("Error in Customer DB Job :- " + str(e))
            util.batch_logging_update(self.customerdb_jobid, "f", str(e))
            util.batch_logging_update(self.all_jobid, "f", str(e))
            sys.exit(1)


if __name__ == "__main__":
    s = StartEnsekPAJobs()

    util.batch_logging_insert(s.all_jobid, 103, "all_pa_jobs", "start_ensek_api_pa_jobs.py")

    # run staging glue job
    print("{0}: Staging Job running...".format(datetime.now().strftime("%H:%M:%S")))
    s.submit_ensek_staging_Gluejob()

    print("{0}: All jobs completed successfully".format(datetime.now().strftime("%H:%M:%S")))

    util.batch_logging_update(s.all_jobid, "e")
