import sys
from datetime import datetime
import timeit
import subprocess

sys.path.append("..")
from common import process_glue_job as glue
from common import utils as util
from common import Refresh_UAT as refresh


class StartD18Jobs:
    def __init__(self):
        self.pythonAlias = util.get_pythonAlias()
        self.env = util.get_env()
        self.dir = util.get_dir()
        self.all_jobid = util.get_jobID()
        self.d18_jobid = util.get_jobID()
        self.d18_download_jobid = util.get_jobID()
        self.d18_staging_jobid = util.get_jobID()
        self.d18_ref_jobid = util.get_jobID()
        self.process_name = "D18 Download Extract and Process "

    def submit_d18_staging_gluejob(self):
        try:
            util.batch_logging_insert(self.d18_staging_jobid, 29, "d18_staging_glue_job", "start_d18_jobs.py")
            jobName = self.dir["glue_staging_job_name"]
            s3_bucket = self.dir["s3_bucket"]
            environment = self.env
            obj_stage = glue.ProcessGlueJob(
                job_name=jobName, s3_bucket=s3_bucket, environment=environment, processJob="d18"
            )
            staging_job_response = obj_stage.run_glue_job()
            if staging_job_response:
                util.batch_logging_update(self.d18_staging_jobid, "e")
                print("{0}: Staging Job Completed successfully".format(datetime.now().strftime("%H:%M:%S")))
                # return staging_job_response
            else:
                print("Error occurred in Staging Job")
                # return staging_job_response
                raise Exception
        except Exception as e:
            util.batch_logging_update(self.d18_staging_jobid, "f", str(e))
            util.batch_logging_update(self.all_jobid, "f", str(e))
            print("Error in Staging Job :- " + str(e))
            sys.exit(1)


if __name__ == "__main__":

    s = StartD18Jobs()

    util.batch_logging_insert(s.all_jobid, 102, "all_d18_jobs", "start_d18_jobs.py")

    # # run staging glue job
    print("{0}: Staging Job running...".format(datetime.now().strftime("%H:%M:%S")))
    s.submit_d18_staging_gluejob()

    print("{0}: All D18 completed successfully".format(datetime.now().strftime("%H:%M:%S")))
    util.batch_logging_update(s.all_jobid, "e")
