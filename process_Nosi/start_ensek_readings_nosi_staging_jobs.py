import sys
from datetime import datetime
import timeit
import subprocess

sys.path.append("..")
from common import process_glue_job as glue
from common import utils as util
from common import Refresh_UAT as refresh


class StartReadingsNOSIJobs:
    def __init__(self):
        self.pythonAlias = util.get_pythonAlias()
        self.env = util.get_env()
        self.dir = util.get_dir()
        self.all_jobid = util.get_jobID()
        self.nosi_jobid = util.get_jobID()
        self.nosi_download_jobid = util.get_jobID()
        self.nosi_staging_jobid = util.get_jobID()
        self.nosi_ref_jobid = util.get_jobID()
        self.process_name = "Nosi"
        self.mirror_jobid = util.get_jobID()

    def submit_internal_readings_nosi_staging_gluejob(self):
        try:
            jobName = self.dir["glue_staging_job_name"]
            util.batch_logging_insert(
                self.nosi_staging_jobid, 44, "nosi_staging_glue_job", "start_ensek_readings_nosi_jobs.py"
            )
            s3_bucket = self.dir["s3_bucket"]
            environment = self.env

            obj_stage = glue.ProcessGlueJob(
                job_name=jobName, s3_bucket=s3_bucket, environment=environment, processJob="nosi"
            )
            job_response = obj_stage.run_glue_job()
            if job_response:
                util.batch_logging_update(self.nosi_staging_jobid, "e")
                print("{0}: Staging Job Completed successfully".format(datetime.now().strftime("%H:%M:%S")))
                # return staging_job_response
            else:
                print("Error occurred in Staging Job")
                # return staging_job_response
                raise Exception
        except Exception as e:
            util.batch_logging_update(self.nosi_staging_jobid, "f", str(e))
            util.batch_logging_update(self.all_jobid, "f", str(e))
            print("Error in Staging Job :- " + str(e))
            sys.exit(1)


if __name__ == "__main__":

    s = StartReadingsNOSIJobs()

    util.batch_logging_insert(s.all_jobid, 132, "all_readings_internal_nosi_jobs", "start_ensek_readings_nosi_jobs.py")

    # # Ensek Internal Readings NOSI Staging
    print("{0}:  Ensek Internal Readings Staging NOSI Jobs running...".format(datetime.now().strftime("%H:%M:%S")))
    s.submit_internal_readings_nosi_staging_gluejob()

    util.batch_logging_update(s.all_jobid, "e")
