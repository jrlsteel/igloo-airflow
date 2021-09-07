import sys
from datetime import datetime
import timeit
import subprocess


from cdw.common import process_glue_job as glue
from cdw.common import utils as util
from cdw.common import Refresh_UAT as refresh
from process_calculated_steps import start_calculated_steps_jobs as sj


class ALP:
    def __init__(self):
        self.process_name = "ALP WCF CV Historical EAC_AQ CONS_ACCU "
        self.pythonAlias = util.get_pythonAlias()
        self.env = util.get_env()
        self.dir = util.get_dir()

        self.all_jobid = util.get_jobID()

        self.alp_wcf_jobid = util.get_jobID()
        self.alp_cv_jobid = util.get_jobID()
        self.alp_wcf_staging_jobid = util.get_jobID()
        self.alp_cv_staging_jobid = util.get_jobID()
        self.alp_ref_jobid = util.get_jobID()

    def submit_alp_wcf_staging_gluejob(self):
        try:
            util.batch_logging_insert(
                self.alp_wcf_staging_jobid, 33, "alp_wcf_staging_glue_job", "start_alp_historical_jobs.py"
            )
            jobName = self.dir["glue_staging_job_name"]
            s3_bucket = self.dir["s3_bucket"]
            environment = self.env

            obj_stage = glue.ProcessGlueJob(
                job_name=jobName, s3_bucket=s3_bucket, environment=environment, processJob="alp-wcf"
            )
            staging_job_response = obj_stage.run_glue_job()
            if staging_job_response:
                util.batch_logging_update(self.alp_wcf_staging_jobid, "e")
                print(
                    "{0}: Staging Job Completed successfully for {1}".format(
                        datetime.now().strftime("%H:%M:%S"), self.process_name
                    )
                )
                # return staging_job_response
            else:
                print("Error occurred in {0} Staging Job".format(self.process_name))
                # return staging_job_response
                raise Exception

        except Exception as e:
            util.batch_logging_update(self.alp_wcf_staging_jobid, "f", str(e))
            util.batch_logging_update(self.all_jobid, "f", str(e))
            print("Error in Staging Job :- " + str(e))
            sys.exit(1)

    def submit_alp_cv_staging_gluejob(self):
        try:
            util.batch_logging_insert(
                self.alp_cv_staging_jobid, 34, "alp_cv_staging_glue_job", "start_alp_historical_jobs.py"
            )
            jobName = self.dir["glue_staging_job_name"]
            s3_bucket = self.dir["s3_bucket"]
            environment = self.env

            obj_stage = glue.ProcessGlueJob(
                job_name=jobName, s3_bucket=s3_bucket, environment=environment, processJob="alp-cv"
            )
            staging_job_response = obj_stage.run_glue_job()
            if staging_job_response:
                util.batch_logging_update(self.alp_cv_staging_jobid, "e")
                print(
                    "{0}: Staging Job Completed successfully for {1}".format(
                        datetime.now().strftime("%H:%M:%S"), self.process_name
                    )
                )
            else:
                print("Error occurred in {0} Staging Job".format(self.process_name))
                # return staging_job_response
                raise Exception

        except Exception as e:
            util.batch_logging_update(self.alp_cv_staging_jobid, "f", str(e))
            util.batch_logging_update(self.all_jobid, "f", str(e))
            print("Error in Staging Job :- " + str(e))
            sys.exit(1)


if __name__ == "__main__":

    s = ALP()

    util.batch_logging_insert(s.all_jobid, 105, "all_alp_jobs", "start_alp_jobs.py")

    #
    # # run alp wcf staging glue job
    print("{0}: Staging Job running for {1}...".format(datetime.now().strftime("%H:%M:%S"), s.process_name))
    s.submit_alp_wcf_staging_gluejob()
    #
    # # run alp cv staging glue job
    print("{0}: Staging Job running for {1}...".format(datetime.now().strftime("%H:%M:%S"), s.process_name))
    s.submit_alp_cv_staging_gluejob()
    #

    util.batch_logging_update(s.all_jobid, "e")
