import sys

from datetime import datetime
import timeit
import subprocess

from cdw.common import process_glue_job as glue
from cdw.common import utils as util


class DailySalesJobs:
    def __init__(self):
        self.pythonAlias = util.get_pythonAlias()
        self.env = util.get_env()
        self.dir = util.get_dir()
        self.all_jobid = util.get_jobID()
        self.daily_sales_batch_jobid = util.get_jobID()

    def submit_daily_sales_batch_gluejob(self):
        try:
            jobName = self.dir["glue_daily_sales_job_name"]
            s3_bucket = self.dir["s3_bucket"]
            environment = self.env

            # Batch Logging
            util.batch_logging_insert(
                self.daily_sales_batch_jobid, 60, "daily_sales_gluejob", "start_daily_sales_jobs.py"
            )

            obj_daily_sales = glue.ProcessGlueJob(
                job_name=jobName, s3_bucket=s3_bucket, environment=environment, processJob="daily_sales"
            )
            daily_sales_job_response = obj_daily_sales.run_glue_job()

            if daily_sales_job_response:

                print("{0}: Daily Sales Job Completed successfully".format(datetime.now().strftime("%H:%M:%S")))

                # Batch Logging
                util.batch_logging_update(self.daily_sales_batch_jobid, "e")

            else:

                print("Error occurred in Daily Sales Job")
                # return staging_job_response
                raise Exception
        except Exception as e:
            print("Error in Daily Sales Glue Job Job :- " + str(e))

            # Batch Logging
            util.batch_logging_update(self.daily_sales_batch_jobid, "f", str(e))

            # write
            sys.exit(1)


if __name__ == "__main__":

    s = DailySalesJobs()

    util.batch_logging_insert(s.all_jobid, 1, "all_daily_sales_jobs", "start_daily_sales_jobs.py")

    # run reference TADO Efficiency glue job
    print("{0}: Dail Sales Batch Glue Job running...".format(datetime.now().strftime("%H:%M:%S")))
    s.submit_daily_sales_batch_gluejob()

    util.batch_logging_update(s.all_jobid, "e")
