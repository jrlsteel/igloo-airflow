import sys

from datetime import datetime
import timeit
import subprocess

from cdw.common import process_glue_job as glue
from cdw.common import utils as util


class EstimatedAdvance:
    def __init__(self):
        self.pythonAlias = util.get_pythonAlias()
        self.env = util.get_env()
        self.dir = util.get_dir()
        self.est_advance_job_id = util.get_jobID()
        self.jobName = self.dir["glue_estimated_advance_job_name"]
        self.s3_bucket = self.dir["s3_bucket"]
        self.environment = self.env

    def submit_estimated_advance_gluejob(self):
        try:

            # Batch Logging
            util.batch_logging_insert(
                self.est_advance_job_id, 50, "estimated_advance_glue_job", "start_est_advance_job.py"
            )

            obj_est_adv = glue.ProcessGlueJob(
                job_name=self.jobName, s3_bucket=self.s3_bucket, environment=self.environment, processJob="est_adv"
            )
            response = obj_est_adv.run_glue_job()

            if response:

                print(
                    "{0}: Estimated {1} Completed successfully".format(
                        datetime.now().strftime("%H:%M:%S"), self.jobName
                    )
                )

                # Batch Logging
                util.batch_logging_update(self.est_advance_job_id, "e")

            else:

                print("Error occurred in {0} Job").format(self.jobName)
                # return staging_job_response
                raise Exception

        except Exception as e:
            print("Error in {0} Glue Job Job :- " + str(e)).format(self.jobName)

            # Batch Logging
            util.batch_logging_update(self.est_advance_job_id, "f", str(e))

            # write
            sys.exit(1)


if __name__ == "__main__":

    s = EstimatedAdvance()

    util.batch_logging_insert(s.est_advance_job_id, 50, "all_estimated_advance_jobs", "start_est_advance_job.py")

    # run glue job
    print("{0}: Estimated Advance Glue Job running...".format(datetime.now().strftime("%H:%M:%S")))
    s.submit_estimated_advance_gluejob()

    util.batch_logging_update(s.est_advance_job_id, "e")
