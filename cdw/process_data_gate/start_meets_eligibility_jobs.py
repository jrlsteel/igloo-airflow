import sys
from datetime import datetime

from cdw.common import process_glue_job as glue
from cdw.common import utils as util


class MeetsEligibilityJobs:
    def __init__(self):
        self.pythonAlias = util.get_pythonAlias()
        self.env = util.get_env()
        self.dir = util.get_dir()
        self.all_jobid = util.get_jobID()
        self.meets_eligibility_jobid = util.get_jobID()

    def submit_meets_eligibility_gluejob(self):
        try:
            jobName = self.dir["glue_meets_eligibility_job_name"]
            s3_bucket = self.dir["s3_bucket"]
            environment = self.env

            # Batch Logging
            util.batch_logging_insert(
                self.meets_eligibility_jobid, 53, "meets_eligibility_gluejob", "start_meets_eligibility_jobs.py"
            )

            obj_meets_eligibility = glue.ProcessGlueJob(
                job_name=jobName, s3_bucket=s3_bucket, environment=environment, processJob="meets_eli"
            )
            meets_eligibility_job_response = obj_meets_eligibility.run_glue_job()

            if meets_eligibility_job_response:

                print("{0}: Meets Eligibility job completed successfully".format(datetime.now().strftime("%H:%M:%S")))

                # Batch Logging
                util.batch_logging_update(self.meets_eligibility_jobid, "e")

            else:

                print("Error occurred in Meets Eligibility job")
                # return staging_job_response
                raise Exception
        except Exception as e:
            print("Error in Meets Eligibility glue job Job :- " + str(e))

            # Batch Logging
            util.batch_logging_update(self.meets_eligibility_jobid, "f", str(e))

            # write
            sys.exit(1)


if __name__ == "__main__":
    s = MeetsEligibilityJobs()

    util.batch_logging_insert(s.all_jobid, 53, "all_meets_eligibility_jobs", "start_meets_eligibility_jobs.py")

    # run reference smart meter eligibility glue job
    print("{0}: Meets Eligibility glue job running...".format(datetime.now().strftime("%H:%M:%S")))
    s.submit_meets_eligibility_gluejob()

    util.batch_logging_update(s.all_jobid, "e")
