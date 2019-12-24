import sys
from datetime import datetime

sys.path.append('..')
from common import process_glue_job as glue
from common import utils as util


class ReportingJobs:
    def __init__(self):
        self.env = util.get_env()
        self.dir = util.get_dir()
        self.reporting_jobid = util.get_jobID()

    def submit_daily_reporting_batch_gluejob(self):
        try:
            job_name = self.dir['glue_reporting_job_name']
            s3_bucket = self.dir['s3_bucket']
            environment = self.env

            # Batch Logging
            util.batch_logging_insert(self.reporting_jobid, 200, 'daily_reporting_gluejob', 'start_reporting_jobs.py')

            daily_reporting_job_response = glue.ProcessGlueJob(job_name=job_name,
                                                               s3_bucket=s3_bucket,
                                                               environment=environment,
                                                               processJob='daily_reporting').run_glue_job()

            if daily_reporting_job_response:
                print("{0}: Daily Reporting Job Completed successfully".format(datetime.now().strftime('%H:%M:%S')))
                # Batch Logging
                util.batch_logging_update(self.reporting_jobid, 'e')
            else:
                print("Error occurred in Daily Reporting Job")
                # return staging_job_response
                raise Exception
        except Exception as e:
            print("Error in Daily Reporting Glue Job: " + str(e))

            # Batch Logging
            util.batch_logging_update(self.reporting_jobid, 'f', str(e))

            # write
            sys.exit(1)


if __name__ == '__main__':
    rj = ReportingJobs()
    rj.submit_daily_reporting_batch_gluejob()
