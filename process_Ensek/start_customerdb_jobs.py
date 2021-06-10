import sys
from datetime import datetime

sys.path.append("..")

from process_Ensek import processAllEnsekPAScripts as ae
from common import process_glue_job as glue

from common import utils as util


class StartCustomerDBJobs:
    def __init__(self):
        self.env = util.get_env()
        self.dir = util.get_dir()

    def submit_customerDB_Gluejob(self):
        try:
            jobname = self.dir["glue_customerDB_job_name"]
            s3_bucket = self.dir["s3_bucket"]
            environment = self.env

            obj_customerDB = glue.ProcessGlueJob(
                job_name=jobname, s3_bucket=s3_bucket, environment=environment, processJob="mirror_all_cdb"
            )
            job_response = obj_customerDB.run_glue_job()
            if job_response:
                print("{0}: CustomerDB Glue Job completed successfully".format(datetime.now().strftime("%H:%M:%S")))
                # return staging_job_response
            else:
                print("Error occurred in CustomerDB Glue Job")
                # return staging_job_response
                raise Exception
        except Exception as e:
            print("Error in Customer DB Job :- " + str(e))
            sys.exit(1)


if __name__ == "__main__":
    s = StartCustomerDBJobs()

    # run Customer DB
    print("{0}:  CustomerDB Jobs running...".format(datetime.now().strftime("%H:%M:%S")))
    s.submit_customerDB_Gluejob()

    print("{0}: All jobs completed successfully".format(datetime.now().strftime("%H:%M:%S")))
