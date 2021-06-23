import sys
from datetime import datetime
import timeit
import subprocess
import timeit
from datetime import datetime


sys.path.append("..")
from common import process_glue_job as glue
from common import utils as util
from common import Refresh_UAT as refresh


class LandRegistry:
    def __init__(self):
        self.process_name = "Land Registry"
        self.pythonAlias = util.get_pythonAlias()
        self.env = util.get_env()
        self.dir = util.get_dir()

        self.all_jobid = util.get_jobID()
        self.landregistry_jobid = util.get_jobID()
        self.landregistry_staging_jobid = util.get_jobID()
        self.landregistry_ref_jobid = util.get_jobID()
        self.mirror_jobid = util.get_jobID()

    def submit_landregistry_gluejob(self):
        try:
            util.batch_logging_insert(
                self.landregistry_ref_jobid, 26, "landregistry_ref_glue_job", "start_landregistry_jobs.py"
            )

            jobName = self.dir["glue_land_registry_job_name"]
            s3_bucket = self.dir["s3_bucket"]
            environment = self.env

            obj_landregistry = glue.ProcessGlueJob(
                job_name=jobName, s3_bucket=s3_bucket, environment=environment, processJob="land_registry"
            )
            landregistry_job_response = obj_landregistry.run_glue_job()

            if landregistry_job_response:
                util.batch_logging_update(self.landregistry_ref_jobid, "e")
                print(
                    "{0}: Ref Glue Job Completed successfully for {1}".format(
                        datetime.now().strftime("%H:%M:%S"), self.process_name
                    )
                )
                # return staging_job_response
            else:
                print("{0}: Error occurred in {1} Job".format(datetime.now().strftime("%H:%M:%S"), self.process_name))
                # return staging_job_response
                raise Exception
        except Exception as e:
            util.batch_logging_update(self.landregistry_ref_jobid, "f", str(e))
            util.batch_logging_update(self.all_jobid, "f", str(e))
            print("Error in Ref Glue Job :- " + str(e))
            sys.exit(1)


if __name__ == "__main__":

    s = LandRegistry()

    util.batch_logging_insert(s.all_jobid, 106, "all_land_registry_jobs", "start_land_registry_jobs.py")

    # run glue job
    print("{0}: Glue Job running for {1}...".format(datetime.now().strftime("%H:%M:%S"), s.process_name))
    s.submit_landregistry_gluejob()

    print("{0}: All {1} completed successfully".format(datetime.now().strftime("%H:%M:%S"), s.process_name))

    util.batch_logging_update(s.all_jobid, "e")
