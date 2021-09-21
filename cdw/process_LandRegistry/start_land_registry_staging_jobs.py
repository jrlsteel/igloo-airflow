import sys
from datetime import datetime
import timeit
import subprocess
import timeit
from datetime import datetime


from cdw.common import process_glue_job as glue
from cdw.common import utils as util
from cdw.common import Refresh_UAT as refresh


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

    def submit_stage2_job(self):
        try:
            util.batch_logging_insert(
                self.landregistry_staging_jobid, 25, "landregistry_staging_glue_job", "start_land_registry_jobs.py"
            )
            jobName = self.dir["glue_staging_job_name"]
            s3_bucket = self.dir["s3_bucket"]
            environment = self.env
            obj_stage = glue.ProcessGlueJob(
                job_name=jobName, s3_bucket=s3_bucket, environment=environment, processJob="land_registry"
            )
            staging_job_response = obj_stage.run_glue_job()
            if staging_job_response:
                util.batch_logging_update(self.landregistry_staging_jobid, "e")
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
            util.batch_logging_update(self.landregistry_staging_jobid, "f", str(e))
            util.batch_logging_update(self.all_jobid, "f", str(e))
            print("Error in Staging Job :- " + str(e))
            sys.exit(1)


if __name__ == "__main__":

    s = LandRegistry()

    util.batch_logging_insert(s.all_jobid, 106, "all_land_registry_jobs", "start_land_registry_jobs.py")

    # run staging glue job
    print("{0}: Staging Job running for {1}...".format(datetime.now().strftime("%H:%M:%S"), s.process_name))
    s.submit_stage2_job()

    util.batch_logging_update(s.all_jobid, "e")
