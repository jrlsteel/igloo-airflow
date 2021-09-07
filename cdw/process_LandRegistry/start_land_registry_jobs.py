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

    def submit_process_s3_mirror_job(self, source_input, destination_input):
        """
        Calls the utils/Refresh_UAT.py script which mirrors s3 data from source to destination fdlder
        :return: None
        """

        print("{0}: >>>> Process {1}<<<<".format(datetime.now().strftime("%H:%M:%S"), self.process_name))
        try:
            util.batch_logging_insert(
                self.mirror_jobid,
                24,
                "land_registry_extract_mirror-" + source_input + "-" + self.env,
                "start_land_registry_jobs.py",
            )
            start = timeit.default_timer()
            r = refresh.SyncS3(source_input, destination_input)
            r.process_sync()

            util.batch_logging_update(self.mirror_jobid, "e")
            print(
                "land_registry_extract_mirror--"
                + source_input
                + "-"
                + self.env
                + " files completed in {1:.2f} seconds".format(
                    datetime.now().strftime("%H:%M:%S"), float(timeit.default_timer() - start)
                )
            )
        except Exception as e:
            util.batch_logging_update(self.mirror_jobid, "f", str(e))
            util.batch_logging_update(self.all_jobid, "f", str(e))
            print("Error in process :- " + str(e))
            sys.exit(1)

    def submit_stage1_job(self):
        """
        Calls the process_land_registry.py.py script which processes the land registry data from http://landregistry.data.gov.uk
        :return: None
        """
        print("{0}: >>>> Process {1}<<<<".format(datetime.now().strftime("%H:%M:%S"), self.process_name))
        try:
            util.batch_logging_insert(
                self.landregistry_jobid, 24, "landregistry_extract_pyscript", "start_land_registry_jobs.py"
            )
            start = timeit.default_timer()
            subprocess.run([self.pythonAlias, "process_land_registry.py"], check=True)
            util.batch_logging_update(self.landregistry_jobid, "e")
            print(
                "{0}: Processing of {2} Data completed in {1:.2f} seconds".format(
                    datetime.now().strftime("%H:%M:%S"), float(timeit.default_timer() - start), self.process_name
                )
            )
        except Exception as e:
            util.batch_logging_update(self.landregistry_jobid, "f", str(e))
            util.batch_logging_update(self.all_jobid, "f", str(e))
            print("Error in process :- " + str(e))
            sys.exit(1)

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
    if s.env == "prod":
        # run processing land regsitry python script
        print("{0}: {1} job is running...".format(datetime.now().strftime("%H:%M:%S"), s.process_name))
        s.submit_stage1_job()

    else:
        # # run processing mirror job
        print("Land Registry  Mirror job is running...".format(datetime.now().strftime("%H:%M:%S"), s.process_name))
        source_input = "s3://igloo-data-warehouse-prod/stage1/LandRegistry/"
        destination_input = "s3://igloo-data-warehouse-" + s.env + "/stage1/LandRegistry/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

    # run staging glue job
    print("{0}: Staging Job running for {1}...".format(datetime.now().strftime("%H:%M:%S"), s.process_name))
    s.submit_stage2_job()

    # run glue job
    print("{0}: Glue Job running for {1}...".format(datetime.now().strftime("%H:%M:%S"), s.process_name))
    s.submit_landregistry_gluejob()

    print("{0}: All {1} completed successfully".format(datetime.now().strftime("%H:%M:%S"), s.process_name))

    util.batch_logging_update(s.all_jobid, "e")
