import sys
from datetime import datetime
import timeit
import subprocess

sys.path.append('..')
from common import process_glue_job as glue
from common import utils as util


class LandRegistry:
    def __init__(self):
        self.process_name = "Land Registry"
        self.pythonAlias = util.get_pythonAlias()
        self.env = util.get_env()
        self.dir = util.get_dir()
        self.landregistry_jobid = util.get_jobID()
        self.landregistry_staging_jobid = util.get_jobID()
        self.landregistry_ref_jobid = util.get_jobID()

    def submit_stage1_job(self):
        """
        Calls the process_land_registry.py.py script which processes the land registry data from http://landregistry.data.gov.uk
        :return: None
        """
        print("{0}: >>>> Process {1}<<<<".format(datetime.now().strftime('%H:%M:%S'), self.process_name))
        try:
            util.batch_logging_insert(self.landregistry_jobid, 24, 'landregistry_extract_pyscript', 'start_land_registry_jobs.py')
            start = timeit.default_timer()
            subprocess.run([self.pythonAlias, "process_land_registry.py"])
            util.batch_logging_update(self.landregistry_staging_jobid, 'e')
            print("{0}: Processing of {2} Data completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'), float(timeit.default_timer() - start), self.process_name))
        except Exception as e:
            util.batch_logging_update(self.landregistry_jobid, 'f', str(e))
            print("Error in process :- " + str(e))
            sys.exit(1)

    def submit_stage2_job(self):
        try:
            util.batch_logging_insert(self.landregistry_staging_jobid, 25, 'landregistry_staging_glue_job','start_land_registry_jobs.py')
            jobName = self.dir['glue_staging_job_name']
            s3_bucket = self.dir['s3_bucket']
            environment = self.env
            obj_stage = glue.ProcessGlueJob(job_name=jobName, s3_bucket=s3_bucket, environment=environment, processJob='land_registry')
            staging_job_response = obj_stage.run_glue_job()
            if staging_job_response:
                util.batch_logging_update(self.landregistry_staging_jobid, 'e')
                print("{0}: Staging Job Completed successfully for {1}".format(datetime.now().strftime('%H:%M:%S'), self.process_name))
                # return staging_job_response
            else:
                print("Error occurred in {0} Staging Job".format(self.process_name))
                # return staging_job_response
                raise Exception
        except Exception as e:
            util.batch_logging_update(self.landregistry_staging_jobid, 'f', str(e))
            print("Error in Staging Job :- " + str(e))
            sys.exit(1)

    def submit_landregistry_gluejob(self):
        try:
            jobName = self.dir['glue_land_registry_job_name']
            s3_bucket = self.dir['s3_bucket']
            environment = self.env

            obj_landregistry = glue.ProcessGlueJob(job_name=jobName, s3_bucket=s3_bucket, environment=environment, processJob='land_registry')
            landregistry_job_response = obj_landregistry.run_glue_job()
            if landregistry_job_response:
                print("{0}: Ref Glue Job Completed successfully for {1}".format(datetime.now().strftime('%H:%M:%S'), self.process_name))
                # return staging_job_response
            else:
                print("{0}: Error occurred in {1} Job".format(datetime.now().strftime('%H:%M:%S'), self.process_name))
                # return staging_job_response
                raise Exception
        except Exception as e:
            print("Error in Ref Glue Job :- " + str(e))
            sys.exit(1)


if __name__ == '__main__':

    s = LandRegistry()

    # run processing land regsitry python script
    print("{0}: {1} job is running...".format(datetime.now().strftime('%H:%M:%S'), s.process_name))
    s.submit_stage1_job()

    # run staging glue job
    print("{0}: Staging Job running for {1}...".format(datetime.now().strftime('%H:%M:%S'), s.process_name))
    s.submit_stage2_job()

    # run glue job
    print("{0}: Glue Job running for {1}...".format(datetime.now().strftime('%H:%M:%S'), s.process_name))
    s.submit_landregistry_gluejob()

    print("{0}: All {1} completed successfully".format(datetime.now().strftime('%H:%M:%S'), s.process_name))

