import sys
from datetime import datetime
import timeit
import subprocess

sys.path.append('..')
from common import process_glue_job as glue
from common import utils as util


class StartD18Jobs:
    def __init__(self):
        self.pythonAlias = util.get_pythonAlias()
        self.env = util.get_env()
        self.dir = util.get_dir()

    def submit_download_d18_job(self):
        """
        Calls the d18 download_d18.py script to download the the d18 files through sftp from Ensek and push the data to s3.
        :return: None
        """

        pythonAlias = util.get_pythonAlias()

        print("{0}: >>>> Downloading D18 files <<<<".format(datetime.now().strftime('%H:%M:%S')))
        try:
            start = timeit.default_timer()
            subprocess.run([pythonAlias, "download_d18.py"])
            print("{0}: download_d18 completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'),
                                                                          float(timeit.default_timer() - start)))
        except Exception as e:
            print("Error in download_d18 process :- " + str(e))
            sys.exit(1)

    def submit_process_d18_job(self):
        """
        Calls the d18 process_d18.py script to which processes the downloaded data from s3 and extracts the BPP and PPC co efficients.
        :return: None
        """

        print("{0}: >>>> Process D18 files <<<<".format(datetime.now().strftime('%H:%M:%S')))
        try:
            start = timeit.default_timer()
            subprocess.run([self.pythonAlias, "process_d18.py"])
            print("{0}: Process D18 files completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'),
                                                                               float(timeit.default_timer() - start)))
        except Exception as e:
            print("Error in download_d18 process :- " + str(e))
            sys.exit(1)

    def submit_d18_staging_gluejob(self):
        try:
            jobName = self.dir['glue_staging_job_name']
            s3_bucket = self.dir['s3_bucket']
            environment = self.env

            obj_stage = glue.ProcessGlueJob(job_name=jobName, s3_bucket=s3_bucket, environment=environment, processJob='d18')
            staging_job_response = obj_stage.run_glue_job()
            if staging_job_response:
                print("{0}: Staging Job Completed successfully".format(datetime.now().strftime('%H:%M:%S')))
                # return staging_job_response
            else:
                print("Error occurred in Staging Job")
                # return staging_job_response
                raise Exception
        except Exception as e:
            print("Error in Staging Job :- " + str(e))
            sys.exit(1)

    def submit_d18_gluejob(self):
        try:
            jobName = self.dir['glue_d18_job_name']
            s3_bucket = self.dir['s3_bucket']
            environment = self.env

            obj_d18 = glue.ProcessGlueJob(job_name=jobName, s3_bucket=s3_bucket, environment=environment, processJob='d18')
            d18_job_response = obj_d18.run_glue_job()
            if d18_job_response:
                print("{0}: D18 Job Completed successfully".format(datetime.now().strftime('%H:%M:%S')))
                # return staging_job_response
            else:
                print("Error occurred in D18 Job")
                # return staging_job_response
                raise Exception
        except Exception as e:
            print("Error in D18 Job :- " + str(e))
            sys.exit(1)


if __name__ == '__main__':

    s = StartD18Jobs()

    # run download d18 python script
    print("{0}: download_d18 job is running...".format(datetime.now().strftime('%H:%M:%S')))
    s.submit_download_d18_job()

    # run processing d18 python script
    print("{0}: process_d18 job is running...".format(datetime.now().strftime('%H:%M:%S')))
    s.submit_process_d18_job()

    # run staging glue job
    print("{0}: Staging Job running...".format(datetime.now().strftime('%H:%M:%S')))
    s.submit_d18_staging_gluejob()

    # run reference d18 glue job
    print("{0}: D18 Glue Job running...".format(datetime.now().strftime('%H:%M:%S')))
    s.submit_d18_gluejob()

    print("{0}: All D18 completed successfully".format(datetime.now().strftime('%H:%M:%S')))

