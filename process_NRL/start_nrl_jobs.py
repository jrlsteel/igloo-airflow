import sys
from datetime import datetime
import timeit
import subprocess

sys.path.append('..')
from common import process_glue_job as glue
from common import utils as util
from common import Refresh_UAT as refresh

class StartReadingsNRLJobs:
    def __init__(self):
        self.pythonAlias = util.get_pythonAlias()
        self.env = util.get_env()
        self.dir = util.get_dir()
        self.all_jobid = util.get_jobID()
        self.nrl_jobid = util.get_jobID()
        self.nrl_download_jobid = util.get_jobID()
        self.nrl_staging_jobid = util.get_jobID()
        self.nrl_ref_jobid = util.get_jobID()
        self.process_name = 'NRL'
        self.mirror_jobid = util.get_jobID()

    def submit_process_s3_mirror_job(self, source_input, destination_input):
        """
        Calls the utils/Refresh_UAT.py script which mirrors s3 data from source to destination fdlder
        :return: None
        """

        print("{0}: >>>> Process {1}<<<<".format(datetime.now().strftime('%H:%M:%S'), self.process_name))
        try:
            util.batch_logging_insert(self.mirror_jobid, 47, 'nrl_extract_mirror-' + source_input + '-' + self.env,
                                      'start_nrl_jobs.py')
            start = timeit.default_timer()
            r = refresh.SyncS3(source_input, destination_input)
            r.process_sync()

            util.batch_logging_update(self.mirror_jobid, 'e')
            print(
                "nrl_extract_mirror--" + source_input + "-" + self.env + " files completed in {1:.2f} seconds".format(
                    datetime.now().strftime('%H:%M:%S'), float(timeit.default_timer() - start)))
        except Exception as e:
            util.batch_logging_update(self.mirror_jobid, 'f', str(e))
            util.batch_logging_update(self.all_jobid, 'f', str(e))
            print("Error in process :- " + str(e))
            sys.exit(1)

    def submit_download_nrl_job(self):

        pythonAlias = util.get_pythonAlias()

        print("{0}: >>>> Downloading NRL files <<<<".format(datetime.now().strftime('%H:%M:%S')))
        try:
            util.batch_logging_insert(self.nrl_download_jobid, 46, 'nrl_download_pyscript', 'start_nrl_jobs.py')
            start = timeit.default_timer()
            subprocess.run([pythonAlias, "download_nrl.py"])
            util.batch_logging_update(self.nrl_download_jobid, 'e')
            print("{0}: download_NRL completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'),
                                                                          float(timeit.default_timer() - start)))
        except Exception as e:
            util.batch_logging_update(self.nrl_download_jobid, 'f', str(e))
            util.batch_logging_update(self.all_jobid, 'f', str(e))
            print("Error in download_NRL process :- " + str(e))
            sys.exit(1)

    def submit_process_nrl_job(self):

        print("{0}: >>>> Process NRL files <<<<".format(datetime.now().strftime('%H:%M:%S')))
        try:
            util.batch_logging_insert(self.nrl_jobid, 47, 'nrl_extract_pyscript','start_nrl_jobs.py')
            start = timeit.default_timer()
            subprocess.run([self.pythonAlias, "process_nrl.py"])
            util.batch_logging_update(self.nrl_jobid, 'e')
            print("{0}: Process NRL files completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'),float(timeit.default_timer() - start)))

        except Exception as e:
            util.batch_logging_update(self.nrl_jobid, 'f', str(e))
            util.batch_logging_update(self.all_jobid, 'f', str(e))
            print("Error in download_NRL process :- " + str(e))
            sys.exit(1)

    def submit_nrl_staging_gluejob(self):
        try:
            jobName = self.dir['glue_staging_job_name']
            util.batch_logging_insert(self.nrl_staging_jobid, 48, 'nrl_staging_glue_job', 'start_nrl_jobs.py')
            s3_bucket = self.dir['s3_bucket']
            environment = self.env

            obj_stage = glue.ProcessGlueJob(job_name=jobName, s3_bucket=s3_bucket, environment=environment, processJob='nrl')
            job_response = obj_stage.run_glue_job()
            if job_response:
                util.batch_logging_update(self.nrl_staging_jobid, 'e')
                print("{0}: NRL Staging Job Completed successfully".format(datetime.now().strftime('%H:%M:%S')))
                # return staging_job_response
            else:
                print("Error occurred in Staging Job")
                raise Exception

        except Exception as e:
            util.batch_logging_update(self.nrl_staging_jobid, 'f', str(e))
            util.batch_logging_update(self.all_jobid, 'f', str(e))
            print("Error in Staging Job :- " + str(e))
            sys.exit(1)

    def submit_nrl_ref_gluejob(self):
        try:
            jobname = self.dir['glue_nrl_job_name']
            util.batch_logging_insert(self.nrl_ref_jobid, 49, 'nrl_ref_glue_job', 'start_nrl_jobs.py')
            s3_bucket = self.dir['s3_bucket']
            environment = self.env

            obj_submit_registrations_meterpoints_status_Gluejob = glue.ProcessGlueJob(job_name=jobname, s3_bucket=s3_bucket, environment=environment, processJob='nrl')
            job_response = obj_submit_registrations_meterpoints_status_Gluejob.run_glue_job()
            if job_response:
                util.batch_logging_update(self.nrl_ref_jobid, 'e')
                print("{0}: NRL Ref Glue Job completed successfully".format(datetime.now().strftime('%H:%M:%S')))
            else:
                print("Error occurred in NRL Glue Job")
                raise Exception

        except Exception as e:
            util.batch_logging_update(self.nrl_ref_jobid, 'f', str(e))
            util.batch_logging_update(self.all_jobid, 'f', str(e))
            print("Error in NRL  DB Job :- " + str(e))
            sys.exit(1)


if __name__ == '__main__':

    s = StartReadingsNRLJobs()

    util.batch_logging_insert(s.all_jobid, 133, 'all_nrl_jobs', 'start_nrl_jobs.py')

    if s.env == 'prod':
        print("{0}: download_NRL job is running...".format(datetime.now().strftime('%H:%M:%S')))
        s.submit_download_nrl_job()

        print("{0}: process_NRL job is running...".format(datetime.now().strftime('%H:%M:%S')))
        s.submit_process_nrl_job()

    else:
        # # run processing mirror job
        print("NRL  Mirror job is running...".format(datetime.now().strftime('%H:%M:%S'), s.process_name))
        source_input = "s3://igloo-data-warehouse-prod/stage1/ReadingsNRL/"
        destination_input = "s3://igloo-data-warehouse-" + s.env + "/stage1/ReadingsNRL/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

    print("{0}:  Ensek NRL Staging Jobs running...".format(datetime.now().strftime('%H:%M:%S')))
    s.submit_nrl_staging_gluejob()

    print("{0}:  Ensek NRL Ref Glue Jobs running...".format(datetime.now().strftime('%H:%M:%S')))
    s.submit_nrl_ref_gluejob()

    print("{0}: All NRL completed successfully".format(datetime.now().strftime('%H:%M:%S')))

    util.batch_logging_update(s.all_jobid, 'e')

