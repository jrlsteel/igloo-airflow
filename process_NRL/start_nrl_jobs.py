import sys
from datetime import datetime
import timeit
import subprocess

sys.path.append('..')
from common import process_glue_job as glue
from common import utils as util


class StartReadingsNOSIJobs:
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

    def submit_download_nrl_job(self):

        pythonAlias = util.get_pythonAlias()

        print("{0}: >>>> Downloading NRL files <<<<".format(datetime.now().strftime('%H:%M:%S')))
        try:
            util.batch_logging_insert(self.nrl_download_jobid, 45, 'nrl_download_pyscript', 'start_nrl_jobs.py')
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
            util.batch_logging_insert(self.nrl_jobid, 46, 'nrl_extract_pyscript','start_nrl_jobs.py')
            start = timeit.default_timer()
            subprocess.run([self.pythonAlias, "process_NRL.py"])
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
            util.batch_logging_insert(self.nrl_staging_jobid, 43, 'nrl_staging_glue_job', 'start_nrl_jobs.py')
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
            util.batch_logging_insert(self.nrl_ref_jobid, 44, 'nrl_ref_glue_job', 'start_nrl_jobs.py')
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

    s = StartReadingsNOSIJobs()

    util.batch_logging_insert(s.all_jobid, 133, 'all_nrl_jobs', 'start_nrl_jobs.py')

    print("{0}: download_NRL job is running...".format(datetime.now().strftime('%H:%M:%S')))
    s.submit_download_nrl_job()

    print("{0}: process_NRL job is running...".format(datetime.now().strftime('%H:%M:%S')))
    s.submit_process_nrl_job()

    print("{0}:  Ensek NRL Staging Jobs running...".format(datetime.now().strftime('%H:%M:%S')))
    s.submit_nrl_staging_gluejob()

    print("{0}:  Ensek NRL Ref Glue Jobs running...".format(datetime.now().strftime('%H:%M:%S')))
    s.submit_nrl_ref_gluejob()

    print("{0}: All NRL completed successfully".format(datetime.now().strftime('%H:%M:%S')))

    util.batch_logging_update(s.all_jobid, 'e')

