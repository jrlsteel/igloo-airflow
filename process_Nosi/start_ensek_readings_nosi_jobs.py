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
        self.nosi_jobid = util.get_jobID()
        self.nosi_download_jobid = util.get_jobID()
        self.nosi_staging_jobid = util.get_jobID()
        self.nosi_ref_jobid = util.get_jobID()

    # def submit_readings_internal_job(self):
    #     """
    #     Calls the Registration Status process_ensek_registration_meterpoint_status.py"script to which processes ensek registrations stus.
    #     :return: None
    #     """
    #
    #     print("{0}: >>>> Process Ensek Internal Readings  <<<<".format(datetime.now().strftime('%H:%M:%S')))
    #     try:
    #         start = timeit.default_timer()
    #         subprocess.run([self.pythonAlias, "process_ensek_internal_readings.py"])
    #         print("{0}: Process Ensek Internal Readings completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'),
    #                                                                            float(timeit.default_timer() - start)))
    #     except Exception as e:
    #         print("Error in Process Ensek Internal Readings process :- " + str(e))
    #         sys.exit(1)

    def submit_internal_readings_nosi_staging_gluejob(self):
        try:
            jobName = self.dir['glue_staging_job_name']
            util.batch_logging_insert(self.nosi_staging_jobid, 43, 'nosi_staging_glue_job', 'start_ensek_readings_nosi_jobs.py')
            s3_bucket = self.dir['s3_bucket']
            environment = self.env

            obj_stage = glue.ProcessGlueJob(job_name=jobName, s3_bucket=s3_bucket, environment=environment,
                                            processJob='ensek-readings-nosi')
            job_response = obj_stage.run_glue_job()
            if job_response:
                util.batch_logging_update(self.nosi_staging_jobid, 'e')
                print("{0}: Staging Job Completed successfully".format(datetime.now().strftime('%H:%M:%S')))
                # return staging_job_response
            else:
                print("Error occurred in Staging Job")
                # return staging_job_response
                raise Exception
        except Exception as e:
            util.batch_logging_update(self.nosi_staging_jobid, 'f', str(e))
            util.batch_logging_update(self.all_jobid, 'f', str(e))
            print("Error in Staging Job :- " + str(e))
            sys.exit(1)

    def submit_internal_readings_nosi_gluejob(self):
        try:
            jobname = self.dir['glue_internal_readings_nosi_job_name']
            util.batch_logging_insert(self.nosi_ref_jobid, 44, 'nosi_ref_glue_job', 'start_ensek_readings_nosi_jobs.py')
            s3_bucket = self.dir['s3_bucket']
            environment = self.env

            obj_submit_registrations_meterpoints_status_Gluejob = glue.ProcessGlueJob(job_name=jobname, s3_bucket=s3_bucket, environment=environment,
                                                 processJob='ensek_ref_readings_nosi')
            job_response = obj_submit_registrations_meterpoints_status_Gluejob.run_glue_job()
            if job_response:
                util.batch_logging_update(self.nosi_ref_jobid, 'e')
                print("{0}: Ensek Internal Readings NOSI Glue Job completed successfully".format(datetime.now().strftime('%H:%M:%S')))
                # returnsubmit_internal_readings_Gluejob
            else:
                print("Error occurred in Internal Readings NOSI Glue Job")
                # return submit_internal_readings_nosi_Gluejob
                raise Exception
        except Exception as e:
            util.batch_logging_update(self.nosi_ref_jobid, 'f', str(e))
            util.batch_logging_update(self.all_jobid, 'f', str(e))
            print("Error in Internal Readings NOSI  DB Job :- " + str(e))
            sys.exit(1)


if __name__ == '__main__':

    s = StartReadingsNOSIJobs()

    util.batch_logging_insert(s.all_jobid, 132, 'all_readings_internal_nosi_jobs', 'start_ensek_readings_nosi_jobs.py')

    #Ensek Internal Readings NOSI Staging
    print("{0}:  Ensek Internal Readings NOSI Jobs running...".format(datetime.now().strftime('%H:%M:%S')))
    s.submit_internal_readings_nosi_staging_gluejob()

    # Ensek Internal Readings NOSI Glue Job
    print("{0}:  Ensek Internal Readings NOSI Jobs running...".format(datetime.now().strftime('%H:%M:%S')))
    s.submit_internal_readings_nosi_gluejob()


    print("{0}: All Ensek Internal Readings Nosi completed successfully".format(datetime.now().strftime('%H:%M:%S')))

    util.batch_logging_update(s.all_jobid, 'e')

