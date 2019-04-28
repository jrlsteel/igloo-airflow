import sys
from datetime import datetime
import timeit
import subprocess

sys.path.append('../..')
from common import process_glue_job as glue
from common import utils as util


class StartReadingsJobs:
    def __init__(self):
        self.pythonAlias = util.get_pythonAlias()
        self.env = util.get_env()
        self.dir = util.get_dir()

    def submit_readings_internal_job(self):
        """
        Calls the Registration Status process_ensek_registration_meterpoint_status.py"script to which processes ensek registrations stus.
        :return: None
        """

        print("{0}: >>>> Process Ensek Internal Readings  <<<<".format(datetime.now().strftime('%H:%M:%S')))
        try:
            start = timeit.default_timer()
            subprocess.run([self.pythonAlias, "process_ensek_internal_readings.py"])
            print("{0}: Process Ensek Internal Readings completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'),
                                                                               float(timeit.default_timer() - start)))
        except Exception as e:
            print("Error in Process Ensek Internal Readings process :- " + str(e))
            sys.exit(1)

    def submit_internal_readings_staging_gluejob(self):
        try:
            jobName = self.dir['glue_staging_job_name']
            s3_bucket = self.dir['s3_bucket']
            environment = self.env

            obj_stage = glue.ProcessGlueJob(job_name=jobName, s3_bucket=s3_bucket, environment=environment,
                                            processJob='internal-readings')
            job_response = obj_stage.run_glue_job()
            if job_response:
                print("{0}: Staging Job Completed successfully".format(datetime.now().strftime('%H:%M:%S')))
                # return staging_job_response
            else:
                print("Error occurred in Staging Job")
                # return staging_job_response
                raise Exception
        except Exception as e:
            print("Error in Staging Job :- " + str(e))
            sys.exit(1)

    # def submit_internal_readings_gluejob(self):
    #     try:
    #         jobname = self.dir['glue_internal_readings_job_name']
    #         s3_bucket = self.dir['s3_bucket']
    #         environment = self.env
    #
    #         obj_submit_registrations_meterpoints_status_Gluejob = glue.ProcessGlueJob(job_name=jobname, s3_bucket=s3_bucket, environment=environment,
    #                                              processJob='internal_readings')
    #         job_response = obj_submit_registrations_meterpoints_status_Gluejob.run_glue_job()
    #         if job_response:
    #             print("{0}: Ensek Internal Readings Glue Job completed successfully".format(datetime.now().strftime('%H:%M:%S')))
    #             # returnsubmit_internal_readings_Gluejob
    #         else:
    #             print("Error occurred in Internal Readings Glue Job")
    #             # return submit_internal_readings_Gluejob
    #             raise Exception
    #     except Exception as e:
    #         print("Error in Internal Readings DB Job :- " + str(e))
    #         sys.exit(1)


if __name__ == '__main__':

    s = StartReadingsJobs()

    #Ensek Internal Readings
    print("{0}:  Ensek Internal Readings Jobs running...".format(datetime.now().strftime('%H:%M:%S')))
    s.submit_readings_internal_job()



    print("{0}: All Ensek Internal Readings completed successfully".format(datetime.now().strftime('%H:%M:%S')))

