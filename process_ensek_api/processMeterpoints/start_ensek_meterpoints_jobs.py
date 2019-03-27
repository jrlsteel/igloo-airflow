import sys
from datetime import datetime
import timeit
import subprocess

sys.path.append('..')
from common import process_glue_job as glue
from common import utils as util


class StartMeterpointsJobs:
    def __init__(self):
        self.pythonAlias = util.get_pythonAlias()
        self.env = util.get_env()
        self.dir = util.get_dir()

    def submit_meterpoints_extract_job(self):
        """
        Calls the Registration Status process_ensek_registration_status.py"script to which processes ensek registrations stus.
        :return: None
        """

        print("{0}: >>>> Process Ensek Registrations by Meterpoint Status  <<<<".format(datetime.now().strftime('%H:%M:%S')))
        try:
            start = timeit.default_timer()
            subprocess.run([self.pythonAlias, "process_ensek_meterpoints.py"])
            print("{0}: Process Ensek Meterpoints completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'),
                                                                               float(timeit.default_timer() - start)))
        except Exception as e:
            print("Error in Process Meterpoints Registrations by Meterpoint Status process :- " + str(e))
            sys.exit(1)

    def submit_meterpoints_extract_staging_gluejob(self):
        try:
            jobName = self.dir['glue_staging_job_name']
            s3_bucket = self.dir['s3_bucket']
            environment = self.env

            obj_stage = glue.ProcessGlueJob(job_name=jobName, s3_bucket=s3_bucket, environment=environment,
                                            processJob='reg-mp-status')
            job_response = obj_stage.run_glue_job()
            if job_response:
                print("{0}: Staging Meterpoints Job Completed successfully".format(datetime.now().strftime('%H:%M:%S')))
                # return staging_job_response
            else:
                print("Error occurred in Staging Job")
                # return staging_job_response
                raise Exception
        except Exception as e:
            print("Error in Staging Job :- " + str(e))
            sys.exit(1)

    # def submit_registrations_meterpoints_status_gluejob(self):
    #     try:
    #         jobname = self.dir['glue_registrations_meterpoints_status_job_name']
    #         s3_bucket = self.dir['s3_bucket']
    #         environment = self.env
    #
    #         obj_submit_registrations_meterpoints_status_Gluejob = glue.ProcessGlueJob(job_name=jobname, s3_bucket=s3_bucket, environment=environment,
    #                                              processJob='reg_mp_status')
    #         job_response = obj_submit_registrations_meterpoints_status_Gluejob.run_glue_job()
    #         if job_response:
    #             print("{0}: Registrations Meterpoints Status Glue Job completed successfully".format(datetime.now().strftime('%H:%M:%S')))
    #             # returnsubmit_registrations_meterpoints_status_Gluejob
    #         else:
    #             print("Error occurred in Registrations Meterpoints Status Glue Job")
    #             # return submit_registrations_meterpoints_status_Gluejob
    #             raise Exception
    #     except Exception as e:
    #         print("Error in Registrations Meterpoints Status DB Job :- " + str(e))
    #         sys.exit(1)


if __name__ == '__main__':

    s = StartMeterpointsJobs()

    #Ensek Meterpoints Extract
    print("{0}:  Registrations Meterpoints Status Jobs running...".format(datetime.now().strftime('%H:%M:%S')))
    s.submit_registrations_meterpoints_status_job()

    #Ensek Meterpoints Staging Jobs
    print("{0}:  Registrations Meterpoints Status Staging Jobs running...".format(datetime.now().strftime('%H:%M:%S')))
    s.submit_registrations_meterpoints_status_staging_gluejob()

    #Ensek Meterpoints ref Tables Jobs
    # print("{0}: Registrations Meterpoints Status Ref Jobs Running...".format(datetime.now().strftime('%H:%M:%S')))
    # s.submit_registrations_meterpoints_status_gluejob()



    print("{0}: All Registrations Meterpoints Status completed successfully".format(datetime.now().strftime('%H:%M:%S')))

