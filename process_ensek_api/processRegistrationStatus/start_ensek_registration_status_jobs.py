import sys
from datetime import datetime
import timeit
import subprocess

sys.path.append('..')
from common import process_glue_job as glue
from common import utils as util


class StartRegistrationsMeterpointsStatusJobs:
    def __init__(self):
        self.pythonAlias = util.get_pythonAlias()
        self.env = util.get_env()
        self.dir = util.get_dir()

    def submit_registrations_meterpoints_status_scripts(self):
        try:
            process_ensek_registrations_status = ae.process_ensek_registrations_status()
            if process_ensek_registrations_status:
                print("{0}: Registration Meterpoint Status Scripts job completed successfully".format(datetime.now().strftime('%H:%M:%S')))
                # return process_ensek_registrations_status
            else:
                print("Error occurred in Registration Meterpoint Status Scripts job")
                # return process_ensek_registrations_status
                raise Exception
        except Exception as e:
            print("Error in Registration Meterpoint Status Scripts :- " + str(e))
            sys.exit(1)

    def submit_registrations_meterpoints_status_staging_Gluejob(self):
        try:
            jobName = self.dir['glue_staging_job_name']
            s3_bucket = self.dir['s3_bucket']
            environment = self.env

            obj_stage = glue.ProcessGlueJob(job_name=jobName, s3_bucket=s3_bucket, environment=environment,
                                            processJob='registrations_meterpoints_status')
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

    def submit_registrations_meterpoints_status_Gluejob(self):
        try:
            jobname = self.dir['glue_registrations_meterpoints_status_job_name']
            s3_bucket = self.dir['s3_bucket']
            environment = self.env

            obj_submit_registrations_meterpoints_status_Gluejob = glue.ProcessGlueJob(job_name=jobname, s3_bucket=s3_bucket, environment=environment,
                                                 processJob='')
            job_response = obj_submit_registrations_meterpoints_status_Gluejob.run_glue_job()
            if job_response:
                print("{0}: Registrations Meterpoints Status Glue Job completed successfully".format(datetime.now().strftime('%H:%M:%S')))
                # returnsubmit_registrations_meterpoints_status_Gluejob
            else:
                print("Error occurred in Registrations Meterpoints Status Glue Job")
                # return submit_registrations_meterpoints_status_Gluejob
                raise Exception
        except Exception as e:
            print("Error in Registrations Meterpoints Status DB Job :- " + str(e))
            sys.exit(1)


if __name__ == '__main__':

    s = StartRegistrationsMeterpointsStatusJobs()

    # run Registrations Meterpoints Status
    print("{0}:  Registrations Meterpoints Status Jobs running...".format(datetime.now().strftime('%H:%M:%S')))
    s.submit_registrations_meterpoints_status_scripts()

    # run Registrations Meterpoints Status Staging Jobs
    print("{0}:  Registrations Meterpoints Status Staging Jobs running...".format(datetime.now().strftime('%H:%M:%S')))
    s.submit_registrations_meterpoints_status_staging_Gluejob()

    # run staging glue job
    print("{0}: Registrations Meterpoints Status Ref Jobs Running...".format(datetime.now().strftime('%H:%M:%S')))
    s.submit_registrations_meterpoints_status_Gluejob()



    print("{0}: All Registrations Meterpoints Status completed successfully".format(datetime.now().strftime('%H:%M:%S')))

