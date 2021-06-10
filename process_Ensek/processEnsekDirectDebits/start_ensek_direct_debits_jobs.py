import sys
from datetime import datetime
import timeit
import subprocess

sys.path.append("../..")
from common import process_glue_job as glue
from common import utils as util


class StartDirectDebitJobs:
    def __init__(self):
        self.pythonAlias = util.get_pythonAlias()
        self.env = util.get_env()
        self.dir = util.get_dir()

    def submit_direct_debits_extract_job(self):
        """
        Calls the Ensek Direct process_ensek_direct_debits.py
        script to which processes ensek registrations status.
        :return: None
        """

        print("{0}: >>>> Process Ensek Direct Debits   <<<<".format(datetime.now().strftime("%H:%M:%S")))
        try:
            start = timeit.default_timer()
            subprocess.run([self.pythonAlias, "process_ensek_direct_debits.py"], check=True)
            print(
                "{0}: Process Ensek Direct Debits completed in {1:.2f} seconds".format(
                    datetime.now().strftime("%H:%M:%S"), float(timeit.default_timer() - start)
                )
            )
        except Exception as e:
            print("Error in Process Ensek Direct Debits  :- " + str(e))
            sys.exit(1)

    def submit_direct_debits_extract_staging_gluejob(self):
        try:
            jobName = self.dir["glue_staging_job_name"]
            s3_bucket = self.dir["s3_bucket"]
            environment = self.env

            obj_stage = glue.ProcessGlueJob(
                job_name=jobName, s3_bucket=s3_bucket, environment=environment, processJob="direct-debits"
            )
            job_response = obj_stage.run_glue_job()
            if job_response:
                print(
                    "{0}: Staging Ensek Direct Debits Job Completed successfully".format(
                        datetime.now().strftime("%H:%M:%S")
                    )
                )
                # return staging_job_response
            else:
                print("Error occurred in Ensek Direct Debits Staging Job")
                # return staging_job_response
                raise Exception
        except Exception as e:
            print("Error in Ensek Direct Debits Staging Job :- " + str(e))
            sys.exit(1)

    def submit_direct_debits_extract_reference_gluejob(self):
        try:
            jobname = self.dir["glue_registrations_direct_debits_status_job_name"]
            s3_bucket = self.dir["s3_bucket"]
            environment = self.env

            obj_submit_registrations_meterpoints_status_Gluejob = glue.ProcessGlueJob(
                job_name=jobname, s3_bucket=s3_bucket, environment=environment, processJob="ref_direct_debits"
            )
            job_response = obj_submit_registrations_meterpoints_status_Gluejob.run_glue_job()
            if job_response:
                print(
                    "{0}: Ensek Direct Debits Reference Glue Job completed successfully".format(
                        datetime.now().strftime("%H:%M:%S")
                    )
                )
            else:
                print("Error occurred in Ensek Direct Debits Reference Glue Job")
                raise Exception
        except Exception as e:
            print("Error in Ensek Direct Debits Reference DB Job :- " + str(e))
            sys.exit(1)


if __name__ == "__main__":

    s = StartMeterpointsJobs()

    # Ensek Direct Debits Extract
    print("{0}:  Ensek Direct Debits Jobs running...".format(datetime.now().strftime("%H:%M:%S")))
    s.submit_direct_debits_extract_job()

    # #Ensek Meterpoints Staging Jobs
    print("{0}:  Ensek  Direct Debits Staging Jobs running...".format(datetime.now().strftime("%H:%M:%S")))
    s.submit_direct_debits_extract_staging_gluejob()

    # Ensek Meterpoints ref Tables Jobs
    print("{0}: Direct Debits Ref Jobs Running...".format(datetime.now().strftime("%H:%M:%S")))
    s.submit_direct_debits_extract_reference_gluejob()

    print("{0}: All Direct Debits Status completed successfully".format(datetime.now().strftime("%H:%M:%S")))
