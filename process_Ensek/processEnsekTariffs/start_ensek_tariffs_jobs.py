import sys
from datetime import datetime
import timeit
import subprocess

sys.path.append("../..")
from common import process_glue_job as glue
from common import utils as util


class StartTariffHistoryJobs:
    def __init__(self):
        self.pythonAlias = util.get_pythonAlias()
        self.env = util.get_env()
        self.dir = util.get_dir()

    def submit_tariff_history_job(self):
        """
        Calls the Tariff process_ensek_tariff_history.py"script to which processes ensek Tariff History.
        :return: None
        """

        print("{0}: >>>> Process Ensek Tariff History <<<<".format(datetime.now().strftime("%H:%M:%S")))
        try:
            start = timeit.default_timer()
            subprocess.run([self.pythonAlias, "process_ensek_tariff_history.py"], check=True)
            print(
                "{0}: Process Ensek Tariff History completed in {1:.2f} seconds".format(
                    datetime.now().strftime("%H:%M:%S"), float(timeit.default_timer() - start)
                )
            )
        except Exception as e:
            print("Error in Process Ensek Tariff History process :- " + str(e))
            sys.exit(1)

    def submit_tariff_history_staging_gluejob(self):
        try:
            jobName = self.dir["glue_staging_job_name"]
            s3_bucket = self.dir["s3_bucket"]
            environment = self.env

            obj_stage = glue.ProcessGlueJob(
                job_name=jobName, s3_bucket=s3_bucket, environment=environment, processJob="tariff-history"
            )
            job_response = obj_stage.run_glue_job()
            if job_response:
                print(
                    "{0}: Tariff History Staging Job Completed successfully".format(datetime.now().strftime("%H:%M:%S"))
                )
                # return staging_job_response
            else:
                print("Error occurred in Tariff History Staging Job")
                # return staging_job_response
                raise Exception
        except Exception as e:
            print("Error in Tariff History Staging Job :- " + str(e))
            sys.exit(1)

    def submit_tariff_history_gluejob(self):
        try:
            jobname = self.dir["glue_tariff_history_job_name"]
            s3_bucket = self.dir["s3_bucket"]
            environment = self.env

            obj_submit_tariff_history_Gluejob = glue.ProcessGlueJob(
                job_name=jobname, s3_bucket=s3_bucket, environment=environment, processJob="tariff_history"
            )
            job_response = obj_submit_tariff_history_Gluejob.run_glue_job()
            if job_response:
                print("{0}: Tariff History Glue Job completed successfully".format(datetime.now().strftime("%H:%M:%S")))

            else:
                print("Error occurred in Tariff History Glue Job")
                # return submit_tariff_history_Gluejob
                raise Exception
        except Exception as e:
            print("Error in Tariff History DB Job :- " + str(e))
            sys.exit(1)


if __name__ == "__main__":

    s = StartTariffHistoryJobs()

    # Tariff History Ensek Extract
    print("{0}:  Tariff History Status Jobs running...".format(datetime.now().strftime("%H:%M:%S")))
    s.submit_tariff_history_job()

    # Tariff History Staging Jobs
    print("{0}:  Tariff History Staging Jobs running...".format(datetime.now().strftime("%H:%M:%S")))
    s.submit_tariff_history_staging_gluejob()

    # Tariff History ref Tables Jobs
    print("{0}: Tariff History Ref Jobs Running...".format(datetime.now().strftime("%H:%M:%S")))
    s.submit_tariff_history_gluejob()

    print("{0}: All Tariff History completed successfully".format(datetime.now().strftime("%H:%M:%S")))
