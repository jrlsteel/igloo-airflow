import sys
from datetime import datetime
import timeit
import subprocess

sys.path.append('../..')
from common import process_glue_job as glue
from common import utils as util


class StartTransactionsJobs:
    def __init__(self):
        self.pythonAlias = util.get_pythonAlias()
        self.env = util.get_env()
        self.dir = util.get_dir()

    def submit_transactions_job(self):
        """
        Calls the Tariff process_ensek_transactions_job.py"script to which processes ensek transactions.
        :return: None
        """

        print("{0}: >>>> Process Ensek Transactions <<<<".format(datetime.now().strftime('%H:%M:%S')))
        try:
            start = timeit.default_timer()
            subprocess.run([self.pythonAlias, "process_ensek_transactions.py"])
            print("{0}: Process Ensek Transactions completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'),
                                                                               float(timeit.default_timer() - start)))
        except Exception as e:
            print("Error in Process Ensek Transactions process :- " + str(e))
            sys.exit(1)

    def submit_transactions_staging_gluejob(self):
        try:
            jobName = self.dir['glue_staging_job_name']
            s3_bucket = self.dir['s3_bucket']
            environment = self.env

            obj_stage = glue.ProcessGlueJob(job_name=jobName, s3_bucket=s3_bucket, environment=environment,
                                            processJob='transactions')
            job_response = obj_stage.run_glue_job()
            if job_response:
                print("{0}: Transactions Staging Job Completed successfully".format(datetime.now().strftime('%H:%M:%S')))
                # return staging_job_response
            else:
                print("Error occurred in Transactions Staging Job")
                # return staging_job_response
                raise Exception
        except Exception as e:
            print("Error in Transactions Staging Job :- " + str(e))
            sys.exit(1)

    def submit_transactions_gluejob(self):
        try:
            jobname = self.dir['glue_transactions_job_name']
            s3_bucket = self.dir['s3_bucket']
            environment = self.env

            obj_submit_tarnsctaions_Gluejob = glue.ProcessGlueJob(job_name=jobname, s3_bucket=s3_bucket, environment=environment,
                                                 processJob='transactions')
            job_response = obj_submit_transactions_Gluejob.run_glue_job()
            if job_response:
                print("{0}: Transactions Glue Job completed successfully".format(datetime.now().strftime('%H:%M:%S')))

            else:
                print("Error occurred in Transactions Glue Job")
                # return submit_transactions_Gluejob
                raise Exception
        except Exception as e:
            print("Error in Transactions DB Job :- " + str(e))
            sys.exit(1)


if __name__ == '__main__':

    s = StartTransactionsJobs()

    #Transcations Ensek Extract
    print("{0}:  Transactions Jobs running...".format(datetime.now().strftime('%H:%M:%S')))
    s.submit_transactions_job()

    #Transactions Staging Jobs
    print("{0}:  Transactions Staging Jobs running...".format(datetime.now().strftime('%H:%M:%S')))
    s.submit_transactions_staging_gluejob()

    #Transactions ref Tables Jobs
    print("{0}: Transactions Ref Jobs Running...".format(datetime.now().strftime('%H:%M:%S')))
    s.submit_transactions_gluejob()


    print("{0}: All Transactions completed successfully".format(datetime.now().strftime('%H:%M:%S')))

