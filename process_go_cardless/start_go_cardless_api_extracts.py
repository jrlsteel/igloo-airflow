import sys
from datetime import datetime
import timeit
import subprocess
import json
import gocardless_pro

sys.path.append('..')
from common import process_glue_job as glue
from common import utils as util
from common import Refresh_UAT as refresh


class GoCardlessAPIExtracts:
    def __init__(self):
        self.pythonAlias = util.get_pythonAlias()
        self.env = util.get_env()
        self.dir = util.get_dir()
        self.all_jobid = util.get_jobID()
        self.goCardless_jobid = util.get_jobID()
        self.square_jobid = util.get_jobID()
        self.staging_jobid = util.get_jobID()
        self.ref_jobid = util.get_jobID()
        self.mirror_jobid = util.get_jobID()
        self.current_environment = self.env

    def retry_function(self, process, process_name):
        for i in range(0, 3):
            try:
                process
            except (json.decoder.JSONDecodeError, gocardless_pro.errors.GoCardlessInternalError,
                    gocardless_pro.errors.MalformedResponseError, subprocess.CalledProcessError) as e:
                util.batch_logging_update(self.goCardless_jobid, 'f', str(e))
                util.batch_logging_update(self.all_jobid, 'f', str(e))
                print("Error in Process {0} API extract :- {1}".format(process_name, str(e)))
                continue
            break

    def extract_go_cardless_payouts_job(self):
        """
        Calls the GoCardless Payouts API extract: go_cardless_payouts.py.
        :return: None
        """

        print("{0}: >>>> Process Go-Cardless Payouts API extract  <<<<".format(datetime.now().strftime('%H:%M:%S')))
        try:
            util.batch_logging_insert(self.goCardless_jobid, 400, 'go_cardless_payout.py',
                                      'start_go_cardless_api_extracts.py')
            start = timeit.default_timer()
            self.retry_function(process=subprocess.run([self.pythonAlias, "go_cardless_payout.py"], check=True),
                                process_name='Go-Cardless Payouts')
            util.batch_logging_update(self.goCardless_jobid, 'e')
            print("{0}: Process Go-Cardless Payouts API extract completed in {1:.2f} seconds".format(
                datetime.now().strftime('%H:%M:%S'),
                float(timeit.default_timer() - start)))
        except Exception as e:
            util.batch_logging_update(self.goCardless_jobid, 'f', str(e))
            util.batch_logging_update(self.all_jobid, 'f', str(e))
            print("Error in Process Go-Cardless Payouts API extract :- " + str(e))
            sys.exit(1)

    def extract_go_cardless_events_job(self):
        """
        Calls the GoCardless Events API extract: go_cardless_events.py.
        :return: None
        """

        print("{0}: >>>> Process Go-Cardless Events API extract  <<<<".format(datetime.now().strftime('%H:%M:%S')))
        try:
            util.batch_logging_insert(self.goCardless_jobid, 400, 'go_cardless_events.py',
                                      'start_go_cardless_api_extracts.py')
            start = timeit.default_timer()
            self.retry_function(process=subprocess.run([self.pythonAlias, "go_cardless_events.py"], check=True),
                                process_name='Go-Cardless Events')
            util.batch_logging_update(self.goCardless_jobid, 'e')
            print("{0}: Process Go-Cardless Events API extract completed in {1:.2f} seconds".format(
                datetime.now().strftime('%H:%M:%S'),
                float(timeit.default_timer() - start)))
        except Exception as e:
            util.batch_logging_update(self.goCardless_jobid, 'f', str(e))
            util.batch_logging_update(self.all_jobid, 'f', str(e))
            print("Error in Process Go-Cardless events API extract :- " + str(e))
            sys.exit(1)

    def extract_go_cardless_customers_job(self):
        """
        Calls the GoCardless Clients API extract: go_cardless_customers.py.
        :return: None
        """

        print("{0}: >>>> Process Go-Cardless Clients API extract  <<<<".format(datetime.now().strftime('%H:%M:%S')))
        try:
            util.batch_logging_insert(self.goCardless_jobid, 400, 'go_cardless_customers.py',
                                      'start_go_cardless_api_extracts.py')
            start = timeit.default_timer()
            self.retry_function(process=subprocess.run([self.pythonAlias, "go_cardless_customers.py"], check=True),
                                process_name='Go-Cardless Customers')
            util.batch_logging_update(self.goCardless_jobid, 'e')
            print("{0}: Process Go-Cardless Clients API extract completed in {1:.2f} seconds".format(
                datetime.now().strftime('%H:%M:%S'),
                float(timeit.default_timer() - start)))
        except Exception as e:
            util.batch_logging_update(self.goCardless_jobid, 'f', str(e))
            util.batch_logging_update(self.all_jobid, 'f', str(e))
            print("Error in Process Go-Cardless Clients API extract :- " + str(e))
            sys.exit(1)

    def submit_go_cardless_staging_gluejob(self):
        print("{0}: >>>> Process Go-Cardless staging glue job <<<<".format(datetime.now().strftime('%H:%M:%S')))
        try:
            jobName = self.dir['glue_staging_job_name']
            s3_bucket = self.dir['s3_bucket']
            environment = self.env
            util.batch_logging_insert(self.staging_jobid, 403, 'GoCardless staging gluejob',
                                      'start_go_cardless_api_extracts.py')
            obj_stage = glue.ProcessGlueJob(job_name=jobName, s3_bucket=s3_bucket, environment=environment,
                                            processJob='go_cardless')
            job_response = obj_stage.run_glue_job()
            if job_response:
                print("{0}: Staging Job Completed successfully".format(datetime.now().strftime('%H:%M:%S')))
            else:
                print("Error occurred in Staging Job")
                raise Exception
        except Exception as e:
            util.batch_logging_update(self.staging_jobid, 'f', str(e))
            util.batch_logging_update(self.all_jobid, 'f', str(e))
            print("Error in Staging Job :- " + str(e))
            sys.exit(1)

    def submit_go_cardless_ref_gluejob(self):
        print("{0}: >>>> Process Go-Cardless ref glue job <<<<".format(datetime.now().strftime('%H:%M:%S')))
        try:
            jobName = self.dir['glue_reporting_job_name']
            s3_bucket = self.dir['s3_bucket']
            environment = self.env
            util.batch_logging_insert(self.ref_jobid, 404, 'GoCardless ref gluejob',
                                      'start_go_cardless_api_extracts.py')
            obj_stage = glue.ProcessGlueJob(job_name=jobName, s3_bucket=s3_bucket, environment=environment,
                                            processJob='go_cardless_reporting')
            job_response = obj_stage.run_glue_job()
            if job_response:
                print("{0}: Ref Glue Job Completed successfully".format(datetime.now().strftime('%H:%M:%S')))
            else:
                print("Error occurred in Ref Glue Job")
                raise Exception
        except Exception as e:
            util.batch_logging_update(self.ref_jobid, 'f', str(e))
            util.batch_logging_update(self.all_jobid, 'f', str(e))
            print("Error in Ref Glue Job :- " + str(e))
            sys.exit(1)

    def submit_process_s3_mirror_job(self, source_input, destination_input):
        """
        Calls the utils/Refresh_UAT.py script which mirrors s3 data from source to destination folder
        :return: None
        """

        print("{0}: >>>> Process GoCardless mirror {1}<<<<".format(datetime.now().strftime('%H:%M:%S'),
                                                                   source_input.split('/')[-1]))
        try:
            util.batch_logging_insert(self.mirror_jobid, 405, 'GoCardless_extract_mirror-' + source_input + '-' +
                                      self.env, 'start_go_cardless_api_extracts.py')
            start = timeit.default_timer()
            r = refresh.SyncS3(source_input, destination_input)
            r.process_sync()

            util.batch_logging_update(self.mirror_jobid, 'e')
            print(
                "GoCardless_extract_mirror-" + source_input + "-" + self.env +
                " files completed in {1:.2f} seconds".format(
                    datetime.now().strftime('%H:%M:%S'), float(timeit.default_timer() - start)))
        except Exception as e:
            util.batch_logging_update(self.mirror_jobid, 'f', str(e))
            util.batch_logging_update(self.all_jobid, 'f', str(e))
            print("Error in process :- " + str(e))
            sys.exit(1)

    def submit_all_mirror_jobs(self, source_env):
        folder_list = [
            self.dir['s3_finance_goCardless_key']['Payouts'],
            self.dir['s3_finance_goCardless_key']['Events'],
            self.dir['s3_finance_goCardless_key']['Mandates-Files'],
            self.dir['s3_finance_goCardless_key']['Subscriptions-Files'],
            self.dir['s3_finance_goCardless_key']['Payments-Files'],
            self.dir['s3_finance_goCardless_key']['Refunds-Files'],
            self.dir['s3_finance_goCardless_key']['Clients']
        ]

        destination_bucket = "s3://{0}/".format(self.dir["s3_finance_bucket"])
        source_bucket = destination_bucket.replace(self.env, source_env)

        for folder in folder_list:
            print("{0}: GC {1} Mirror job running...".format(datetime.now().strftime('%H:%M:%S'), folder.strip('/')))
            source_input = source_bucket + folder
            destination_input = destination_bucket + folder
            s.submit_process_s3_mirror_job(source_input, destination_input)


if __name__ == '__main__':
    s = GoCardlessAPIExtracts()

    util.batch_logging_insert(s.all_jobid, 402, 'all_go_cardless_api_jobs', 'start_go_cardless_api_extracts.py')

    master_source = util.get_master_source("go_cardless")
    current_env = util.get_env()
    if master_source == current_env:  # current environment is master source, run the data extract script
        # Payouts API Endpoint
        print("{0}:  Go-Cardless Payouts API extract running...".format(datetime.now().strftime('%H:%M:%S')))
        s.extract_go_cardless_payouts_job()

        # Events API Endpoint
        print("{0}:  Go-Cardless Event API extract running...".format(datetime.now().strftime('%H:%M:%S')))
        s.extract_go_cardless_events_job()

        # Clients API Endpoint
        print("{0}:  Go-Cardless Customers API extract running...".format(datetime.now().strftime('%H:%M:%S')))
        s.extract_go_cardless_customers_job()

    else:  # current environment is not the master source, mirror the new data from the master source
        # Go Cardless Mirror Jobs
        print("{0}:  Go Cardless Mirror Jobs running...".format(datetime.now().strftime('%H:%M:%S')))
        s.submit_all_mirror_jobs(master_source)

    # Go Cardless Staging Jobs
    print("{0}:  Go Cardlesss Staging Jobs running...".format(datetime.now().strftime('%H:%M:%S')))
    s.submit_go_cardless_staging_gluejob()

    # Go Cardless Reporting Jobs
    print("{0}:  Go Cardlesss Reporting Jobs running...".format(datetime.now().strftime('%H:%M:%S')))
    s.submit_go_cardless_ref_gluejob()

    print("{0}: All Go-Cardless API extracts completed successfully".format(datetime.now().strftime('%H:%M:%S')))
    util.batch_logging_update(s.all_jobid, 'e')
