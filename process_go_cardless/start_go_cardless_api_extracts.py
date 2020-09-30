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
        self.job_ids = {
            "all": util.get_jobID(),
            "payouts": util.get_jobID(),
            "events": util.get_jobID(),
            "customers": util.get_jobID(),
            "mirror": util.get_jobID(),
            "staging": util.get_jobID(),
            "ref": util.get_jobID()
        }
        self.current_environment = self.env

    def run_subprocess_extract(self, pyfile, parent_pyfile, job_code, job_name, quit_on_fail=True):
        """
        Calls the pyfile using subprocess.run and handles logging
        :return: None
        """

        print(
            "{0}: >>>> Process Go-Cardless {1} API extract  <<<<".format(datetime.now().strftime('%H:%M:%S'), job_name))
        try:
            # log job start
            util.batch_logging_insert(self.job_ids[job_name], job_code, pyfile, parent_pyfile)
            # start timer
            start = timeit.default_timer()
            # run job
            subprocess.run([self.pythonAlias, pyfile], check=True)
            # log job end
            util.batch_logging_update(self.job_ids[job_name], 'e')
            print("{0}: Process Go-Cardless {2} API extract completed in {1:.2f} seconds".format(
                datetime.now().strftime('%H:%M:%S'),
                float(timeit.default_timer() - start),
                job_name))
        except Exception as e:
            # log job failure
            util.batch_logging_update(self.job_ids[job_name], 'f', str(e))
            print("Error in Process Go-Cardless {0} API extract :- ".format(job_name) + str(e))
            # if quit_on_fail is True, log all-job failure & exit
            if quit_on_fail:
                util.batch_logging_update(self.job_ids["all"], 'f', str(e))
                sys.exit(1)

    def submit_go_cardless_staging_gluejob(self):
        print("{0}: >>>> Process Go-Cardless staging glue job <<<<".format(datetime.now().strftime('%H:%M:%S')))
        try:
            jobName = self.dir['glue_staging_job_name']
            s3_bucket = self.dir['s3_bucket']
            environment = self.env
            util.batch_logging_insert(self.job_ids["staging"], 403, 'GoCardless staging gluejob',
                                      'start_go_cardless_api_extracts.py')
            obj_stage = glue.ProcessGlueJob(job_name=jobName, s3_bucket=s3_bucket, environment=environment,
                                            processJob='go_cardless')
            job_response = obj_stage.run_glue_job()
            if job_response:
                print("{0}: Staging Job Completed successfully".format(datetime.now().strftime('%H:%M:%S')))
                util.batch_logging_update(self.job_ids["staging"], 'e')
            else:
                print("Error occurred in Staging Job")
                raise Exception
        except Exception as e:
            util.batch_logging_update(self.job_ids["staging"], 'f', str(e))
            util.batch_logging_update(self.job_ids["all"], 'f', str(e))
            print("Error in Staging Job :- " + str(e))
            sys.exit(1)

    def submit_go_cardless_ref_gluejob(self):
        print("{0}: >>>> Process Go-Cardless ref glue job <<<<".format(datetime.now().strftime('%H:%M:%S')))
        try:
            jobName = self.dir['glue_reporting_job_name']
            s3_bucket = self.dir['s3_bucket']
            environment = self.env
            util.batch_logging_insert(self.job_ids["ref"], 404, 'GoCardless ref gluejob',
                                      'start_go_cardless_api_extracts.py')
            obj_stage = glue.ProcessGlueJob(job_name=jobName, s3_bucket=s3_bucket, environment=environment,
                                            processJob='go_cardless_reporting')
            job_response = obj_stage.run_glue_job()
            if job_response:
                print("{0}: Ref Glue Job Completed successfully".format(datetime.now().strftime('%H:%M:%S')))
                util.batch_logging_update(self.job_ids["ref"], 'e')
            else:
                print("Error occurred in Ref Glue Job")
                raise Exception
        except Exception as e:
            util.batch_logging_update(self.job_ids["ref"], 'f', str(e))
            util.batch_logging_update(self.job_ids["all"], 'f', str(e))
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
            util.batch_logging_insert(self.job_ids["mirror"], 405, 'GoCardless_extract_mirror-' + source_input + '-' +
                                      self.env, 'start_go_cardless_api_extracts.py')
            start = timeit.default_timer()
            r = refresh.SyncS3(source_input, destination_input)
            r.process_sync()

            util.batch_logging_update(self.job_ids["mirror"], 'e')
            print(
                "GoCardless_extract_mirror-" + source_input + "-" + self.env +
                " files completed in {1:.2f} seconds".format(
                    datetime.now().strftime('%H:%M:%S'), float(timeit.default_timer() - start)))
        except Exception as e:
            util.batch_logging_update(self.job_ids["mirror"], 'f', str(e))
            util.batch_logging_update(self.job_ids["all"], 'f', str(e))
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

        destination_bucket = "s3://{0}".format(self.dir["s3_finance_bucket"])
        source_bucket = destination_bucket.replace(self.env, source_env)

        for folder in folder_list:
            print("{0}: GC {1} Mirror job running...".format(datetime.now().strftime('%H:%M:%S'), folder.strip('/')))
            source_path = source_bucket + folder
            print("source: {0}".format(source_path))
            destination_path = destination_bucket + folder
            print("destination: {0}".format(destination_path))
            s.submit_process_s3_mirror_job(source_path, destination_path)


if __name__ == '__main__':
    s = GoCardlessAPIExtracts()

    util.batch_logging_insert(s.job_ids["all"], 402, 'all_go_cardless_api_jobs', 'start_go_cardless_api_extracts.py')

    master_source = util.get_master_source("go_cardless")
    current_env = util.get_env()
    print("Current environment: {0}, Master_Source: {1}".format(current_env, master_source))
    if master_source == current_env:  # current environment is master source, run the data extract script
        # Payouts API Endpoint
        print("{0}:  Go-Cardless Payouts API extract running...".format(datetime.now().strftime('%H:%M:%S')))
        s.run_subprocess_extract(pyfile='go_cardless_payout.py',
                                 parent_pyfile='start_go_cardless_api_extracts.py',
                                 job_code=400,
                                 job_name='payouts')

        # Events API Endpoint
        print("{0}:  Go-Cardless Event API extract running...".format(datetime.now().strftime('%H:%M:%S')))
        s.run_subprocess_extract(pyfile='go_cardless_events.py',
                                 parent_pyfile='start_go_cardless_api_extracts.py',
                                 job_code=400,
                                 job_name='events')

        # Clients API Endpoint
        print("{0}:  Go-Cardless Customers API extract running...".format(datetime.now().strftime('%H:%M:%S')))
        s.run_subprocess_extract(pyfile='go_cardless_customers.py',
                                 parent_pyfile='start_go_cardless_api_extracts.py',
                                 job_code=400,
                                 job_name='customers')

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
    util.batch_logging_update(s.job_ids["all"], 'e')
