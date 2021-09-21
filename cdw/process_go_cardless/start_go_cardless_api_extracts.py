import sys
import timeit
import subprocess

from cdw.common import process_glue_job as glue
from cdw.common import process_glue_crawler
from cdw.common import utils as util
from cdw.common import Refresh_UAT as refresh
from cdw.conf import config
import traceback


class GoCardlessAPIExtracts:
    def __init__(self, logger):
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
            "ref": util.get_jobID(),
        }
        self.current_environment = self.env
        if logger is None:
            self.iglog = util.IglooLogger()
        else:
            self.iglog = logger

    def run_subprocess_extract(self, pyfile, parent_pyfile, job_code, job_name, quit_on_fail=True):
        """
        Calls the pyfile using subprocess.run and handles logging
        :return: None
        """

        self.iglog.in_prod_env(">>>> Process Go-Cardless {0} API extract  <<<<".format(job_name))
        try:
            # log job start
            util.batch_logging_insert(self.job_ids[job_name], job_code, pyfile, parent_pyfile)
            # start timer
            start = timeit.default_timer()
            # run job
            subprocess.run([self.pythonAlias, pyfile], check=True)
            # log job end
            util.batch_logging_update(self.job_ids[job_name], "e")
            self.iglog.in_prod_env(
                "Process Go-Cardless {0} API extract completed in {1:.2f} seconds".format(
                    job_name, float(timeit.default_timer() - start)
                )
            )
        except Exception as e:
            self.iglog.in_prod_env(traceback.format_exc())
            # log job failure
            util.batch_logging_update(self.job_ids[job_name], "f", str(e))
            self.iglog.in_prod_env("Error in Process Go-Cardless {0} API extract :- ".format(job_name) + str(e))
            # if quit_on_fail is True, log all-job failure & exit
            if quit_on_fail:
                util.batch_logging_update(self.job_ids["all"], "f", str(e))
                sys.exit(1)

    def submit_go_cardless_staging_gluejob(self):
        self.iglog.in_prod_env(">>>> Process Go-Cardless staging glue job <<<<")
        try:
            jobName = self.dir["glue_staging_job_name"]
            s3_bucket = self.dir["s3_bucket"]
            environment = self.env
            util.batch_logging_insert(
                self.job_ids["staging"], 403, "GoCardless staging gluejob", "start_go_cardless_api_extracts.py"
            )
            obj_stage = glue.ProcessGlueJob(
                job_name=jobName, s3_bucket=s3_bucket, environment=environment, processJob="go_cardless"
            )
            job_response = obj_stage.run_glue_job()
            if job_response:
                self.iglog.in_prod_env("Staging Job Completed successfully")
                util.batch_logging_update(self.job_ids["staging"], "e")
            else:
                self.iglog.in_prod_env("Error occurred in Staging Job")
                raise Exception
        except Exception as e:
            self.iglog.in_prod_env(traceback.format_exc())
            util.batch_logging_update(self.job_ids["staging"], "f", str(e))
            util.batch_logging_update(self.job_ids["all"], "f", str(e))
            self.iglog.in_prod_env("Error in Staging Job :- " + str(e))
            sys.exit(1)

    def submit_go_cardless_ref_gluejob(self):
        self.iglog.in_prod_env(">>>> Process Go-Cardless ref glue job <<<<")
        try:
            jobName = self.dir["glue_reporting_job_name"]
            s3_bucket = self.dir["s3_bucket"]
            environment = self.env
            util.batch_logging_insert(
                self.job_ids["ref"], 404, "GoCardless ref gluejob", "start_go_cardless_api_extracts.py"
            )
            obj_stage = glue.ProcessGlueJob(
                job_name=jobName, s3_bucket=s3_bucket, environment=environment, processJob="go_cardless_reporting"
            )
            job_response = obj_stage.run_glue_job()
            if job_response:
                self.iglog.in_prod_env("Ref Glue Job Completed successfully")
                util.batch_logging_update(self.job_ids["ref"], "e")
            else:
                self.iglog.in_prod_env("Error occurred in Ref Glue Job")
                raise Exception
        except Exception as e:
            self.iglog.in_prod_env(traceback.format_exc())
            util.batch_logging_update(self.job_ids["ref"], "f", str(e))
            util.batch_logging_update(self.job_ids["all"], "f", str(e))
            self.iglog.in_prod_env("Error in Ref Glue Job :- " + str(e))
            sys.exit(1)

    def submit_process_s3_mirror_job(self, source_input, destination_input):
        """
        Calls the utils/Refresh_UAT.py script which mirrors s3 data from source to destination folder
        :return: None
        """

        self.iglog.in_prod_env(">>>> Process GoCardless mirror {0}<<<<".format(source_input.split("/")[-1]))
        try:
            util.batch_logging_insert(
                self.job_ids["mirror"],
                405,
                "GoCardless_extract_mirror-" + source_input + "-" + self.env,
                "start_go_cardless_api_extracts.py",
            )
            start = timeit.default_timer()
            r = refresh.SyncS3(source_input, destination_input)
            r.process_sync(
                env={
                    "AWS_ACCESS_KEY_ID": config.s3_config["access_key"],
                    "AWS_SECRET_ACCESS_KEY": config.s3_config["secret_key"],
                }
            )

            util.batch_logging_update(self.job_ids["mirror"], "e")
            self.iglog.in_prod_env(
                "GoCardless_extract_mirror-{src}-{env} files completed in {time:.2f} seconds".format(
                    src=source_input, env=self.env, time=float(timeit.default_timer() - start)
                )
            )
        except Exception as e:
            self.iglog.in_prod_env(traceback.format_exc())
            util.batch_logging_update(self.job_ids["mirror"], "f", str(e))
            util.batch_logging_update(self.job_ids["all"], "f", str(e))
            self.iglog.in_prod_env("Error in process :- " + str(e))
            sys.exit(1)

    def submit_all_mirror_jobs(self, source_env):
        folder_list = [
            self.dir["s3_finance_goCardless_key"]["Payouts"],
            self.dir["s3_finance_goCardless_key"]["Events"],
            self.dir["s3_finance_goCardless_key"]["Mandates-Files"],
            self.dir["s3_finance_goCardless_key"]["Subscriptions-Files"],
            self.dir["s3_finance_goCardless_key"]["Payments-Files"],
            self.dir["s3_finance_goCardless_key"]["Refunds-Files"],
            self.dir["s3_finance_goCardless_key"]["Clients"],
        ]

        destination_bucket = "s3://{0}".format(self.dir["s3_finance_bucket"])
        source_dir = util.get_dir(source_env)
        source_bucket = "s3://{}".format(source_dir["s3_finance_bucket"])

        for folder in folder_list:
            source_path = source_bucket + folder
            self.iglog.in_test_env("source: {0}".format(source_path))
            destination_path = destination_bucket + folder
            self.iglog.in_test_env("destination: {0}".format(destination_path))
            s.submit_process_s3_mirror_job(source_path, destination_path)


if __name__ == "__main__":
    iglog = util.IglooLogger(source="GC Parent Script")
    s = GoCardlessAPIExtracts(iglog)

    util.batch_logging_insert(s.job_ids["all"], 402, "all_go_cardless_api_jobs", "start_go_cardless_api_extracts.py")

    # Events API Endpoint (master source handling is done within this method so this is always run)
    s.run_subprocess_extract(
        pyfile="go_cardless_events.py",
        parent_pyfile="start_go_cardless_api_extracts.py",
        job_code=400,
        job_name="events",
    )

    master_source = util.get_master_source("go_cardless")
    current_env = util.get_env()
    iglog.in_prod_env("Current environment: {0}, Master_Source: {1}".format(current_env, master_source))
    if master_source == current_env:  # current environment is master source, run the data extract script
        # Payouts API Endpoint
        s.run_subprocess_extract(
            pyfile="go_cardless_payout.py",
            parent_pyfile="start_go_cardless_api_extracts.py",
            job_code=400,
            job_name="payouts",
        )

        # Clients API Endpoint
        s.run_subprocess_extract(
            pyfile="go_cardless_customers.py",
            parent_pyfile="start_go_cardless_api_extracts.py",
            job_code=400,
            job_name="customers",
        )

    else:  # current environment is not the master source, mirror the new data from the master source
        # Go Cardless Mirror Jobs
        iglog.in_prod_env("Go Cardless Mirror Jobs running")
        s.submit_all_mirror_jobs(master_source)

        # run the crawler to pick up any new partitions
        iglog.in_prod_env("Running the events crawler")
        process_glue_crawler.run_glue_crawler("data-crawler-fin-gc-events-stage1")

    # Go Cardless Staging Jobs
    iglog.in_prod_env("Go Cardlesss Staging Jobs running")
    s.submit_go_cardless_staging_gluejob()

    # Go Cardless Reporting Jobs
    iglog.in_prod_env("Go Cardlesss Reporting Jobs running")
    s.submit_go_cardless_ref_gluejob()

    iglog.in_prod_env("All Go-Cardless API extracts completed successfully")
    util.batch_logging_update(s.job_ids["all"], "e")
