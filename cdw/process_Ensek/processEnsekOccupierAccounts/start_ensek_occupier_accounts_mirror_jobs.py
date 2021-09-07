import sys
from datetime import datetime
import timeit
import subprocess

# import  common.process_glue_job  as glue
from cdw.common import process_glue_job as glue
from cdw.common import utils as util
from cdw.common import Refresh_UAT as refresh
from cdw.conf import config as con


class StartOccupierAccountsJobs:
    def __init__(self):
        self.process_name = "Occupier Accounts"
        self.pythonAlias = util.get_pythonAlias()
        self.env = util.get_env()
        self.dir = util.get_dir()
        self.occupier_accounts_extract_jobid = util.get_jobID()
        self.occupier_accounts_staging_jobid = util.get_jobID()
        self.occupier_accounts_reference_jobid = util.get_jobID()
        self.occupier_accounts_job_id = util.get_jobID()
        self.jobName = self.dir["glue_estimated_advance_job_name"]

    def submit_process_s3_mirror_job(self, source_input, destination_input):
        """
        Calls the utils/Refresh_UAT.py script which mirrors s3 data from source to destination fdlder
        :return: None
        """

        print("{0}: >>>> Process {1}<<<<".format(datetime.now().strftime("%H:%M:%S"), self.process_name))
        try:
            util.batch_logging_insert(
                self.occupier_accounts_job_id,
                21,
                "occupier_account_extract_mirror-" + source_input + "-" + self.env,
                "start_ensek_occupier_accounts_mirror_jobs.py",
            )
            start = timeit.default_timer()
            r = refresh.SyncS3(source_input, destination_input)

            r.process_sync(
                env={
                    "AWS_ACCESS_KEY_ID": con.s3_config["access_key"],
                    "AWS_SECRET_ACCESS_KEY": con.s3_config["secret_key"],
                }
            )

            util.batch_logging_update(self.occupier_accounts_job_id, "e")
            print(
                "occupier_account_extract_mirror-"
                + source_input
                + "-"
                + self.env
                + " files completed in {1:.2f} seconds".format(
                    datetime.now().strftime("%H:%M:%S"), float(timeit.default_timer() - start)
                )
            )
        except Exception as e:
            util.batch_logging_update(self.occupier_accounts_job_id, "f", str(e))
            util.batch_logging_update(self.occupier_accounts_job_id, "f", str(e))
            print("Error in process :- " + str(e))
            sys.exit(1)

    def submit_occupier_accounts_extract_job(self):
        """
        Calls the Occupier Accounts process_ensek_occupier_accounts.py script to which processes Occupuier Acocunts
        Allows the reduction in Range calls for most of the Ensek API requests.
        :return: None
        """

        print("{0}: >>>> Process Ensek Occupier Accounts   <<<<".format(datetime.now().strftime("%H:%M:%S")))
        try:
            util.batch_logging_insert(
                self.occupier_accounts_extract_jobid,
                51,
                "Occupier Accounts pyscript",
                "start_ensek_occupier_accounts_jobs.py",
            )
            start = timeit.default_timer()
            subprocess.run([self.pythonAlias, "process_ensek_occupier_accounts.py"], check=True)
            print(
                "{0}: Process Ensek Occupier  completed in {1:.2f} seconds".format(
                    datetime.now().strftime("%H:%M:%S"), float(timeit.default_timer() - start)
                )
            )
            util.batch_logging_update(self.occupier_accounts_extract_jobid, "e")
        except Exception as e:
            util.batch_logging_update(self.occupier_accounts_extract_jobid, "f", str(e))
            util.batch_logging_update(self.occupier_accounts_job_id, "f", str(e))
            print("Error in Process Occupier Accounts  :- " + str(e))
            sys.exit(1)


if __name__ == "__main__":

    s = StartOccupierAccountsJobs()

    util.batch_logging_insert(
        s.occupier_accounts_job_id, 103, "all_occupier_accounts_job", "start_ensek_occupier_accounts_mirror_jobs.py"
    )

    print("Running Environment: {0}".format(s.env.upper()))

    if s.env in ["prod", "newprod"]:
        # run Occupier Accounts Jobs
        print("{0}: Occupier Accounts Jobs running...".format(datetime.now().strftime("%H:%M:%S")))
        s.submit_occupier_accounts_extract_job()

    elif s.env in ["preprod", "uat", "dev"]:
        s3_destination_bucket = s.dir["s3_bucket"]
        s3_source_bucket = s.dir["s3_source_bucket"]

        # run Occupier Accounts Jobs Jobs in UAT
        print("{0}:  Occupier Accounts Jobs running...".format(datetime.now().strftime("%H:%M:%S")))

        print(
            "Ensek Occupier Accounts Job Mirror  job is running...".format(
                datetime.now().strftime("%H:%M:%S"), s.process_name
            )
        )
        source_input = "s3://" + s3_source_bucket + "/stage2/stage2_OccupierAccounts/"
        destination_input = "s3://" + s3_destination_bucket + "/stage2/stage2_OccupierAccounts/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

    print("{0}: job completed successfully".format(datetime.now().strftime("%H:%M:%S")))

    util.batch_logging_update(s.occupier_accounts_job_id, "e")
