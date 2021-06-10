import sys
from datetime import datetime
import timeit
import subprocess

sys.path.append("..")
from common import process_glue_job as glue
from common import utils as util
from common import Refresh_UAT as refresh
from conf import config as con


class StartReadingsNRLJobs:
    def __init__(self):
        self.pythonAlias = util.get_pythonAlias()
        self.env = util.get_env()
        self.dir = util.get_dir()
        self.all_jobid = util.get_jobID()
        self.nrl_jobid = util.get_jobID()
        self.nrl_download_jobid = util.get_jobID()
        self.nrl_staging_jobid = util.get_jobID()
        self.nrl_ref_jobid = util.get_jobID()
        self.process_name = "NRL"
        self.mirror_jobid = util.get_jobID()

    def submit_process_s3_mirror_job(self, source_input, destination_input):
        """
        Calls the utils/Refresh_UAT.py script which mirrors s3 data from source to destination fdlder
        :return: None
        """

        print("{0}: >>>> Process {1}<<<<".format(datetime.now().strftime("%H:%M:%S"), self.process_name))
        try:
            util.batch_logging_insert(
                self.mirror_jobid, 47, "nrl_extract_mirror-" + source_input + "-" + self.env, "start_nrl_jobs.py"
            )
            start = timeit.default_timer()
            r = refresh.SyncS3(source_input, destination_input)
            r.process_sync(
                env={
                    "AWS_ACCESS_KEY_ID": con.s3_config["access_key"],
                    "AWS_SECRET_ACCESS_KEY": con.s3_config["secret_key"],
                }
            )

            util.batch_logging_update(self.mirror_jobid, "e")
            print(
                "nrl_extract_mirror--"
                + source_input
                + "-"
                + self.env
                + " files completed in {1:.2f} seconds".format(
                    datetime.now().strftime("%H:%M:%S"), float(timeit.default_timer() - start)
                )
            )
        except Exception as e:
            util.batch_logging_update(self.mirror_jobid, "f", str(e))
            util.batch_logging_update(self.all_jobid, "f", str(e))
            print("Error in process :- " + str(e))
            sys.exit(1)

    def submit_download_nrl_job(self):

        pythonAlias = util.get_pythonAlias()

        print("{0}: >>>> Downloading NRL files <<<<".format(datetime.now().strftime("%H:%M:%S")))
        try:
            util.batch_logging_insert(self.nrl_download_jobid, 46, "nrl_download_pyscript", "start_nrl_jobs.py")
            start = timeit.default_timer()
            subprocess.run([pythonAlias, "download_nrl.py"], check=True)
            util.batch_logging_update(self.nrl_download_jobid, "e")
            print(
                "{0}: download_NRL completed in {1:.2f} seconds".format(
                    datetime.now().strftime("%H:%M:%S"), float(timeit.default_timer() - start)
                )
            )
        except Exception as e:
            util.batch_logging_update(self.nrl_download_jobid, "f", str(e))
            util.batch_logging_update(self.all_jobid, "f", str(e))
            print("Error in download_NRL process :- " + str(e))
            sys.exit(1)

    def submit_process_nrl_job(self):

        print("{0}: >>>> Process NRL files <<<<".format(datetime.now().strftime("%H:%M:%S")))
        try:
            util.batch_logging_insert(self.nrl_jobid, 47, "nrl_extract_pyscript", "start_nrl_jobs.py")
            start = timeit.default_timer()
            subprocess.run([self.pythonAlias, "process_nrl.py"], check=True)
            util.batch_logging_update(self.nrl_jobid, "e")
            print(
                "{0}: Process NRL files completed in {1:.2f} seconds".format(
                    datetime.now().strftime("%H:%M:%S"), float(timeit.default_timer() - start)
                )
            )

        except Exception as e:
            util.batch_logging_update(self.nrl_jobid, "f", str(e))
            util.batch_logging_update(self.all_jobid, "f", str(e))
            print("Error in download_NRL process :- " + str(e))
            sys.exit(1)


if __name__ == "__main__":

    s = StartReadingsNRLJobs()

    util.batch_logging_insert(s.all_jobid, 133, "all_nrl_jobs", "start_nrl_jobs.py")

    s3_destination_bucket = s.dir["s3_bucket"]
    s3_source_bucket = s.dir["s3_source_bucket"]

    if s.env == "newprod":

        print("{0}: download_NRL job is running...".format(datetime.now().strftime("%H:%M:%S")))
        s.submit_download_nrl_job()

        print("{0}: process_NRL job is running...".format(datetime.now().strftime("%H:%M:%S")))
        s.submit_process_nrl_job()

    else:
        # # run processing mirror job
        print("NRL  Mirror job is running...".format(datetime.now().strftime("%H:%M:%S"), s.process_name))
        source_input = "s3://" + s3_source_bucket + "/stage1/ReadingsNRL/"
        destination_input = "s3://" + s3_destination_bucket + "/stage1/ReadingsNRL/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

    print("{0}: All NRL completed successfully".format(datetime.now().strftime("%H:%M:%S")))

    util.batch_logging_update(s.all_jobid, "e")
