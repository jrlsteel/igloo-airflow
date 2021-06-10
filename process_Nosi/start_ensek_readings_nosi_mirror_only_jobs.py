import sys
from datetime import datetime
import timeit
import subprocess

sys.path.append("..")
from common import process_glue_job as glue
from common import utils as util
from common import Refresh_UAT as refresh
from conf import config as con


class StartReadingsNOSIJobs:
    def __init__(self):
        self.pythonAlias = util.get_pythonAlias()
        self.env = util.get_env()
        self.dir = util.get_dir()
        self.all_jobid = util.get_jobID()
        self.nosi_jobid = util.get_jobID()
        self.nosi_download_jobid = util.get_jobID()
        self.nosi_staging_jobid = util.get_jobID()
        self.nosi_ref_jobid = util.get_jobID()
        self.process_name = "Nosi"
        self.mirror_jobid = util.get_jobID()

    def submit_process_s3_mirror_job(self, source_input, destination_input):
        """
        Calls the utils/Refresh_UAT.py script which mirrors s3 data from source to destination fdlder
        :return: None
        """

        print("{0}: >>>> Process {1}<<<<".format(datetime.now().strftime("%H:%M:%S"), self.process_name))
        try:
            util.batch_logging_insert(
                self.mirror_jobid,
                43,
                "nosi_extract_mirror-" + source_input + "-" + self.env,
                "start_ensek_readings_nosi_jobs.py",
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
                "nosi_extract_mirror--"
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

    def submit_download_nosi_job(self):

        pythonAlias = util.get_pythonAlias()

        print("{0}: >>>> Downloading NOSI files <<<<".format(datetime.now().strftime("%H:%M:%S")))
        try:
            util.batch_logging_insert(
                self.nosi_download_jobid, 43, "nosi_download_pyscript", "start_ensek_readings_nosi_jobs.py"
            )
            start = timeit.default_timer()
            subprocess.run([pythonAlias, "download_nosi.py"], check=True)
            util.batch_logging_update(self.nosi_download_jobid, "e")
            print(
                "{0}: download_NOSI completed in {1:.2f} seconds".format(
                    datetime.now().strftime("%H:%M:%S"), float(timeit.default_timer() - start)
                )
            )
        except Exception as e:
            util.batch_logging_update(self.nosi_download_jobid, "f", str(e))
            util.batch_logging_update(self.all_jobid, "f", str(e))
            print("Error in download_NOSI process :- " + str(e))
            sys.exit(1)


if __name__ == "__main__":

    s = StartReadingsNOSIJobs()

    util.batch_logging_insert(s.all_jobid, 132, "all_readings_internal_nosi_jobs", "start_ensek_readings_nosi_jobs.py")

    s3_destination_bucket = s.dir["s3_bucket"]
    s3_source_bucket = s.dir["s3_source_bucket"]

    if s.env == "newprod":
        # # # Ensek Internal Readings NOSI Downloading
        print("{0}: download_NOSI job is running...".format(datetime.now().strftime("%H:%M:%S")))
        s.submit_download_nosi_job()
    else:
        # # run processing mirror job
        print("Nosi Mirror job is running...".format(datetime.now().strftime("%H:%M:%S"), s.process_name))
        source_input = "s3://" + s3_source_bucket + "/stage1/ReadingsNOSIGas/"
        destination_input = "s3://" + s3_destination_bucket + "/stage1/ReadingsNOSIGas/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

    print("{0}: All Ensek Internal Readings NOSI completed successfully".format(datetime.now().strftime("%H:%M:%S")))

    util.batch_logging_update(s.all_jobid, "e")
