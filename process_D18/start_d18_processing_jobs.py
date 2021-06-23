import sys
from datetime import datetime
import timeit
import subprocess

sys.path.append("..")
from common import process_glue_job as glue
from common import utils as util
from common import Refresh_UAT as refresh


class StartD18Jobs:
    def __init__(self):
        self.pythonAlias = util.get_pythonAlias()
        self.env = util.get_env()
        self.dir = util.get_dir()
        self.all_jobid = util.get_jobID()
        self.d18_jobid = util.get_jobID()
        self.d18_download_jobid = util.get_jobID()
        self.d18_staging_jobid = util.get_jobID()
        self.d18_ref_jobid = util.get_jobID()
        self.process_name = "D18 Download Extract and Process "

    def submit_process_d18_job(self):
        """
        Calls the d18 process_d18.py script to which processes the downloaded data from s3 and extracts the BPP and PPC co efficients.
        :return: None
        """

        print("{0}: >>>> Process D18 files <<<<".format(datetime.now().strftime("%H:%M:%S")))
        try:
            util.batch_logging_insert(self.d18_jobid, 28, "d18_extract_pyscript", "start_d18_jobs.py")
            start = timeit.default_timer()
            subprocess.run([self.pythonAlias, "process_d18.py"], check=True)
            util.batch_logging_update(self.d18_jobid, "e")
            print(
                "{0}: Process D18 files completed in {1:.2f} seconds".format(
                    datetime.now().strftime("%H:%M:%S"), float(timeit.default_timer() - start)
                )
            )
        except Exception as e:
            util.batch_logging_update(self.d18_jobid, "f", str(e))
            util.batch_logging_update(self.all_jobid, "f", str(e))
            print("Error in download_d18 process :- " + str(e))
            sys.exit(1)


if __name__ == "__main__":

    s = StartD18Jobs()

    util.batch_logging_insert(s.all_jobid, 102, "all_d18_jobs", "start_d18_jobs.py")

    # run processing d18 python script
    print("{0}: process_d18 job is running...".format(datetime.now().strftime("%H:%M:%S")))
    s.submit_process_d18_job()

    print("{0}: All D18 completed successfully".format(datetime.now().strftime("%H:%M:%S")))
    util.batch_logging_update(s.all_jobid, "e")
