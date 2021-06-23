import sys
from datetime import datetime
import timeit
import subprocess

sys.path.append("..")
from common import process_glue_job as glue
from common import utils as util
from common import Refresh_UAT as refresh


class StartEPCJobs:
    def __init__(self):
        self.process_epc_full_name = "EPC Full Download"
        self.process_epc_cert_name = "EPC Certificates"
        self.process_epc_reco_name = "EPC Recommendations"
        self.process_epc_mirror_name = "EPC Full Recommends Certs Mirror"
        self.pythonAlias = util.get_pythonAlias()
        self.env = util.get_env()
        self.dir = util.get_dir()
        self.all_jobid = util.get_jobID()
        self.certificates_jobid = util.get_jobID()
        self.recommendations_jobid = util.get_jobID()
        self.certificates_staging_jobid = util.get_jobID()
        self.recommendations_staging_jobid = util.get_jobID()
        self.certificates_ref_jobid = util.get_jobID()
        self.mirror_jobid = util.get_jobID()

    def submit_process_epc_full_job(self):
        """
        Calls the epc process_epc_certificates.py script
        :return: None
        """

        print("{0}: >>>> Process EPC Certificates Data <<<<".format(datetime.now().strftime("%H:%M:%S")))
        try:
            util.batch_logging_insert(
                self.certificates_jobid, 56, "epc_full_extract_pyscript", "start_epc_full_jobs.py"
            )
            start = timeit.default_timer()
            subprocess.run([self.pythonAlias, "process_download_epc_data.py"], check=True)
            util.batch_logging_update(self.certificates_jobid, "e")
            print(
                "{0}: Process EPC Certificates files completed in {1:.2f} seconds".format(
                    datetime.now().strftime("%H:%M:%S"), float(timeit.default_timer() - start)
                )
            )
        except Exception as e:
            util.batch_logging_update(self.certificates_jobid, "f", str(e))
            util.batch_logging_update(self.all_jobid, "f", str(e))
            print("Error in EPC Certificates process :- " + str(e))
            sys.exit(1)


if __name__ == "__main__":

    s = StartEPCJobs()

    util.batch_logging_insert(s.all_jobid, 108, "all_epc_jobs", "start_epc_full_jobs.py")

    # run processing download epc data script
    print("{0}: {1} job is running...".format(datetime.now().strftime("%H:%M:%S"), s.process_epc_full_name))
    s.submit_process_epc_full_job()

    util.batch_logging_update(s.all_jobid, "e")
