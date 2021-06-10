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

    def submit_ref_epc_certificates_gluejob(self):
        try:
            jobName = self.dir["glue_epc_job_name"]
            s3_bucket = self.dir["s3_bucket"]
            environment = self.env
            util.batch_logging_insert(
                self.certificates_ref_jobid, 59, "epc_certificates_ref_glue_job", "start_epc_full_jobs.py"
            )
            obj = glue.ProcessGlueJob(
                job_name=jobName, s3_bucket=s3_bucket, environment=environment, processJob="epc_cert_all"
            )
            job_response = obj.run_glue_job()
            if job_response:
                util.batch_logging_update(self.certificates_ref_jobid, "e")
                print(
                    "{0}: Ref Glue Job Completed successfully for {1}".format(
                        datetime.now().strftime("%H:%M:%S"), self.process_epc_cert_name
                    )
                )
                # return staging_job_response
            else:
                print(
                    "{0}: Error occurred in {1} Job".format(
                        datetime.now().strftime("%H:%M:%S"), self.process_epc_cert_name
                    )
                )
                # return staging_job_response
                raise Exception
        except Exception as e:
            util.batch_logging_update(self.certificates_ref_jobid, "f", str(e))
            util.batch_logging_update(self.all_jobid, "f", str(e))
            print("Error in Ref Glue Job :- " + str(e))
            sys.exit(1)


if __name__ == "__main__":

    s = StartEPCJobs()

    util.batch_logging_insert(s.all_jobid, 108, "all_epc_jobs", "start_epc_full_jobs.py")

    # run EPC Certificates glue job
    print("{0}: Glue Job running for {1}...".format(datetime.now().strftime("%H:%M:%S"), s.process_epc_cert_name))
    s.submit_ref_epc_certificates_gluejob()

    print("{0}: All D18 completed successfully".format(datetime.now().strftime("%H:%M:%S")))

    util.batch_logging_update(s.all_jobid, "e")
