import sys
from datetime import datetime
import timeit
import subprocess

from cdw.common import process_glue_job as glue
from cdw.common import utils as util


class ConsumptionAccuracy:
    def __init__(self):
        self.process_name = "Consumption Accuracy"
        self.pythonAlias = util.get_pythonAlias()
        self.env = util.get_env()
        self.dir = util.get_dir()

        self.all_jobid = util.get_jobID()
        self.cons_accu_ref_jobid = util.get_jobID()

    def submit_consumption_accuracy_gluejob(self):
        try:
            util.batch_logging_insert(
                self.cons_accu_ref_jobid, 42, "eac_cons_accu_glue_job", "start_consumption_accuracy_jobs.py"
            )

            jobName = self.dir["glue_cons_accu_job_name"]
            s3_bucket = self.dir["s3_bucket"]
            environment = self.env

            obj_eac_aq = glue.ProcessGlueJob(
                job_name=jobName, s3_bucket=s3_bucket, environment=environment, processJob="cons_accu"
            )
            eac_aq_job_response = obj_eac_aq.run_glue_job()
            if eac_aq_job_response:
                util.batch_logging_update(self.cons_accu_ref_jobid, "e")
                print("{0}: {1} Completed successfully".format(datetime.now().strftime("%H:%M:%S"), self.process_name))
            else:
                print("Error occurred in {1} Job")
                raise Exception
        except Exception as e:
            util.batch_logging_update(self.cons_accu_ref_jobid, "f", str(e))
            print("Error in {1} Job : " + str(e))
            sys.exit(1)


if __name__ == "__main__":

    s = ConsumptionAccuracy()

    util.batch_logging_insert(s.all_jobid, 105, "consumption_accuracy", "start_igloo_ind_eac_aq_jobs.py")

    # run consumption accuracy job
    print("{0}: Consumption Accuracy Job running...".format(datetime.now().strftime("%H:%M:%S")))
    ca_obj = ConsumptionAccuracy()
    ca_obj.submit_consumption_accuracy_gluejob()

    util.batch_logging_update(s.all_jobid, "e")
