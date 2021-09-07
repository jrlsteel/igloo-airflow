import sys
from datetime import datetime
import timeit
import subprocess


from cdw.common import process_glue_job as glue
from cdw.common import utils as util
from cdw.common import Refresh_UAT as refresh
from process_calculated_steps import start_calculated_steps_jobs as sj


class ALP:
    def __init__(self):
        self.process_name = "ALP WCF CV Historical EAC_AQ CONS_ACCU "
        self.pythonAlias = util.get_pythonAlias()
        self.env = util.get_env()
        self.dir = util.get_dir()

        self.all_jobid = util.get_jobID()

        self.alp_wcf_jobid = util.get_jobID()
        self.alp_cv_jobid = util.get_jobID()
        self.alp_wcf_staging_jobid = util.get_jobID()
        self.alp_cv_staging_jobid = util.get_jobID()
        self.alp_ref_jobid = util.get_jobID()


if __name__ == "__main__":

    s = ALP()

    util.batch_logging_insert(s.all_jobid, 105, "all_alp_jobs", "start_alp_jobs.py")

    # run consumption accuracy job
    print("{0}: All Calcs..".format(datetime.now().strftime("%H:%M:%S")))
    sj_obj = sj.CalcSteps()
    sj_obj.startCalcJobs()

    util.batch_logging_update(s.all_jobid, "e")
