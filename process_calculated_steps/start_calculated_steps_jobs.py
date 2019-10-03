import sys
from datetime import datetime

sys.path.append('..')

from process_eac_aq import start_eac_aq_pa_jobs as eacaqpa
from process_eac_aq import start_igloo_ind_eac_aq_jobs as iglindeacaq
from process_eac_aq import start_consumption_accuracy_jobs as ca
from process_tado import start_tado_efficiency_jobs as ta
from process_EstimatedAdvance import start_est_advance_job as est_adv
from process_aurora import start_daily_sales_jobs as ds

from common import utils as util


class CalcSteps:
    def __init__(self):
        self.process_name = "calculated steps"
        self.pythonAlias = util.get_pythonAlias()
        self.env = util.get_env()
        self.dir = util.get_dir()

        self.all_jobid = util.get_jobID()


if __name__ == '__main__':

    s = CalcSteps()

    util.batch_logging_insert(s.all_jobid, 100, 'all_calc_steps', 'start_calculated_steps_jobs.py')
    print("{0}: Starting {1}".format(datetime.now().strftime('%H:%M:%S'), s.process_name))

    print("{0}: EAC and AQ Glue Job running...".format(datetime.now().strftime('%H:%M:%S')))
    eacaqpa_obj = eacaqpa.EacAqPa()
    eacaqpa_obj.submit_eac_aq_gluejob()

    # run eac and aq v1 calculation job
    print("{0}: EAC and AQ Glue V1 Job running...".format(datetime.now().strftime('%H:%M:%S')))
    iglindeacaq_obj = iglindeacaq.IglIndEacAq()
    iglindeacaq_obj.submit_eac_aq_gluejob()

    # run consumption accuracy job
    print("{0}: Consumption Accuracy Job running...".format(datetime.now().strftime('%H:%M:%S')))
    ca_obj = ca.ConsumptionAccuracy()
    ca_obj.submit_consumption_accuracy_gluejob()

    # run TADO efficiency job
    print("{0}: TADO Efficiency Job running...".format(datetime.now().strftime('%H:%M:%S')))
    ta_obj = ta.TADOEfficiencyJobs()
    ta_obj.submit_tado_efficiency_batch_gluejob()

    # run daily sales job
    print("{0}: Daily Sales Job running...".format(datetime.now().strftime('%H:%M:%S')))
    ds_obj = ds.DailySalesJobs()
    ds_obj.submit_daily_sales_batch_gluejob()

    # run Estimated Advance Job
    print("{0}: Estimated Advance Job running...".format(datetime.now().strftime('%H:%M:%S')))
    est_adv_obj = est_adv.EstimatedAdvance()
    est_adv_obj.submit_estimated_advance_gluejob()

    print("{0}: All {1} completed successfully".format(datetime.now().strftime('%H:%M:%S'), s.process_name))

    util.batch_logging_update(s.all_jobid, 'e')

