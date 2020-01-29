import sys
from datetime import datetime
import timeit
sys.path.append('..')

from process_Ensek import processAllEnsekPAScripts as ae
from common import process_glue_job as glue

from common import utils as util
from common import Sync_files as ensek_sync


class StartEnsekFlowJobs:

    def __init__(self):
        self.env = util.get_env()
        self.dir = util.get_dir()
        self.all_jobid = util.get_jobID()
        self.staging_jobid = util.get_jobID()
        self.mirror_jobid = util.get_jobID()
        self.customerdb_jobid = util.get_jobID()
        self.ref_jobid = util.get_jobID()
        self.ref_eac_aq_jobid = util.get_jobID()
        self.process_name = "Ensek Flows Extract and Process "


    def submit_process_ensek_flows(self, source_input, destination_input, _IAM):
        """
        Calls the utils/Sync_files.py script which mirrors ensek data
        :return: None
        """

        print("{0}: >>>> Process {1}<<<<".format(datetime.now().strftime('%H:%M:%S'), self.process_name))
        try:
            util.batch_logging_insert(self.mirror_jobid, 130, 'process_ensek_flows-' + source_input + '-' + self.env,
                                      'start_ensek_api_pa_jobs.py')
            start = timeit.default_timer()
            r = ensek_sync.SyncS3(source_input, destination_input, _IAM)
            r.process_sync()

            util.batch_logging_update(self.mirror_jobid, 'e')
            print( "process_ensek_flows-" + source_input + "-" + self.env + " files completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'), float(timeit.default_timer() - start)))
        except Exception as e:
            util.batch_logging_update(self.mirror_jobid, 'f', str(e))
            util.batch_logging_update(self.all_jobid, 'f', str(e))
            print("Error in process :- " + str(e))
            sys.exit(1)


if __name__ == '__main__':
    s = StartEnsekFlowJobs()

    util.batch_logging_insert(s.all_jobid, 130, 'all_pa_jobs', 'start_ensek_api_flows_jobs.py')

    print("Ensek Meterpoints Mirror  job is running...".format(datetime.now().strftime('%H:%M:%S'), s.process_name))
    source_input = "s3://igloo-data-warehouse-uat/process_ensek_flows/"
    destination_input = "s3://igloo-data-warehouse-" + s.env + "/stage1Flows/"
    IAM = 'inbound'
    s.submit_process_ensek_flows(source_input, destination_input, IAM)
