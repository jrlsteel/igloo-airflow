import sys
from datetime import datetime
import timeit
sys.path.append('..')

from process_Ensek import processAllEnsekPAScripts as ae
from common import process_glue_job as glue

from common import utils as util
from common import Refresh_UAT as refresh
from conf import config as con

class StartEnsekPAJobs:

    def __init__(self):
        self.env = util.get_env()
        self.dir = util.get_dir()
        self.all_jobid = util.get_jobID()
        self.staging_jobid = util.get_jobID()
        self.mirror_jobid = util.get_jobID()
        self.customerdb_jobid = util.get_jobID()
        self.ref_jobid = util.get_jobID()
        self.ref_eac_aq_jobid = util.get_jobID()
        self.process_name = "Ensek PA Extract and Process "

    def submit_process_s3_mirror_job(self, source_input, destination_input):
        """
        Calls the utils/Refresh_UAT.py script which mirrors s3 data from source to destination fdlder
        :return: None
        """

        print("{0}: >>>> Process {1}<<<<".format(datetime.now().strftime('%H:%M:%S'), self.process_name))
        try:
            util.batch_logging_insert(self.mirror_jobid, 8, 'ensek_extract_mirror-' + source_input + '-' + self.env,
                                      'start_ensek_api_pa_jobs.py')
            start = timeit.default_timer()
            r = refresh.SyncS3(source_input, destination_input)
            r.process_sync(env={
                'AWS_ACCESS_KEY_ID': con.s3_config['access_key'],
                'AWS_SECRET_ACCESS_KEY': con.s3_config['secret_key']
            })

            util.batch_logging_update(self.mirror_jobid, 'e')
            print( "ensek_extract_mirror-" + source_input + "-" + self.env + " files completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'), float(timeit.default_timer() - start)))
        except Exception as e:
            util.batch_logging_update(self.mirror_jobid, 'f', str(e))
            util.batch_logging_update(self.all_jobid, 'f', str(e))
            print("Error in process :- " + str(e))
            sys.exit(1)

    def submit_all_ensek_pa_scripts(self):
        try:
            all_ensek_pa_scripts_response = ae.process_all_ensek_pa_scripts()
            if all_ensek_pa_scripts_response:
                print("{0}: All Ensek Scripts job completed successfully".format(datetime.now().strftime('%H:%M:%S')))
                # return all_ensek_scripts_response
            else:
                print("Error occurred in All Ensek Scripts job")
                # return all_ensek_scripts_response
                raise Exception
        except Exception as e:
            print("Error in Ensek Scripts :- " + str(e))
            util.batch_logging_update(self.all_jobid, 'f', str(e))
            sys.exit(1)



if __name__ == '__main__':
    s = StartEnsekPAJobs()

    util.batch_logging_insert(s.all_jobid, 103, 'all_pa_jobs', 'start_ensek_api_pa_jobs.py')

    if s.env == 'prod':
        # run PA Ensek Jobs
        print("{0}:  PA Ensek Jobs running...".format(datetime.now().strftime('%H:%M:%S')))
        s.submit_all_ensek_pa_scripts()

    elif s.env in ['preprod', 'newprod', 'uat']:
        s3_destination_bucket = s.dir['s3_bucket']
        s3_source_bucket = s.dir['s3_source_bucket']

        # run PA Ensek Jobs in UAT PreProd Limit of 100 accounts
        print("{0}:  PA Ensek Jobs running...".format(datetime.now().strftime('%H:%M:%S')))
        #s.submit_all_ensek_pa_scripts()

        print("Ensek Meterpoints Mirror  job is running...".format(datetime.now().strftime('%H:%M:%S'), s.process_name))
        source_input = "s3://" + s3_source_bucket + "/stage1/MeterPoints/"
        destination_input = "s3://" + s3_destination_bucket + "/stage1/MeterPoints/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        print("Ensek Meterpoints Attributes mirror  job is running...".format(datetime.now().strftime('%H:%M:%S'), s.process_name))
        source_input = "s3://" + s3_source_bucket + "/stage1/MeterPointsAttributes/"
        destination_input = "s3://" + s3_destination_bucket + "/stage1/MeterPointsAttributes/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        print("Ensek Meters Mirror  job is running...".format(datetime.now().strftime('%H:%M:%S'), s.process_name))
        source_input = "s3://" + s3_source_bucket + "/stage1/Meters/"
        destination_input = "s3://" + s3_destination_bucket + "/stage1/Meters/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        source_input = "s3://" + s3_source_bucket + "/stage1/MetersAttributes/"
        destination_input = "s3://" + s3_destination_bucket + "/stage1/MetersAttributes/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        print("Ensek Registers Mirror  job is running...".format(datetime.now().strftime('%H:%M:%S'), s.process_name))
        source_input = "s3://" + s3_source_bucket + "/stage1/Registers/"
        destination_input = "s3://" + s3_destination_bucket + "/stage1/Registers/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        print("Ensek Registers Attributes mirror  job is running...".format(datetime.now().strftime('%H:%M:%S'),
                                                                         s.process_name))
        source_input = "s3://" + s3_source_bucket + "/stage1/RegistersAttributes/"
        destination_input = "s3://" + s3_destination_bucket + "/stage1/RegistersAttributes/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        source_input = "s3://" + s3_source_bucket + "/stage1/ReadingsInternal/"
        destination_input = "s3://" + s3_destination_bucket + "/stage1/ReadingsInternal/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

    print("{0}: All jobs completed successfully".format(datetime.now().strftime('%H:%M:%S')))

    util.batch_logging_update(s.all_jobid, 'e')
