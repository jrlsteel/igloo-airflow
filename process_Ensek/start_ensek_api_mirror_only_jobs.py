import sys
from datetime import datetime
import timeit

sys.path.append('..')

from process_Ensek import processAllEnsekScripts as ae
from common import process_glue_job as glue

from common import utils as util
from common import Refresh_UAT as refresh


class StartEnsekJobs:

    def __init__(self):
        self.env = util.get_env()
        self.dir = util.get_dir()
        self.all_jobid = util.get_jobID()
        self.mirror_jobid = util.get_jobID()
        self.staging_jobid = util.get_jobID()
        self.ref_jobid = util.get_jobID()
        self.process_name = "Ensek Non PA Extract and Process "


    def submit_process_s3_mirror_job(self, source_input, destination_input):
        """
        Calls the utils/Refresh_UAT.py script which mirrors s3 data from source to destination fdlder
        :return: None
        """

        print("{0}: >>>> Process {1}<<<<".format(datetime.now().strftime('%H:%M:%S'), self.process_name))
        try:
            util.batch_logging_insert(self.mirror_jobid, 8, 'ensek_extract_mirror-' + source_input + '-' + self.env,
                                      'start_ensek_api_jobs.py')
            start = timeit.default_timer()
            r = refresh.SyncS3(source_input, destination_input)
            r.process_sync()

            util.batch_logging_update(self.mirror_jobid, 'e')
            print("ensek_extract_mirror-" + source_input + "-" + self.env +" files completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'),float(timeit.default_timer() - start)))
        except Exception as e:
            util.batch_logging_update(self.mirror_jobid, 'f', str(e))
            util.batch_logging_update(self.all_jobid, 'f', str(e))
            print("Error in process :- " + str(e))
            sys.exit(1)



    def submit_all_ensek_scripts(self):
        try:
            all_ensek_scripts_response = ae.process_all_ensek_scripts()
            if all_ensek_scripts_response:
                print("{0}: All Ensek Scripts job completed successfully".format(datetime.now().strftime('%H:%M:%S')))
                # return all_ensek_scripts_response
            else:
                print("Error occurred in All Ensek Scripts job")
                # return all_ensek_scripts_response
                raise Exception
        except Exception as e:
            util.batch_logging_update(self.all_jobid, 'f', str(e))
            print("Error in Ensek Scripts :- " + str(e))
            sys.exit(1)

if __name__ == '__main__':

    s = StartEnsekJobs()

    util.batch_logging_insert(s.all_jobid, 101, 'all_non_pa_jobs', 'start_ensek_api_jobs.py')

    if s.env == 'prod':
        # run all ensek scripts
        print("{0}: Ensek Scripts running...".format(datetime.now().strftime('%H:%M:%S')))
        s.submit_all_ensek_scripts()

    elif s.env in ['preprod', 'uat']:

        # run NonPA Ensek Jobs in UAT PreProd Limit of 100 accounts
        print("{0}: Ensek Scripts running...".format(datetime.now().strftime('%H:%M:%S')))
        #This needs to be uncommented when we have access to internal apis at ensek
        #s.submit_all_ensek_scripts()

        print("Ensek Account Status  Mirror  job is running...".format(datetime.now().strftime('%H:%M:%S'), s.process_name))
        source_input = "s3://igloo-data-warehouse-preprod/stage1/AccountStatus/"
        destination_input = "s3://igloo-data-warehouse-" + s.env + "/stage1/AccountStatus/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        print("Ensek Registration Meterpoint Status Elec mirror  job is running...".format(datetime.now().strftime('%H:%M:%S'),s.process_name))
        source_input = "s3://igloo-data-warehouse-preprod/stage1/RegistrationsElecMeterpoint/"
        destination_input = "s3://igloo-data-warehouse-" + s.env + "/stage1/RegistrationsElecMeterpoint/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        print("Ensek Registration Status  Gas Mirror  job is running...".format(datetime.now().strftime('%H:%M:%S'), s.process_name))
        source_input = "s3://igloo-data-warehouse-preprod/stage1/RegistrationsGasMeterpoint/"
        destination_input = "s3://igloo-data-warehouse-" + s.env + "/stage1/RegistrationsGasMeterpoint/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        print("Ensek Internal Estimates Elec  mirror  job is running...".format(datetime.now().strftime('%H:%M:%S'),s.process_name))
        source_input = "s3://igloo-data-warehouse-preprod/stage1/EstimatesElecInternal/"
        destination_input = "s3://igloo-data-warehouse-" + s.env + "/stage1/EstimatesElecInternal/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        print("Ensek Internal Estimates Gas  mirror  job is running...".format(datetime.now().strftime('%H:%M:%S'),s.process_name))
        source_input = "s3://igloo-data-warehouse-preprod/stage1/EstimatesGasInternal/"
        destination_input = "s3://igloo-data-warehouse-" + s.env + "/stage1/EstimatesGasInternal/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        print("Ensek Tariff History Mirror  job is running...".format(datetime.now().strftime('%H:%M:%S'), s.process_name))
        source_input = "s3://igloo-data-warehouse-preprod/stage1/TariffHistory/"
        destination_input = "s3://igloo-data-warehouse-" + s.env + "/stage1/TariffHistory/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        print("Ensek Tariff History Elec Stand Charge Mirror  job is running...".format(datetime.now().strftime('%H:%M:%S'),s.process_name))
        source_input = "s3://igloo-data-warehouse-preprod/stage1/TariffHistoryElecStandCharge/"
        destination_input = "s3://igloo-data-warehouse-" + s.env + "/stage1/TariffHistoryElecStandCharge/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        print("Ensek Tariff History Gas Stand Charge Mirror  job is running...".format( datetime.now().strftime('%H:%M:%S'), s.process_name))
        source_input = "s3://igloo-data-warehouse-preprod/stage1/TariffHistoryGasStandCharge/"
        destination_input = "s3://igloo-data-warehouse-" + s.env + "/stage1/TariffHistoryGasStandCharge/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        print("Ensek Tariff History Elec Unit Rates Mirror  job is running...".format(datetime.now().strftime('%H:%M:%S'), s.process_name))
        source_input = "s3://igloo-data-warehouse-preprod/stage1/TariffHistoryElecUnitRates/"
        destination_input = "s3://igloo-data-warehouse-" + s.env + "/stage1/TariffHistoryElecUnitRates/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        print("Ensek Tariff History Gas Unit Rates Mirror  job is running...".format(datetime.now().strftime('%H:%M:%S'), s.process_name))
        source_input = "s3://igloo-data-warehouse-preprod/stage1/TariffHistoryGasUnitRates/"
        destination_input = "s3://igloo-data-warehouse-" + s.env + "/stage1/TariffHistoryGasUnitRates/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        print("Ensek Account Transactions mirror  job is running...".format(datetime.now().strftime('%H:%M:%S'),s.process_name))
        source_input = "s3://igloo-data-warehouse-preprod/stage1/AccountTransactions/"
        destination_input = "s3://igloo-data-warehouse-" + s.env + "/stage1/AccountTransactions/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        print("Ensek Account Settings mirror  job is running...".format(datetime.now().strftime('%H:%M:%S'),s.process_name))
        source_input = "s3://igloo-data-warehouse-preprod/stage1/AccountSettings/"
        destination_input = "s3://igloo-data-warehouse-" + s.env + "/stage1/AccountSettings/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

    util.batch_logging_update(s.all_jobid, 'e')
