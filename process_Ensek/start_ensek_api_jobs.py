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
            print("ensek_extract_mirror-" + source_input + " files completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'),float(timeit.default_timer() - start)))
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

    def submit_ensek_staging_Gluejob(self):
        try:
            jobName = self.dir['glue_staging_job_name']
            s3_bucket = self.dir['s3_bucket']
            environment = self.env

            util.batch_logging_insert(self.staging_jobid, 13, 'ensek_non_pa_staging_gluejob', 'start_ensek_api_jobs.py')
            obj_stage = glue.ProcessGlueJob(job_name=jobName, s3_bucket=s3_bucket, environment=environment, processJob='ensek-non-pa')
            job_response = obj_stage.run_glue_job()
            if job_response:
                util.batch_logging_update(self.staging_jobid, 'e')
                print("{0}: Staging Job Completed successfully".format(datetime.now().strftime('%H:%M:%S')))
                # return staging_job_response
            else:
                print("Error occurred in Staging Job")
                # return staging_job_response
                raise Exception
        except Exception as e:
            util.batch_logging_update(self.staging_jobid, 'f', str(e))
            util.batch_logging_update(self.all_jobid, 'f', str(e))
            print("Error in Staging Job :- " + str(e))
            sys.exit(1)

    def submit_Ensek_Gluejob(self):
        try:
            jobname = self.dir['glue_ensek_job_name']
            s3_bucket = self.dir['s3_bucket']
            environment = self.env
            util.batch_logging_insert(self.ref_jobid, 14, 'ensek_non_pa_ref_gluejob','start_ensek_api_jobs.py')
            obj_ensek = glue.ProcessGlueJob(job_name=jobname, s3_bucket=s3_bucket, environment=environment,
                                            processJob='ensek')
            job_response = obj_ensek.run_glue_job()
            if job_response:
                util.batch_logging_update(self.ref_jobid, 'e')
                print("{0}: Ensek Glue Job completed successfully".format(datetime.now().strftime('%H:%M:%S')))
                # return staging_job_response
            else:
                print("Error occurred in Ensek Glue Job")
                # return staging_job_response
                raise Exception
        except Exception as e:
            util.batch_logging_update(self.ref_jobid, 'f', str(e))
            util.batch_logging_update(self.all_jobid, 'f', str(e))
            print("Error in Ensek Job :- " + str(e))
            sys.exit(1)


if __name__ == '__main__':

    s = StartEnsekJobs()

    util.batch_logging_insert(s.all_jobid, 101, 'all_non_pa_jobs', 'start_ensek_api_jobs.py')

    if s.env == 'prod':
        # run all ensek scripts
        print("{0}: Ensek Scripts running...".format(datetime.now().strftime('%H:%M:%S')))
        s.submit_all_ensek_scripts()
    else:
        print("Ensek Account Status  Mirror  job is running...".format(datetime.now().strftime('%H:%M:%S'), s.process_name))
        source_input = "s3://igloo-data-warehouse-prod/stage1/AccountStatus/"
        destination_input = "s3://igloo-data-warehouse-uat/stage1/AccountStatus/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        print("Ensek Registration Meterpoint Status Elec mirror  job is running...".format(datetime.now().strftime('%H:%M:%S'),s.process_name))
        source_input = "s3://igloo-data-warehouse-prod/stage1/RegistrationsElecMeterpoint/"
        destination_input = "s3://igloo-data-warehouse-uat/stage1/RegistrationsElecMeterpoint/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        print("Ensek Registration Status  Gas Mirror  job is running...".format(datetime.now().strftime('%H:%M:%S'), s.process_name))
        source_input = "s3://igloo-data-warehouse-prod/stage1/RegistrationsGasMeterpoint/"
        destination_input = "s3://igloo-data-warehouse-uat/stage1/RegistrationsGasMeterpoint/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        print("Ensek Internal Estimates Elec  mirror  job is running...".format(datetime.now().strftime('%H:%M:%S'),s.process_name))
        source_input = "s3://igloo-data-warehouse-prod/stage1/EstimatesElecInternal/"
        destination_input = "s3://igloo-data-warehouse-uat/stage1/EstimatesElecInternal/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        print("Ensek Internal Estimates Gas  mirror  job is running...".format(datetime.now().strftime('%H:%M:%S'),s.process_name))
        source_input = "s3://igloo-data-warehouse-prod/stage1/EstimatesGasInternal/"
        destination_input = "s3://igloo-data-warehouse-uat/stage1/EstimatesGasInternal/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        print("Ensek Tariff History Mirror  job is running...".format(datetime.now().strftime('%H:%M:%S'), s.process_name))
        source_input = "s3://igloo-data-warehouse-prod/stage1/TariffHistory/"
        destination_input = "s3://igloo-data-warehouse-uat/stage1/TariffHistory/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        print("Ensek Tariff History Elec Stand Charge Mirror  job is running...".format(datetime.now().strftime('%H:%M:%S'),s.process_name))
        source_input = "s3://igloo-data-warehouse-prod/stage1/TariffHistoryElecStandCharge/"
        destination_input = "s3://igloo-data-warehouse-uat/stage1/TariffHistoryElecStandCharge/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        print("Ensek Tariff History Gas Stand Charge Mirror  job is running...".format( datetime.now().strftime('%H:%M:%S'), s.process_name))
        source_input = "s3://igloo-data-warehouse-prod/stage1/TariffHistoryGasStandCharge/"
        destination_input = "s3://igloo-data-warehouse-uat/stage1/TariffHistoryGasStandCharge/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        print("Ensek Tariff History Elec Unit Rates Mirror  job is running...".format(datetime.now().strftime('%H:%M:%S'), s.process_name))
        source_input = "s3://igloo-data-warehouse-prod/stage1/TariffHistoryElecUnitRates/"
        destination_input = "s3://igloo-data-warehouse-uat/stage1/TariffHistoryElecUnitRates/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        print("Ensek Tariff History Gas Unit Rates Mirror  job is running...".format(datetime.now().strftime('%H:%M:%S'), s.process_name))
        source_input = "s3://igloo-data-warehouse-prod/stage1/TariffHistoryGasUnitRates/"
        destination_input = "s3://igloo-data-warehouse-uat/stage1/TariffHistoryGasUnitRates/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        print("Ensek Account Transactions mirror  job is running...".format(datetime.now().strftime('%H:%M:%S'),s.process_name))
        source_input = "s3://igloo-data-warehouse-prod/stage1/AccountTransactions/"
        destination_input = "s3://igloo-data-warehouse-uat/stage1/AccountTransactions/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

    #run staging glue job
    print("{0}: Ensek Staging Job running...".format(datetime.now().strftime('%H:%M:%S')))
    s.submit_ensek_staging_Gluejob()

    #run reference jobs
    print("{0}: Ensek Reference Jobs running...".format(datetime.now().strftime('%H:%M:%S')))
    s.submit_Ensek_Gluejob()

    print("{0}: All jobs completed successfully".format(datetime.now().strftime('%H:%M:%S')))

    util.batch_logging_update(s.all_jobid, 'e')
