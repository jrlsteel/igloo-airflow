import sys
from datetime import datetime
import timeit

sys.path.append('..')



from common import utils as util
from common import Refresh_UAT as refresh


class StartMirrorJobs:

    def __init__(self):
        self.env = util.get_env()
        self.dir = util.get_dir()
        self.all_jobid = util.get_jobID()
        self.mirror_jobid = util.get_jobID()
        self.process_name = "Mirror S3 Folder"


    def submit_process_s3_mirror_job(self, source_input, destination_input):
        """
        Calls the utils/Refresh_UAT.py script which mirrors s3 data from source to destination fdlder
        :return: None
        """

        print("{0}: >>>> Process {1}<<<<".format(datetime.now().strftime('%H:%M:%S'), self.process_name))
        try:
            util.batch_logging_insert(self.mirror_jobid, 60, 's3_mirror-' + source_input + '-' + self.env,
                                      'start_mirror_api_jobs.py')
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



if __name__ == '__main__':

    s = StartMirrorJobs()

    util.batch_logging_insert(s.all_jobid, 60, 'all_mirror_jobs', 'start_mirror_api_jobs.py')

    if s.env == 'prod':
        # run all ensek scripts
        print("{0}: Running in Prod NOT PERMITTED...".format(datetime.now().strftime('%H:%M:%S')))
    else:
        print("Ensek Meterpoints Mirror  job is running...".format(datetime.now().strftime('%H:%M:%S'), s.process_name))
        source_input = "s3://igloo-data-warehouse-prod/stage1/MeterPoints/"
        destination_input = "s3://igloo-data-warehouse-" + s.env + "/stage1/MeterPoints/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        print("Ensek Meterpoints Attributes mirror  job is running...".format(datetime.now().strftime('%H:%M:%S'),
                                                                              s.process_name))
        source_input = "s3://igloo-data-warehouse-prod/stage1/MeterPointsAttributes/"
        destination_input = "s3://igloo-data-warehouse-" + s.env + "/stage1/MeterPointsAttributes/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        print("Ensek Meters Mirror  job is running...".format(datetime.now().strftime('%H:%M:%S'), s.process_name))
        source_input = "s3://igloo-data-warehouse-prod/stage1/Meters/"
        destination_input = "s3://igloo-data-warehouse-" + s.env + "/stage1/Meters/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        print("Ensek Meters Attributes mirror  job is running...".format(datetime.now().strftime('%H:%M:%S'),
                                                                         s.process_name))
        source_input = "s3://igloo-data-warehouse-prod/stage1/MetersAttributes/"
        destination_input = "s3://igloo-data-warehouse-" + s.env + "/stage1/MetersAttributes/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        print("Ensek Registers Mirror  job is running...".format(datetime.now().strftime('%H:%M:%S'), s.process_name))
        source_input = "s3://igloo-data-warehouse-prod/stage1/Registers/"
        destination_input = "s3://igloo-data-warehouse-" + s.env + "/stage1/Registers/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        print("Ensek Registers Attributes mirror  job is running...".format(datetime.now().strftime('%H:%M:%S'),
                                                                            s.process_name))
        source_input = "s3://igloo-data-warehouse-prod/stage1/RegistersAttributes/"
        destination_input = "s3://igloo-data-warehouse-" + s.env + "/stage1/RegistersAttributes/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        print("Ensek Readings Internal mirror  job is running...".format(datetime.now().strftime('%H:%M:%S'),
                                                                         s.process_name))
        source_input = "s3://igloo-data-warehouse-prod/stage1/ReadingsInternal/"
        destination_input = "s3://igloo-data-warehouse-" + s.env + "/stage1/ReadingsInternal/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        print("Ensek Account Status  Mirror  job is running...".format(datetime.now().strftime('%H:%M:%S'), s.process_name))
        source_input = "s3://igloo-data-warehouse-prod/stage1/AccountStatus/"
        destination_input = "s3://igloo-data-warehouse-" + s.env + "/stage1/AccountStatus/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        print("Ensek Registration Meterpoint Status Elec mirror  job is running...".format(datetime.now().strftime('%H:%M:%S'),s.process_name))
        source_input = "s3://igloo-data-warehouse-prod/stage1/RegistrationsElecMeterpoint/"
        destination_input = "s3://igloo-data-warehouse-" + s.env + "/stage1/RegistrationsElecMeterpoint/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        print("Ensek Registration Status  Gas Mirror  job is running...".format(datetime.now().strftime('%H:%M:%S'), s.process_name))
        source_input = "s3://igloo-data-warehouse-prod/stage1/RegistrationsGasMeterpoint/"
        destination_input = "s3://igloo-data-warehouse-" + s.env + "/stage1/RegistrationsGasMeterpoint/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        print("Ensek Internal Estimates Elec  mirror  job is running...".format(datetime.now().strftime('%H:%M:%S'),s.process_name))
        source_input = "s3://igloo-data-warehouse-prod/stage1/EstimatesElecInternal/"
        destination_input = "s3://igloo-data-warehouse-" + s.env + "/stage1/EstimatesElecInternal/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        print("Ensek Internal Estimates Gas  mirror  job is running...".format(datetime.now().strftime('%H:%M:%S'),s.process_name))
        source_input = "s3://igloo-data-warehouse-prod/stage1/EstimatesGasInternal/"
        destination_input = "s3://igloo-data-warehouse-" + s.env + "/stage1/EstimatesGasInternal/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        print("Ensek Tariff History Mirror  job is running...".format(datetime.now().strftime('%H:%M:%S'), s.process_name))
        source_input = "s3://igloo-data-warehouse-prod/stage1/TariffHistory/"
        destination_input = "s3://igloo-data-warehouse-" + s.env + "/stage1/TariffHistory/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        print("Ensek Tariff History Elec Stand Charge Mirror  job is running...".format(datetime.now().strftime('%H:%M:%S'),s.process_name))
        source_input = "s3://igloo-data-warehouse-prod/stage1/TariffHistoryElecStandCharge/"
        destination_input = "s3://igloo-data-warehouse-" + s.env + "/stage1/TariffHistoryElecStandCharge/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        print("Ensek Tariff History Gas Stand Charge Mirror  job is running...".format( datetime.now().strftime('%H:%M:%S'), s.process_name))
        source_input = "s3://igloo-data-warehouse-prod/stage1/TariffHistoryGasStandCharge/"
        destination_input = "s3://igloo-data-warehouse-" + s.env + "/stage1/TariffHistoryGasStandCharge/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        print("Ensek Tariff History Elec Unit Rates Mirror  job is running...".format(datetime.now().strftime('%H:%M:%S'), s.process_name))
        source_input = "s3://igloo-data-warehouse-prod/stage1/TariffHistoryElecUnitRates/"
        destination_input = "s3://igloo-data-warehouse-" + s.env + "/stage1/TariffHistoryElecUnitRates/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        print("Ensek Tariff History Gas Unit Rates Mirror  job is running...".format(datetime.now().strftime('%H:%M:%S'), s.process_name))
        source_input = "s3://igloo-data-warehouse-prod/stage1/TariffHistoryGasUnitRates/"
        destination_input = "s3://igloo-data-warehouse-" + s.env + "/stage1/TariffHistoryGasUnitRates/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        print("Ensek Account Transactions mirror  job is running...".format(datetime.now().strftime('%H:%M:%S'),s.process_name))
        source_input = "s3://igloo-data-warehouse-prod/stage1/AccountTransactions/"
        destination_input = "s3://igloo-data-warehouse-" + s.env + "/stage1/AccountTransactions/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        # # run EPC Certificates Mirror Job
        print("EPC Certificates  Mirror  job is running...".format(datetime.now().strftime('%H:%M:%S'), s.process_name))
        source_input = "s3://igloo-data-warehouse-prod/stage1/EPC/EPCCertificates/"
        destination_input = "s3://igloo-data-warehouse-" + s.env + "/stage1/EPC/EPCCertificates/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        # # run EPC Recommendations Mirror Job
        print("EPC Certificates  Mirror  job is running...".format(datetime.now().strftime('%H:%M:%S'), s.process_name))
        source_input = "s3://igloo-data-warehouse-prod/stage1/EPC/EPCRecommendations/"
        destination_input = "s3://igloo-data-warehouse-" + s.env + "/stage1/EPC/EPCRecommendations/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        # # run processing alp wcf script
        print("D18 BPP Mirror  job is running...".format(datetime.now().strftime('%H:%M:%S'), s.process_name))
        source_input = "s3://igloo-data-warehouse-prod/stage1/D18/D18BPP/"
        destination_input = "s3://igloo-data-warehouse-" + s.env + "/stage1/D18/D18BPP/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        print("D18 PPC Mirror  job is running...".format(datetime.now().strftime('%H:%M:%S'), s.process_name))
        source_input = "s3://igloo-data-warehouse-prod/stage1/D18/D18PPC/"
        destination_input = "s3://igloo-data-warehouse-" + s.env + "/stage1/D18/D18PPC/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        # # run processing mirror job
        print("NRL  Mirror job is running...".format(datetime.now().strftime('%H:%M:%S'), s.process_name))
        source_input = "s3://igloo-data-warehouse-prod/stage1/ReadingsNRL/"
        destination_input = "s3://igloo-data-warehouse-" + s.env + "/stage1/ReadingsNRL/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        # # run processing mirror job
        print("Nosi Mirror job is running...".format(datetime.now().strftime('%H:%M:%S'), s.process_name))
        source_input = "s3://igloo-data-warehouse-prod/stage1/ReadingsNOSIGas/"
        destination_input = "s3://igloo-data-warehouse-" + s.env + "/stage1/ReadingsNOSIGas/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        # # run processing mirror job
        print("Land Registry  Mirror job is running...".format(datetime.now().strftime('%H:%M:%S'), s.process_name))
        source_input = "s3://igloo-data-warehouse-prod/stage1/LandRegistry/"
        destination_input = "s3://igloo-data-warehouse-" + s.env + "/stage1/LandRegistry/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        # # run processing alp wcf script
        print("ALp CV job is running...".format(datetime.now().strftime('%H:%M:%S'), s.process_name))
        source_input = "s3://igloo-data-warehouse-prod/stage1/ALP/AlpCV/"
        destination_input = "s3://igloo-data-warehouse-" + s.env + "/stage1/ALP/AlpCV/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        # # run processing alp wcf script
        print("ALP WCF job is running...".format(datetime.now().strftime('%H:%M:%S'), s.process_name))
        source_input = "s3://igloo-data-warehouse-prod/stage1/ALP/AlpWCF/"
        destination_input = "s3://igloo-data-warehouse-" + s.env + "/stage1/ALP/AlpWCF/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        # # run processing mirror job
        print("Weather Mirror job is running...".format(datetime.now().strftime('%H:%M:%S'), s.process_name))
        source_input = "s3://igloo-data-warehouse-prod/stage1/HistoricalWeather/"
        destination_input = "s3://igloo-data-warehouse-" + s.env + "/stage1/HistoricalWeather/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

    util.batch_logging_update(s.all_jobid, 'e')
