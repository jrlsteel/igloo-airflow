import sys
import multiprocessing
from time import sleep
from multiprocessing import freeze_support
import timeit

sys.path.append('..')

from connections import connect_db as db
from common import utils as util


class ProcessNRL:

    def __init__(self):
        self.dir = util.get_dir()
        self.bucket_name = self.dir['s3_bucket']
        self.prefix = self.dir['s3_nrl_key']['Raw']
        self.suffix = self.dir['s3_nrl_key']['Suffix']
        self.upload_key_nrl = self.dir['s3_nrl_key']['NRL']

    def process_data(self, nrl_keys):

        """
        :param nrl_keys: list of NRL files stored in s3
        :return: None

        This function copies the NRL data that is being loaded into NRLRaw folder and
            1. Picks the T-04 records from NRLRaw files which is in .flw format and stores back in s3 in proper csv format.
        """

        try:
            s31 = db.get_S3_Connections_client()

            # loop each file
            for nrlkey in nrl_keys:
                print(nrlkey)

                obj = s31.get_object(Bucket=self.bucket_name, Key=nrlkey)

                obj_str = obj['Body'].read().decode('utf-8').splitlines(True)
                # get the filename from the key
                filename = nrlkey.replace(self.prefix, '')

                fileNRL_csv = filename.replace(self.suffix, '.csv')

                filekey = fileNRL_csv.replace('.csv', '') + ','

                # initializing variables
                line_A00 = line_T04 = file_date = line_nrl = ''

                # header line for BPP
                line_header = "FILE_KEY,FILE_DATE,FILE_NAME,TRANSACTION_TYPE,SUPPLY_POINT_CONFIRMATION_REFERENCE,NOMINATION_SHIPPER_REF,LDZ_IDENTIFIER,EXIT_ZONE_IDENTIFIER,END_USER_CATEGORY,SUPPLY_METER_POINT_REFERENCE,POSTCODE_OUTCODE,POSTCODE_INCODE,METER_SERIAL_NUMBER,CONVERTOR_SERIAL_NUMBER,NUMBER_OF_DIALS_OR_DIGITS,METER_STATUS,CORRECTION_FACTOR,IMPERIAL_METER_INDICATOR,READING_FACTOR,METER_READING_UNITS,DRE_PRESENT,SUPPLY_METER_POINT_AQ,REVISED_SUPPLY_METER_POINT_AQ,REVISED_SUPPLY_METER_POINT_AQ_EFFECTIVE_DATE,AQ_CALC_PERIOD_START,AQ_CALC_PERIOD_END,START_READING,END_READING,ROUND_THE_CLOCK,NUMBER_OF_EXCHANGES,PERCENTAGE_AQ_CHANGE,CONFIRMATION_EFFECTIVE_DATE,TRANSPORTER_NOMINATION_REFERENCE,OFFER_NUMBER,SUPPLY_METER_POINT_SOQ,AQ_CORRECTION_REASON_CODE,BACKSTOP_DATE,FORMULA_YEAR_SMP_AQ,FORMULA_YEAR_SMP_SOQ,FORMULA_YEAR_SMP_AQ_START_READING,FORMULA_YEAR_SMP_AQ_END_READING,FORMULA_YEAR_SMP_AQ_START_DATE,CLASS_1_THRESHOLD_CROSSER_COUNT,PRIORITY_CONSUMER_COUNT,METER_READ_BATCH_FREQUENCY,MRF_TYPE_CODE,MARKET_CATEGORY,EUC_EFFECTIVE_DATE,EUC_CHANGE_REASON_CODE,EUC_DESCRIPTION,SUPPLY_POINT_CATEGORY,FORMULA_YEAR_SMP_AQ_START_READING_DATE,FORMULA_YEAR_SMP_AQ_END_READING_DATE"

                # write header
                line_final = line_header + '\n'

                # read lines in one file
                for lines in obj_str:
                    line = lines.replace('\"', '')
                    if line.split(',')[0] == 'A00':
                        line_A00 = line.replace(',', '-').replace('\n', '').replace('\r', '') + ','
                        file_date = line_A00.split('-')[3] + ','
                    if line.split(',')[0] == 'T04':
                        line_T04 = line.replace('\"', '')
                        line_nrl += line_A00 + file_date + filekey + line_T04
                        # print(line_nrl)

                line_final += line_nrl

                s31.put_object(Bucket=self.bucket_name, Key=self.upload_key_nrl + fileNRL_csv, Body=line_final)

        except Exception as e:
            print(" Error :" + str(e))
            sys.exit(1)


if __name__ == "__main__":

    freeze_support()
    s3 = db.get_S3_Connections_client()
    p = ProcessNRL()
    nrl_keys_s3 = util.get_keys_from_s3(s3, bucket_name=p.bucket_name, prefix=p.prefix, suffix=p.suffix)

    ##### Enable this to test without multiprocessing
    # p.process_data(nrl_keys_s3)

    ######### multiprocessing starts  ##########
    env = util.get_env()
    if env == 'uat':
        n = 12  # number of process to run in parallel
    else:
        n = 12
    print(len(nrl_keys_s3))
    k = int(len(nrl_keys_s3) / n)  # get equal no of files for each process
    print(k)
    processes = []
    lv = 0

    start = timeit.default_timer()

    for i in range(n+1):
        p1 = ProcessNRL()
        print(i)
        uv = i * k
        if i == n:
            # print(nrl_keys_s3[l:])
            t = multiprocessing.Process(target=p1.process_data, args=(nrl_keys_s3[lv:],))
        else:
            # print(nrl_keys_s3[l:u])
            t = multiprocessing.Process(target=p1.process_data, args=(nrl_keys_s3[lv:uv],))
        lv = uv

        processes.append(t)

    for p in processes:
        p.start()
        sleep(2)

    for process in processes:
        process.join()
    ####### multiprocessing Ends #########

    print("Process completed in " + str(timeit.default_timer() - start) + ' seconds')
