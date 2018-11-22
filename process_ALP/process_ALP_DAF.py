import sys
import os
import multiprocessing
from time import sleep
from multiprocessing import freeze_support
import timeit

sys.path.append('..')

from connections import connect_db as db


class ProcessALPDAF:

    def __init__(self):

        self.bucket_name = 'igloo-uat-bucket'
        self.prefix = 'ensek-meterpoints/alp-daf'
        self.suffix = '.txt'
        self.alp_daf_archive_key = 'ensek-meterpoints/alp/alp-daf-archive'
        self.upload_key_alp_daf = 'ensek-meterpoints/alp/alp-daf-csv/'
        self.alp_daf_dir = sys.path[0] + '/files/alp/'


    def process_alp_daf_data(self, alp_daf_keys):
        s31 = db.get_S3_Connections_client()
        try:
            # loop each file
            for alp_daf_key in alp_daf_keys:
                #print(alp_daf_key)

                obj = s31.get_object(Bucket=self.bucket_name, Key=alp_daf_key)

                obj_str = obj['Body'].read().decode('utf-8').splitlines(True)

                if not os.path.exists(self.alp_daf_dir):
                    os.makedirs(self.alp_daf_dir)

                filename = alp_pn_key.replace('ensek-meterpoints/alp/alp-daf/', '')
                file_alp_daf_csv = filename.replace('.LPA', '_alp_daf.csv')

                # initializing variables
                line_header_alp_daf = 'RegionProfile,Date,ForecastDocumentation,Variance'

                # write header
                full_line = line_header_alp_daf + '\n'

                # read lines in one file
                for lines in obj_str:

                    full_line += lines + '\n'

                # upload to s3
                #s31.put_object(Bucket=self.bucket_name, Key=self.upload_key_alp_daf + file_alp_daf_csv, Body=full_line)
                print(self.upload_key_alp_daf + file_alp_daf_csv)

                # archive alp_pn
                copy_source = {
                                 'Bucket': self.bucket_name,
                                 'Key': alp_daf_key
                               }

                #s31.copy(copy_source, self.bucket_name, self.alp_daf_archive_key + '/' + filename)
                print(self.alp_daf_archive_key + '/' + filename)

        except:
            raise

    def get_keys_from_s3(self, s3):
        alp_pn_keys = []
        # get all the files in alp_pn object
        for obj in s3.list_objects(Bucket=self.bucket_name, Prefix=self.prefix)['Contents']:
            if obj['Key'].endswith(self.suffix):
                alp_pn_keys.append(obj['Key'])
        return alp_pn_keys


if __name__ == "__main__":

#Why this number of processes
    freeze_support()
    s3 = db.get_S3_Connections_client()
    p = ProcessALPDAF()
    alp_daf_keys_s3 = p.get_keys_from_s3(s3)
    k = int(len(alp_daf_keys_s3) / + 3)
    processes = []
    l = 0
    start = timeit.default_timer()
    for i in range(6):
        p1 = ProcessALPDAF()
        print(i)
        u = i * k
        if i == 5:
            #print(alp_daf_keys_s3[l:])
            t = multiprocessing.Process(target=p1.process_alp_daf_data, args=(alp_daf_keys_s3[l:],))
        else:
            #print(alp_daf_keys_s3[l:u])
            t = multiprocessing.Process(target=p1.process_alp_daf_data, args=(alp_daf_keys_s3[l:u],))
        l = u

        processes.append(t)

    for p in processes:
        p.start()
        sleep(2)

    for process in processes:
        process.join()

    print("Process completed in " + str(timeit.default_timer() - start) + ' seconds')
