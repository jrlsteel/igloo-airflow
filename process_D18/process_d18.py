import sys
import os
import multiprocessing

sys.path.append('..')

from conf import config as con
from connections import connect_db as db


class ProcessD18:

    def __init__(self):
        self.bucket_name = 'igloo-uat-bucket'
        self.prefix = 'D018/'
        self.suffix = '.flw'
        self.d018_archive_key = 'D018Archive'
        self.upload_key_BPP = 'D018CSV/BPP/'
        self.upload_key_PPC = 'D018CSV/PPC/'
        self.D018_dir = sys.path[0] + '/files/D018/'
        self.s3 = db.get_S3_Connections_client()

    def process_d18_data(self, d18_keys):

        try:
            # loop each file
            for d18key in d18_keys:
                print(d18key)
                obj = self.s3.get_object(Bucket=self.bucket_name, Key=d18key)

                obj_str = obj['Body'].read().decode('utf-8').splitlines(True)

                if not os.path.exists(self.D018_dir):
                    os.makedirs(self.D018_dir)

                filename = d18key.replace('D018/', '')
                fileBPP_csv = filename.replace('.flw', '_BPP.csv')
                filePPC_csv = filename.replace('.flw', '_PPC.csv')

                # initializing variables
                line_BPP_2 = line_PPC_2 = line_BPP_1 = prev_line_PPC = prev_line_BPP = line_ZHV = line_GSP = line_PCL = line_BPP = line_PPC = line_SSC = line_VMR = ''

                # read lines in one file
                for lines in obj_str:
                    if lines.split('|')[0] == 'ZHV':
                        line_ZHV = lines.replace('\n', '')
                    elif lines.split('|')[0] == 'GSP':
                        line_GSP = lines.replace('\n', '')
                    elif lines.split('|')[0] == 'PCL':
                        line_PCL = lines.replace('\n', '')
                    elif lines.split('|')[0] == 'BPP':
                        line_BPP = lines.replace('\n', '')
                    elif lines.split('|')[0] == 'SSC':
                        line_SSC = lines.replace('\n', '')
                    elif lines.split('|')[0] == 'VMR':
                        line_VMR = lines.replace('\n', '')
                    elif lines.split('|')[0] == 'PPC':
                        line_PPC = lines.replace('\n', '')

                    if line_BPP != '' and line_BPP != prev_line_BPP:
                        line_BPP_1 = line_ZHV + line_GSP + line_PCL + line_BPP
                        prev_line_BPP = line_BPP
                        # f_BPP.write(line_BPP_1 + '\n')
                        line_BPP_2 += line_BPP_1
                        # print(line_BPP_1)

                    if line_PPC != '' and line_PPC != prev_line_PPC:
                        line_PPC_1 = line_ZHV + line_GSP + line_PCL + line_SSC + line_VMR + line_PPC
                        prev_line_PPC = line_PPC
                        # f_PPC.write(line_PPC_1 + '\n')
                        line_PPC_2 += line_PPC_1 + '\n'
                        # print(line_PPC_1)

                # upload to s3
                self.s3.put_object(Bucket=self.bucket_name, Key=self.upload_key_BPP + fileBPP_csv, Body=line_BPP_2)
                self.s3.put_object(Bucket=self.bucket_name, Key=self.upload_key_PPC + filePPC_csv, Body=line_PPC_2)

                # archive d18
                copy_source = {
                                'Bucket': self.bucket_name,
                                'Key': d18key
                              }

                self.s3.copy(copy_source, self.bucket_name, self.d018_archive_key + '/' + filename)

        except:
            raise
        # finally:
            # f_BPP.close()
            # f_PPC.close()

    def get_keys_from_s3(self):
        d18_keys_s3 = []
        # get all the files in D018 object
        for obj in self.s3.list_objects(Bucket=self.bucket_name, Prefix=self.prefix)['Contents']:
            if obj['Key'].endswith(self.suffix):
                d18_keys_s3.append(obj['Key'])
        return d18_keys_s3


if __name__ == "__main__":

    p = ProcessD18()
    d18_keys_s3 = p.get_keys_from_s3()
    k = int(len(d18_keys_s3) / 5)
    processes = []
    l = 0

    for i in range(6):
        print(i)
        u = i * k
        if i == 5:
            # print(d18_keys[l:])
            t = multiprocessing.Process(target=p.process_d18_data, args=(d18_keys_s3[l:],))
        else:
            # print(d18_keys[l:u])
            t = multiprocessing.Process(target=p.process_d18_data, args=(d18_keys_s3[l:u],))
        l = u
        processes.append(t)

    for p in processes:
        p.start()

    for process in processes:
        process.join()

