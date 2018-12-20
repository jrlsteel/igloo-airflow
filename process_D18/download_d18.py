import sys
import io
import multiprocessing
from multiprocessing import freeze_support
import timeit

sys.path.append('..')

from connections import connect_db as db
from common import utils as util

class GetD18Files:

    def __init__(self):
        self.dir = util.get_dir()
        self.bucket_name = self.dir['s3_bucket']
        self.upload_key = self.dir['s3_d18_key']['D18Raw']
        self.sftp_d18_dir = self.dir['s3_d18_key']['D18_SFTP']

    def sftp_to_Ensek(self, d18_files, s3):
        """
        This function does the following tasks:
                        1. Downloads the data from Ensek SFTP folder.
                        2. Read into memory
                        3. Copy the data to s3

        :param d18_files: Contains the list of files to be processed
        :param s3: holds the s3 connection
        :return: None:
        """
        sftp = None
        try:
            sftp = db.get_ensek_sftp_connection()  # get sftp connection
            i = 1
            for file in d18_files:
                print(file)
                if i == 50:  # reset connection for every 50 files read to avoid timeout error.
                    sftp.close()
                    sftp = db.get_ensek_sftp_connection()
                    i = 0

                filename = str(file)
                filepath = '/' + self.sftp_d18_dir + '/' + filename

                with io.BytesIO() as file_data:  # read files in memory and copy to s3
                    sftp.getfo(filepath, file_data)
                    file_data.seek(0)
                    s3.put_object(Bucket=self.bucket_name, Key=self.upload_key + filename, Body=file_data)

                i = i+1
                # break

        except Exception as e:
            print("Error :" + str(e))
            sys.exit(1)

        finally:
            if sftp is not None:
                sftp.close()  # close connection

    def get_all_d18_files(self):
        """
        :return: All the files from Ensek D18 folder through SFTP
        """

        sftp = db.get_ensek_sftp_connection()  # get sftp connection
        ensek_d18_files = sftp.listdir(self.sftp_d18_dir)  # get all d18 files ensek
        sftp.close()

        return ensek_d18_files


if __name__ == '__main__':

    freeze_support()

    s3 = db.get_S3_Connections_client()  # get s3 connection
    s = GetD18Files()
    d18_files = s.get_all_d18_files()  # get list of files from ensek through sftp

    start = timeit.default_timer()

    # s.sftp_to_Ensek(d18_files, s3) ##### Enable this to test without multiprocessing
    ######### multiprocessing starts  ##########
    env = util.get_env()
    if env == 'uat':
        n = 12  # number of process to run in parallel
    else:
        n = 24
    print(len(d18_files))
    k = int(len(d18_files) / n)  # get equal no of files for each process
    print(k)
    processes = []
    lv = 0

    for i in range(n+1):
        s = GetD18Files()
        print(i)
        uv = i * k
        if i == n:
            # print(d18_keys_s3[l:])
            t = multiprocessing.Process(target=s.sftp_to_Ensek, args=(d18_files[lv:], s3))
        else:
            # print(d18_keys_s3[l:u])
            t = multiprocessing.Process(target=s.sftp_to_Ensek, args=(d18_files[lv:uv], s3))
        lv = uv

        processes.append(t)

    for p in processes:
        p.start()

    for process in processes:
        process.join()
    ####### multiprocessing Ends #########

    print("Process completed in " + str(timeit.default_timer() - start) + ' seconds')