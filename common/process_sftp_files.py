import sys
import io
import multiprocessing
from multiprocessing import freeze_support
import timeit

sys.path.append('..')

from connections import connect_db as db
from common import utils as util


class ProcessSFTPFiles:

    def __init__(self, key):
        self.dir = util.get_dir()
        self.key = key
        self.bucket_name = self.dir['s3_bucket']
        self.sftp_path = self.dir[self.key]['SFTP']
        self.s3_upload_key = self.dir[self.key]['Raw']
        self.i = 1
        self.sftp_conn, self.files = util.get_files_from_sftp(self.sftp_path)
        self.s3 = db.get_S3_Connections_client()

    def __upload_to_s3(self, file):

        print(file)
        if self.i == 50:  # reset connection for every 50 files read to avoid timeout error.
            self.sftp_conn.close()
            self.sftp_conn = db.get_ensek_sftp_connection()
            self.i = 0
            print('connection reset')

        filename = str(file)
        filepath = '/' + self.sftp_path + '/' + filename

        with io.BytesIO() as file_data:  # read files in memory and copy to s3
            self.sftp_conn.getfo(filepath, file_data)
            file_data.seek(0)
            self.s3.put_object(Bucket=self.bucket_name, Key=self.s3_upload_key + filename, Body=file_data)

        self.i = self.i + 1

    def upload_to_s3(self):
        """
        This function does the following tasks:
            1. Downloads the data from SFTP folder.
            2. Read into memory
            3. Copy the data to s3

        :param : Contains the list of files to be processed
        :param s3: holds the s3 connection
        :return: None
        """
        try:
            i = 1
            [self.__upload_to_s3(file) for file in self.files]

        except Exception as e:
            print("Error :" + str(e))
            sys.exit(1)

        finally:
            if self.sftp_conn is not None:
                self.sftp_conn.close()  # close connection


if __name__ == '__main__':

    freeze_support()
    key = ''
    s = ProcessSFTPFiles(key)

    start = timeit.default_timer()

    s.upload_to_s3()   # Enable this to test without multiprocessing

    ######### multiprocessing starts  ##########

    # env = util.get_env()
    # if env == 'uat':
    #     n = 12  # number of process to run in parallel
    # else:
    #     n = 24
    # print(len(files))
    # k = int(len(files) / n)  # get equal no of files for each process
    # print(k)
    # processes = []
    # lv = 0
    #
    # for i in range(n+1):
    #     s = DownloadNRLFiles()
    #     print(i)
    #     uv = i * k
    #     if i == n:
    #         # print(d18_keys_s3[l:])
    #         t = multiprocessing.Process(target=s.main, args=(files[lv:], s3, sftp_conn))
    #     else:
    #         # print(d18_keys_s3[l:u])
    #         t = multiprocessing.Process(target=s.main, args=(files[lv:uv], s3, sftp_conn))
    #     lv = uv
    #
    #     processes.append(t)
    #
    # for p in processes:
    #     p.start()
    #
    # for process in processes:
    #     process.join()
    # ###### multiprocessing Ends #########
    #
    # print("Process completed in " + str(timeit.default_timer() - start) + ' seconds')