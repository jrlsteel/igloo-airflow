import sys
import io
import multiprocessing
from multiprocessing import freeze_support
import timeit

sys.path.append("..")

from connections import connect_db as db
from common import utils as util


class ProcessSFTPFiles:
    def __init__(self, key):
        self.dir = util.get_dir()
        self._key = key
        self.bucket_name = self.dir["s3_bucket"]
        self.sftp_path = self.dir[self._key]["SFTP"]
        self.s3_upload_key = self.dir[self._key]["Raw"]
        self.s3 = db.get_S3_Connections_client()

    def upload_to_s3(self, files, replace_delimiter_from=""):
        """
        This function does the following tasks:
            1. Downloads the data from SFTP folder.
            2. Read into memory
            3. Copy the data to s3
        """
        sftp_conn = None
        try:
            sftp_conn = db.get_ensek_sftp_connection()
            i = 0
            for file in files:
                print(file)
                if i == 50:  # reset connection for every 50 files read to avoid timeout error.

                    sftp_conn.close()
                    i = 0
                    print("connection reset")
                    sftp_conn = db.get_ensek_sftp_connection()

                filename = str(file)
                filepath = "/" + self.sftp_path + "/" + filename

                with io.BytesIO() as file_data:  # read files in memory and copy to s3
                    sftp_conn.getfo(filepath, file_data)
                    file_data.seek(0)

                    if replace_delimiter_from:
                        data_str = str(file_data.getvalue(), "UTF-8")
                        data = data_str.replace(replace_delimiter_from, ",")
                        # print(data)
                        self.s3.put_object(Bucket=self.bucket_name, Key=self.s3_upload_key + filename, Body=data)
                    else:
                        self.s3.put_object(Bucket=self.bucket_name, Key=self.s3_upload_key + filename, Body=file_data)

                i = i + 1

        except Exception as e:
            print("Error :" + str(e))
            sys.exit(1)

        finally:
            sftp_conn.close()  # close connection


if __name__ == "__main__":

    freeze_support()
    key = ""
    # s = ProcessSFTPFiles(key)
    #
    # start = timeit.default_timer()
    #
    # s.upload_to_s3()   # Enable this to test without multiprocessing
