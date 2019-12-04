import sys
import requests
import os
import shutil
import zipfile
import fnmatch
import time
import multiprocessing
from multiprocessing import freeze_support
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings("ignore")

sys.path.append('..')

from conf import config as con
from common import utils as util
from connections.connect_db import get_boto_S3_Connections as s3_con
from connections import connect_db as db

class GetEPCFullFiles:

    def __init__(self):
        self.dir = util.get_dir()
        self.bucket_name = self.dir['s3_bucket']
        self.s3 = s3_con(self.bucket_name)
        self.EPCFullDownload_path = self.dir['s3_epc_full_key']['EPCFullDownload_path']
        self.EPCFullExtract_path = self.dir['s3_epc_full_key']['EPCFullExtract_path']
        self.EPCFullCertificates = self.dir['s3_epc_full_key']['EPCFullCertificates']
        self.EPCFullRecommendations = self.dir['s3_epc_full_key']['EPCFullRecommendations']

    # UNZIP DOWNLOAD FILE
    def unzip_epc_zip(self, path_to_zip_file, extract_path):
        with zipfile.ZipFile(path_to_zip_file, 'r') as zip_ref:
            print("UnZip Process Started")
            zip_ref.extractall(extract_path)

    # PREPARE FILES FOR S3 BUCKET
    def epc_pre_S3(self, extract_path, certificates_path, recommendation_path):
        # CHECK IF DIRECTORY EXISTStion_path)
        if not os.path.exists(os.path.expanduser(certificates_path)):
          os.makedirs(os.path.expanduser(certificates_path))

        # CHECK IF DIRECTORY EXISTS
        if not os.path.exists(os.path.expanduser(recommendation_path)):
          os.makedirs(os.path.expanduser(recommendation_path))


        full_extract_path = os.path.basename(extract_path) + os.sep

        for subdir, dirs, files in os.walk(os.path.basename(extract_path)):
            for file in files:
                # print os.path.join(subdir, file)
                fspath = subdir + os.sep + file
                fullpath = subdir + "-" + file
                newFileName = fullpath.replace(full_extract_path, '')

                if fnmatch.fnmatch(newFileName, '*certificates.csv'):
                    # MOVE TO CERTIFICATES DIRECTORY
                    s3_directory_name = subdir.replace('EPC_full\\', '')
                    filename_path = os.path.expanduser(certificates_path) + newFileName
                    shutil.copyfile(fspath, os.path.expanduser(certificates_path) + newFileName)
                    p.push_to_s3(filename_path, newFileName, s3_directory_name)
                    print(newFileName)

                elif fnmatch.fnmatch(newFileName, '*recommendations.csv'):
                    # MOVE TO recommendations DIRECTORY
                    s3_directory_name = fullpath.replace(subdir, '')
                    filename_path = os.path.expanduser(recommendation_path) + newFileName
                    shutil.copyfile(fspath, os.path.expanduser(recommendation_path) + newFileName)
                    p.push_to_s3(filename_path, newFileName, s3_directory_name)
                    print(newFileName)



    def push_to_s3(self, filename_path, newFileName,subdir):
        k = self.s3
        dir_s3 = self.dir
        file_location = filename_path
        epc_rows_df = pd.read_csv(file_location)
        if epc_rows_df.empty:
            print(" - has no EPC data")
        else:
            #May have to add this lines in later if glue cat does not work.
            # epc_rows_df.columns = epc_rows_df.columns.str.replace('-', '_')
            # epc_rows_df = epc_rows_df.replace(',', '-', regex=True)
            # epc_rows_df = epc_rows_df.replace('"', '', regex=True)
            epc_rows_df_string = epc_rows_df.to_csv(None, index=False)
            file_name_full_epc = newFileName
            if fnmatch.fnmatch(newFileName, '*certificates.csv'):
                # MOVE TO CERTIFICATES DIRECTORY
                k.key = dir_s3['s3_epc_full_key']['EPCFullCertificates'] + '/' + subdir + '/' + file_name_full_epc
                # print(epc_rows_df_string)
                k.set_contents_from_string(epc_rows_df_string)

            elif fnmatch.fnmatch(newFileName, '*recommendations.csv'):
                # MOVE TO recommendations DIRECTORY
                k.key = dir_s3['s3_epc_full_key']['EPCFullRecommendations'] + '/' + subdir + '/' + file_name_full_epc
                # print(epc_rows_df_string)
                k.set_contents_from_string(epc_rows_df_string)




    # DOWNLOAD FILE
    def download_epc_zip(self):

        dir_s3 = self.dir
        bucket_name = self.bucket_name
        k= self.s3
        site_url, file_url = util.get_epc_api_info_full('igloo_epc_full')
        token = con.igloo_epc_full["token"]
        fullsite_url = site_url + token

        #download_path = "~/enzek-meterpoint-readings/process_EPC/all-domestic-certificates.zip"
        #extract_path = "./EPC_full"
        #certificates_path = "./EPCCertificates/"
        #recommendation_path = "./EPCRecommendations/"

        download_path = "~" + os.sep + "enzek-meterpoint-readings" + os.sep + "process_EPC" + os.sep + "all-domestic-certificates.zip"
        extract_path = "." + os.sep + "EPC_full"
        certificates_path = "." + os.sep + "EPCCertificates" + os.sep
        recommendation_path = "." + os.sep + "EPCRecommendations" + os.sep

        # OPEN SESSION
        s = requests.Session()
        s.get(fullsite_url)
        s.post(fullsite_url)

        # DOWNLOAD ZIP FILE
        # with open(os.path.basename(download_path), 'wb') as file:
        #     r = s.get(file_url, stream=True, timeout=3600)
        #     for chunk in r.iter_content(chunk_size=1024):
        #         if chunk:
        #             file.write(chunk)
        #             file.flush()

        # UNZIP ZIP FILE
        # self.unzip_epc_zip(os.path.basename(download_path), os.path.abspath(extract_path))

        # ETRACT FILES FOR S3
        p.epc_pre_S3(os.path.abspath(extract_path), certificates_path, recommendation_path)



if __name__ == '__main__':

    p = GetEPCFullFiles()
    p.download_epc_zip()









