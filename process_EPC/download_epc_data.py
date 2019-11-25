import timeit

import sys
import requests
import os
import shutil
import zipfile
import fnmatch
import multiprocessing
from multiprocessing import freeze_support

sys.path.append('..')

from conf import config as con
from common import utils as util
from connections.connect_db import get_boto_S3_Connections as s3_con

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
            zip_ref.extractall(extract_path)

    # PREPARE FILES FOR S3 BUCKET
    def epc_pre_S3(self, extract_path):
        certificates_path = self.EPCFullCertificates
        recommendation_path = self.EPCFullRecommendations

        full_extract_path = os.path.basename(extract_path) + os.sep

        for subdir, dirs, files in os.walk(os.path.basename(extract_path)):
            for file in files:
                # print os.path.join(subdir, file)
                fspath = subdir + os.sep + file
                fullpath = subdir + "_" + file
                newFileName = fullpath.replace(full_extract_path, '')

                if fnmatch.fnmatch(newFileName, '*certificates.csv'):
                    # MOVE TO CERTIFICATES DIRECTORY
                    shutil.copyfile(fspath, os.path.basename(certificates_path) + newFileName)
                    print(newFileName)

                elif fnmatch.fnmatch(newFileName, '*recommendations.csv'):
                    # MOVE TO recommendations DIRECTORY
                    shutil.copyfile(fspath, os.path.basename(recommendation_path) + newFileName)
                    print(newFileName)

    # DOWNLOAD FILE
    def download_epc_zip(self):
        site_url, file_url = util.get_epc_api_info_full('igloo_epc_full')
        token = con.igloo_epc_full["token"]
        fullsite_url = site_url + token
        download_path = self.EPCFullDownload_path
        extract_path =  self.EPCFullExtract_path

        # OPEN SESSION
        s = requests.Session()
        s.get(fullsite_url)
        s.post(fullsite_url) 

        # DOWNLOAD ZIP FILE
        with open(os.path.basename(download_path), 'wb') as file:
            r = s.get(file_url, stream=True, timeout=3600)
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    file.write(chunk)
                    file.flush()

        # UNZIP ZIP FILE
        self.unzip_epc_zip(os.path.basename(download_path), os.path.basename(extract_path))

        # ETRACT FILES FOR S3
        self.epc_pre_S3(extract_path)


    def extract_epc_full_data(self,k,dir_s3):
        dir_s3 = self.dir
        k = self.s3
        k.key = dir_s3['s3_epc_full_key']['EPCFullDownload_path']





if __name__ == '__main__':

    #freeze_support()

    p = GetEPCFullFiles()
    p.download_epc_zip()




