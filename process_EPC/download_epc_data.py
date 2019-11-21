import sys
import requests
import os
import shutil
import zipfile
import fnmatch

sys.path.append('..')

from conf import config as con
from common import utils as util

class GetEPCFullFiles:

    def __init__(self):
        pass

    # UNZIP DOWNLOAD FILE
    def unzip_epc_zip(self, path_to_zip_file, extract_path):
        with zipfile.ZipFile(path_to_zip_file, 'r') as zip_ref:
            zip_ref.extractall(extract_path)

    # PREPARE FILES FOR S3 BUCKET
    def epc_pre_S3(self, extract_path):
        # download_path = "~/Downloads/test/all-domestic-certificates.zip"
        # extract_path = "~/Downloads/test/EPC"

        certificates_path = "~/Downloads/test/s3_Dir/CERT/"
        recommendation_path = "~/Downloads/test/s3_Dir/REC/"

        # CHECK IF DIRECTORY EXISTS
        if not os.path.exists(os.path.expanduser(certificates_path)):
            os.makedirs(os.path.expanduser(certificates_path))

        # CHECK IF DIRECTORY EXISTS
        if not os.path.exists(os.path.expanduser(recommendation_path)):
            os.makedirs(os.path.expanduser(recommendation_path))

            full_extract_path = os.path.expanduser(extract_path) + os.sep

            for subdir, dirs, files in os.walk(os.path.expanduser(extract_path)):
                for file in files:
                    # print os.path.join(subdir, file)
                    fspath = subdir + os.sep + file
                    fullpath = subdir + "_" + file
                    newFileName = fullpath.replace(full_extract_path, '')

                    if fnmatch.fnmatch(newFileName, '*certificates.csv'):
                        # MOVE TO CERTIFICATES DIRECTORY
                        shutil.copyfile(fspath, os.path.expanduser(certificates_path) + newFileName)
                        print(newFileName)

                    elif fnmatch.fnmatch(newFileName, '*recommendations.csv'):
                        # MOVE TO recommendations DIRECTORY
                        shutil.copyfile(fspath, os.path.expanduser(recommendation_path) + newFileName)
                        print(newFileName)

    # DOWNLOAD FILE
    def download_epc_zip(self):
        site_url, file_url = util.get_epc_api_info_full('igloo_epc_certificates_full')
        download_path = "~/Downloads/test/all-domestic-certificates.zip"
        extract_path = "~/Downloads/test/EPC"

        # OPEN SESSION
        s = requests.Session()
        s.get(site_url)
        s.post(site_url)

        # DOWNLOAD ZIP FILE
        with open(os.path.expanduser(download_path), 'wb') as file:
            r = s.get(file_url, stream=True, timeout=3600)
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    file.write(chunk)
                    file.flush()

        # UNZIP ZIP FILE
        unzip_epc_zip(os.path.expanduser(download_path), os.path.expanduser(extract_path))

        # ETRACT FILES FOR S3
        epc_pre_S3(extract_path)


if __name__ == '__main__':

    p = GetEPCFullFiles()
    p.download_epc_zip()