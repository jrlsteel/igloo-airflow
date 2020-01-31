import timeit

import boto3
import glob
import pandas as pd
import requests
import json
from pandas.io.json import json_normalize
from ratelimit import limits, sleep_and_retry
import time
from requests import ConnectionError
import csv
import multiprocessing
from multiprocessing import freeze_support
from datetime import datetime

import sys
import os

sys.path.append('..')

from common import utils as util
from conf import config as con
from connections.connect_db import get_boto_S3_Connections as s3_con
from connections import connect_db as db



class ExtractEnsekFiles(object):

    def __init__(self):
        self.env = util.get_env()
        self.dir = util.get_dir()
        self.bucket_name = self.dir['s3_bucket']
        self.now = datetime.now()
        self.prefix = self.dir['s3_ensekflow_key']['EFprefix']
        self.suffix = self.dir['s3_ensekflow_key']['EFSuffix']
        self.EFStartAfter = self.dir['s3_ensekflow_key']['EFStartAfter']
        self.fileSource = "s3://igloo-data-warehouse-" + self.env + "/stage1Flows/outbound/master/"
        self.fileStore = "s3://igloo-data-warehouse-" + self.env + "/stage1Flows/outbound/"
        self.s3 = boto3.client("s3")
        self.all_objects = s3.list_objects(Bucket='igloo-data-warehouse-uat')

    def process_flow_data(self, flow_keys):
        bucket_name = self.bucket_name

        """
        :param d18_keys: list of D18 files stored in s3
        :return: None

        This function copies the D18 data that is being loaded into D18Raw folder and
            1. Splits the data into BPP and PPC in .csv format and stores back in s3.
            2. Also the data is archived in s3
        """

        try:
            s31 = db.get_S3_Connections_client()

            # loop each file
            for fkey in flow_keys:
                print(fkey)
                print(bucket_name)
                obj = s31.get_object(Bucket=bucket_name, Key=fkey)

                obj_str = obj['Body'].read().decode('utf-8').splitlines(True)
                # get the filename from the key
                filename = fkey.replace(self.prefix, '')
                with open(filename) as fp:
                    row = fp.readline()
                    filekey = row[2]
                    # print(df_string)
                    file_name_readings = filename + '_' + str(filekey)
                    # Copy from master directory to subdirectory
                    #s3_resource.Object(self.bucket_name, self.fileSource+filekey+"/"+filename).copy_from(CopySource=self.fileStore+filename)
                    # upload to s3
                    keypath = self.prefix + "/" + filekey + filename
                    print(keypath)

                    copy_source = {
                                    'Bucket': self.bucket_name,
                                    'Key': fkey
                                  }

                    # archive d18
                    s31.copy(copy_source, self.bucket_name, keypath)
                    fp.close()



        except Exception as e:
            print(" Error :" + str(e))
            sys.exit(1)




    def get_keys_from_s3(self, s3):
        """
        This function gets all the d18_keys that needs to be processed.
        :param s3: holds the s3 connection
        :return: list of d18 filenames
        """
        s3client = s3
        bucket = self.bucket_name
        flow_keys = []
        EFprefix= self.EFStartAfter
        theobjects = s3client.list_objects_v2(Bucket=bucket, StartAfter=EFprefix)
        for object in theobjects['Contents']:
            if object['Key'].endswith(self.suffix):
                flow_keys.append(object['Key'])
        return flow_keys




if __name__ == '__main__':

    freeze_support()
    s3 = db.get_S3_Connections_client()
    p = ExtractEnsekFiles()
    ef_keys_s3 = p.get_keys_from_s3(s3)

    #Ensek Internal Estimates Ensek Extract
    print("{0}: Ensek flows extract Jobs running...".format(datetime.now().strftime('%H:%M:%S')))
    print(ef_keys_s3)
    p.process_flow_data(ef_keys_s3)




