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
import pandas_redshift as pr

import sys
import os

sys.path.append('..')

from common import utils as util
from conf import config as con
from connections.connect_db import get_boto_S3_Connections as s3_con
from connections import connect_db as db



class ExtractEnsekFiles(object):

    def __init__(self, _type):
        self.env = util.get_env()
        self.dir = util.get_dir()
        self.bucket_name = self.dir['s3_bucket']
        self.now = datetime.now()
        self.type = _type
        self.prefix = self.dir['s3_ensekflow_key'][self.type]['EFprefix']
        self.suffix = self.dir['s3_ensekflow_key'][self.type]['EFSuffix']
        self.EFStartAfter = self.dir['s3_ensekflow_key'][self.type]['EFStartAfter']
        self.EFfileStore = self.dir['s3_ensekflow_key'][self.type]['EFfileStore']
        self.connect = db.get_redshift_connection()

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
            df_flowid = pd.DataFrame()
            for fkey in flow_keys:
                print(fkey)
                obj = s31.get_object(Bucket=bucket_name, Key=fkey)
                obj_str = obj['Body'].read().decode('utf-8').splitlines(True)
                filename = fkey.replace(self.EFStartAfter, '')

                fileUFF_csv = filename.replace(self.suffix, '_UFF.csv')
                file_content = []
                for lines in obj_str:
                    line_rep = lines.replace('\n', '').replace('|', ',')
                    line_sp = line_rep.split(',')
                    file_content.append(line_sp)
                flow_id = file_content[0][2]
                row_1 = file_content[0]
                row_2 = file_content[1]
                etlchange = self.now
                worddoc =''
                for wd in file_content:
                    worddoc += ','.join(wd) + '\n'
                filecontents = worddoc
                # upload to s3
                keypath = self.EFfileStore + flow_id  + filename
                # establish the flow id and place the header records in a data frame + file contents + etlchange timestamp as additional columns into {dataFlow flowId)
                df_flowid = df_flowid.append({'flow_id': flow_id, 'row_1': row_1, 'row_2': row_2, 'filecontents': filecontents, 'etlchange': etlchange}, ignore_index=True)
                print(keypath)
                copy_source = {
                                'Bucket': self.bucket_name,
                                'Key': fkey
                              }

                # archive File
                #s31.copy(copy_source, self.bucket_name, keypath)
                # break
                # upload to s3
                s31.put_object(Bucket=self.bucket_name, Key=keypath, Body=worddoc)
                #der = df_flowid.head(10)

            # Write the DataFrame to redshift
            pr.pandas_to_redshift(data_frame = df_flowid, redshift_table_name = 'public.testtable', append = True )
            pr.close_up_shop()

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
    p = ExtractEnsekFiles("outbound")
    ef_keys_s3 = p.get_keys_from_s3(s3)

    #Ensek Internal Estimates Ensek Extract
    print("{0}: Ensek flows extract Jobs running...".format(datetime.now().strftime('%H:%M:%S')))
    #print(ef_keys_s3)
    p.process_flow_data(ef_keys_s3)




