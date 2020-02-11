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
from time import sleep
from multiprocessing import freeze_support
from datetime import datetime
import pandas_redshift as pr
from concurrent.futures import ProcessPoolExecutor
from queue import Queue

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
        :param ensekFlows_keys: list of D18 files stored in s3
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
                row_counter = 0
                for lines in obj_str:
                    line_rep = lines.replace('\n', '').replace('|', ',')
                    line_sp = line_rep.split(',')
                    #file_content2.append(line_sp)
                    file_content.append(lines)
                    row_counter += 1
                #flow_id = file_content2[0][2]
                flow_id = file_content[0].replace('\n', '').replace('|', ',').split(',')[2]
                row_1 = file_content[0]
                row_2 = file_content[1]
                row_footer = file_content[(row_counter - 1)]
                file_n = filename.replace('/', '')
                etlchange = self.now
                worddoc =''
                for wd in file_content:
                    worddoc += ''.join(wd) #+ '\n'
                filecontents = worddoc
                # upload to s3
                keypath = self.EFfileStore + flow_id  + filename
                # establish the flow id and place the header records in a data frame + file contents + etlchange timestamp as additional columns into {dataFlow flowId)
                df_flowid = df_flowid.append({'filename': file_n,'flow_id': flow_id, 'row_1': row_1, 'row_2': row_2, 'row_footer': row_footer, 'filecontents': filecontents, 'etlchange': etlchange}, ignore_index=True)

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
            #pr.pandas_to_redshift(data_frame = df_flowid, redshift_table_name = 'public.testtable2')
            self.redshift_upsert(df=df_flowid, crud_type='i')

        except Exception as e:
            print(" Error :" + str(e))
            print(file_n)
            #sys.exit(1)



    def get_keys_from_s3(self, s3):
        """
        This function gets only maximum of 1000 keys per request.
        :param s3: holds the s3 connection
        :return: list of Ensek Flow filenames
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



    def get_keys_from_s3_page(self):
        """
        This function gets all the keys that needs to be processed.
        :param s3: holds the s3 connection
        :return: list of filenames
        """
        s3client = s3
        bucket = self.bucket_name
        q = Queue()
        #flow_keys = []
        EFprefix= self.EFStartAfter
        paginator = s3client.get_paginator("list_objects")
        page_iterator = paginator.paginate(Bucket=bucket, Prefix=EFprefix)
        for page in page_iterator:
            if "Contents" in page:
                for key in page[ "Contents" ]:
                    if key[ "Key" ].endswith(self.suffix): # and key[ "Key" ].startswith(EFprefix):
                        keyString = key[ "Key" ]
                        q.put(keyString)
        flow_keys = list(q.queue)
        # Empty queue
        with q.mutex:
            q.queue.clear()
        return flow_keys



    def redshift_upsert(sql=None, df=None, crud_type=None):
        '''
        This function gets connection to RedShift Database.
        :param sql: the sql to run
        '''
        try:
            table_name = 'public.ref_dataflows_inbound'
            pr = db.get_redshift_connection()
            if crud_type == 'i':
                pr.pandas_to_redshift(df, table_name, index=None, append=True)
            if crud_type in ('u', 'd'):
                pr.exec_commit(sql)
            pr.close_up_shop()
        except Exception as e:
            return e



if __name__ == '__main__':

    freeze_support()
    s3 = db.get_S3_Connections_client()
    p = ExtractEnsekFiles("inbound")
    # Extract all keys required

    start = timeit.default_timer()
    print("Extracting Keys.....")
    #ef_keys_s3 = p.get_keys_from_s3_page()
    # Test with 1000 records
    ef_keys_s3 = p.get_keys_from_s3(s3)

    print(len(ef_keys_s3))
    #Ensek Internal Estimates Ensek Extract
    print("{0}: Ensek flows extract Jobs running...".format(datetime.now().strftime('%H:%M:%S')))

    #p.process_flow_data(ef_keys_s3) ##### Enable this to test without multiprocessing
    ######### multiprocessing starts  ##########
    env = util.get_env()
    if env == 'uat':
        n = 12  # number of process to run in parallel
    else:
        n = 12
    print(len(ef_keys_s3))
    k = int(len(ef_keys_s3) / n)  # get equal no of files for each process
    print(k)
    processes = []
    lv = 0

    #start = timeit.default_timer()

    for i in range(n+1):
        p1 = ExtractEnsekFiles("inbound")
        print(i)
        uv = i * k
        if i == n:
            # print(ef_keys_s3[l:])
            t = multiprocessing.Process(target=p1.process_flow_data, args=(ef_keys_s3[lv:],))
        else:
            # print(ef_keys_s3[l:u])
            t = multiprocessing.Process(target=p1.process_flow_data, args=(ef_keys_s3[lv:uv],))
        lv = uv

        processes.append(t)

    for p in processes:
        p.start()
        sleep(2)

    for process in processes:
        process.join()
    ####### multiprocessing Ends #########

    print("Process completed in " + str(timeit.default_timer() - start) + ' seconds')