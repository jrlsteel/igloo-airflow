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
import queue


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
        self.outputTable = self.dir['s3_ensekflow_key'][self.type]['outputTable']

    def process_flow_data(self, flow_keys):
        bucket_name = self.bucket_name

        """
        :param ensekFlows_keys: list of files stored in s3
        :return: None

        This function copies the inbound and outbound data that is being loaded into stage1Flows folder and
         generates the {FlowId} from the 3rd field in the header row
            1. Interrogate the files and establish the flow id and place the header records in a 
               data frame + file contents + etlchange timestamp as additional columns into {dataFlow flowId) named dataframe
            2. Copy file to {FlowId} sub-directory.
            3. When the last file is processed insert data frame into appropriate table this should include 
                        -- header records  (first 2)
                        -- file contents column
                        -- and  include an etlchange (timestamp) as the last.
            4. Move files from those sub-directories to {archive} directories of the same structure.
            5. Delete file from parent sub-directory 
        """

        try:
            s31 = db.get_S3_Connections_client()

            # loop each file
            df_flowid = pd.DataFrame()
            for fkey in flow_keys:
                #print(fkey)
                obj = s31.get_object(Bucket=bucket_name, Key=fkey)
                obj_str = obj['Body'].read().decode('ISO-8859-1').splitlines(True)


                filename = fkey.replace(self.EFStartAfter, '')

                fileUFF_csv = filename.replace(self.suffix, '_UFF.csv')
                file_content = []
                row_counter = 0
                for lines in obj_str:
                    file_content.append(lines)
                    row_counter += 1
                flow_id = file_content[0].replace('\n', '').replace('|', ',').split(',')[2].replace('"', '')
                row_1 = file_content[0].replace('"', '')
                row_2 = file_content[1].replace('"', '')
                row_footer = file_content[(row_counter - 1)].replace('"', '')
                file_n = filename.replace('/', '')
                etlchange = self.now
                """
                worddoc =''
                for wd in file_content:
                    worddoc += ''.join(wd) 
                """
                objlen = len(obj_str)
                i = 0
                worddoc = ''
                while i < objlen:
                    worddoc += obj_str[i]  # .replace("\n", "--")
                    i += 1

                filecontents = worddoc
                content_cnt = int(round(len(filecontents)))
                # upload to s3
                keypath = self.EFfileStore + flow_id + filename
                filecontents_url = "https://igloo-data-warehouse-uat.s3-eu-west-1.amazonaws.com/stage1Flows/inbound/master" + filename
                # establish the flow id and place the header records in a data frame + file contents + etlchange timestamp as additional columns into {dataFlow flowId)
                # df_flowid = pd.DataFrame()
                # if file_n[:4] != 'D019':
                if content_cnt > 4000.0:
                    filecontents = filecontents_url
                else:
                    filecontents = filecontents
                #if flow_id == 'D0058001':
                df_flowid = df_flowid.append(
                    {'filename': file_n, 'flow_id': flow_id, 'row_1': row_1, 'row_2': row_2, 'row_footer': row_footer,
                     'filecontents': filecontents, 'etlchange': etlchange}, ignore_index=True)
                # self.redshift_upsert(df=df_flowid, crud_type='i')

                #print(keypath)
                copy_source = {
                    'Bucket': self.bucket_name,
                    'Key': fkey
                }

                # archive File
                # s31.copy(copy_source, self.bucket_name, keypath)
                # break
                # upload to s3
                s31.put_object(Bucket=self.bucket_name, Key=keypath, Body=worddoc)

            # Write the DataFrame to redshift
            # pr.pandas_to_redshift(data_frame = df_flowid, redshift_table_name = 'public.testtable2')
            # df_flowid
            # self.redshift_upsert(df=df_flowid, crud_type='i')
            return df_flowid

        except Exception as e:
            print(" Error :" + str(e))
            sys.exit(1)

    def f_put(self, flow_keys, q):
        print("f_put start")
        data = self.process_flow_data(flow_keys)
        q.put(data)

    def f_get(self, q):
        #print("f_get start")
        while (1):
            data = pd.DataFrame()
            loop = 0
            while not q.empty():
                data = q.get()
                print("get (loop: %s)" % loop)
                time.sleep(0)
                loop += 1
            time.sleep(1.)
            # self.redshift_upsert(df=data, crud_type='i')

    def get_keys_from_s3(self, s3):
        """
        This function gets only maximum of 1000 keys per request.
        :param s3: holds the s3 connection
        :return: list of Ensek Flow filenames
        """
        s3client = s3
        bucket = self.bucket_name
        flow_keys = []
        EFprefix = self.EFStartAfter
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
        # flow_keys = []
        EFprefix = self.EFStartAfter
        paginator = s3client.get_paginator("list_objects")
        page_iterator = paginator.paginate(Bucket=bucket, Prefix=EFprefix)
        for page in page_iterator:
            if "Contents" in page:
                for key in page["Contents"]:
                    if key["Key"].lower().endswith(self.suffix):  # and key[ "Key" ].startswith(EFprefix):
                        keyString = key["Key"]
                        q.put(keyString)
        flow_keys = list(q.queue)
        # Empty queue
        with q.mutex:
            q.queue.clear()
        return flow_keys

    def redshift_upsert(self, sql=None, df=None, crud_type=None):
        '''
        This function gets connection to RedShift Database.
        :param sql: the sql to run
        '''
        try:
            table_name = 'public.' + self.outputTable
            pr = db.get_redshift_connection()
            if crud_type == 'i':
                pr.pandas_to_redshift(df, table_name, index=None, append=True)
            if crud_type in ('u', 'd'):
                pr.exec_commit(sql)
            pr.close_up_shop()
        except Exception as e:
            return e



class IterableQueue():
    def __init__(self,source_queue):
            self.source_queue = source_queue
    def __iter__(self):
        while True:
            try:
               yield self.source_queue.get_nowait()
            except queue.Empty:
               return



if __name__ == '__main__':

    freeze_support()
    s3 = db.get_S3_Connections_client()
    flowtype = ""
    prc = 1
    while prc <= 2:
        if prc == 1:
            flowtype = "outbound"
        else:
            flowtype = "inbound"

        pkf = ExtractEnsekFiles(flowtype)
        itr = IterableQueue
        # Extract all keys required

        start = timeit.default_timer()
        print("Extracting Keys.....")
        ef_keys_s3 = pkf.get_keys_from_s3_page()
        # Test with 1000 records
        # ef_keys_s3 = pkf.get_keys_from_s3(s3)
        print(len(ef_keys_s3))

        # Ensek Internal Estimates Ensek Extract
        print("{0}: Ensek flows {1} extract Jobs running...".format(datetime.now().strftime('%H:%M:%S'), flowtype))

        # p.process_flow_data(ef_keys_s3) ##### Enable this to test without multiprocessing

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

        # start = timeit.default_timer()

        m = multiprocessing.Manager()
        q = m.Queue()
        q2 = m.Queue()
        for i in range(n + 1):
            pkf1 = ExtractEnsekFiles(flowtype)
            print(i)
            uv = i * k
            if i == n:
                # print(ef_keys_s3[lv:])
                t = multiprocessing.Process(target=pkf1.f_put, args=(ef_keys_s3[lv:], q))
            else:
                # print(ef_keys_s3[lv:uv])
                t = multiprocessing.Process(target=pkf1.f_put, args=(ef_keys_s3[lv:uv], q))
            lv = uv

            processes.append(t)

        data_df = pd.DataFrame()
        for p in processes:
            p.start()
            sleep(2)

        for process in processes:
            while not q.empty():
                data = q.get()
                sleep(5)
                q2.put(data)
            process.join()

        # Completed Parallel Processes
        print(q2.qsize())

        # Write the DataFrame to redshift
        for n in IterableQueue(q2):
            pkf.redshift_upsert(df=n ,crud_type='i')
        ####### multiprocessing Ends #########

        print("Process completed in " + str(timeit.default_timer() - start) + ' seconds')

        prc += 1
