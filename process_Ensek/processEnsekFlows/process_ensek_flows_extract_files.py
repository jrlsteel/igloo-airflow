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
import concurrent.futures
#from concurrent.futures import ProcessPoolExecutor
from queue import Queue

import sys
import os

sys.path.append('..')

from common import utils as util
from conf import config as con
from connections.connect_db import get_boto_S3_Connections as s3_con
from connections import connect_db as db



class ExtractEnsekFiles(multiprocessing.Process):

    def __init__(self, _type, ef_keys_s3, output_queue):
        multiprocessing.Process.__init__(self)
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
        self.ef_keys_s3 = ef_keys_s3
        self.output_queue = output_queue

    def process_flow_data(self, fkey):
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
            #for fkey in flow_keys:
            print(fkey)
            obj = s31.get_object(Bucket=bucket_name, Key=fkey)
            obj_str = obj['Body'].read().decode('utf-8').splitlines(True)
            obj_str2 = obj_str[0]

            filename = fkey.replace(self.EFStartAfter, '')

            fileUFF_csv = filename.replace(self.suffix, '_UFF.csv')
            file_content = []
            row_counter = 0
            for lines in obj_str:
                file_content.append(lines)
                row_counter += 1
            flow_id = file_content[0].replace('\n', '').replace('|', ',').split(',')[2]
            row_1 = file_content[0]
            row_2 = file_content[1]
            row_footer = file_content[(row_counter - 1)]
            file_n = filename.replace('/', '')
            etlchange = self.now
            """
            worddoc =''
            for wd in file_content:
                worddoc += ''.join(wd) 
            """
            objlen = len(obj_str)
            i=0
            worddoc = ''
            while i < objlen:
                worddoc += obj_str[i]  #.replace("\n", "--")
                i += 1

            filecontents = worddoc
            content_cnt = int(round(len(filecontents)))
            # upload to s3
            keypath = self.EFfileStore + flow_id  + filename
            filecontents_url = "s3://igloo-data-warehouse-uat/stage1Flows/inbound/master" + filename
            # establish the flow id and place the header records in a data frame + file contents + etlchange timestamp as additional columns into {dataFlow flowId)
            #df_flowid = pd.DataFrame()
            #if file_n[:4] != 'D019':
            if self.type == 'inbound':
                filecontents = filecontents_url
            else:
                filecontents = filecontents
            df_flowid = df_flowid.append({'filename': file_n,'flow_id': flow_id, 'row_1': row_1, 'row_2': row_2, 'row_footer': row_footer, 'filecontents': filecontents, 'etlchange': etlchange}, ignore_index=True)
            # self.redshift_upsert(df=df_flowid, crud_type='i')

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

            # Write the DataFrame to redshift
            #pr.pandas_to_redshift(data_frame = df_flowid, redshift_table_name = 'public.testtable2')
            #self.redshift_upsert(df=df_flowid, crud_type='i')
            print(df_flowid.head(5))
            self.output_queue.put(S3WriteTask(df_flowid))

        except Exception as e:
            print(" Error :" + str(e))
            sys.exit(1)


    def redshift_upsert(self, sql=None, df=None, crud_type=None):
        '''
        This function gets connection to RedShift Database.
        :param sql: the sql to run
        '''

        table_name = 'public.' + self.outputTable
        pr = db.get_redshift_connection()
        if crud_type == 'i':
            pr.pandas_to_redshift(df, table_name, index=None, append=True)
        if crud_type in ('u', 'd'):
            pr.exec_commit(sql)
        pr.close_up_shop()


    def run(self):
        while True:
            if self.ef_keys_s3.empty():
                # print(self.name + " sleeping")
                sleep(1)
            else:
                key_task = self.ef_keys_s3.get()
                if key_task is None:
                    self.ef_keys_s3.task_done()
                    break
                else:
                    self.process_flow_data(key_task)
                    self.ef_keys_s3.task_done()
                    # print(self.name + " writing")
        print(self.name + " exiting")



class GetKeys(multiprocessing.Process):

    def __init__(self, _type):
        multiprocessing.Process.__init__(self)
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
        #self.ef_keys_s3 = ef_keys_s3

    def get_keys_from_s3(self, s3):
        """
        This function gets only maximum of 1000 keys per request.
        :param s3: holds the s3 connection
        :return: list of Ensek Flow filenames
        """
        s3client = s3
        bucket = self.bucket_name
        q = multiprocessing.JoinableQueue()
        #flow_keys = []
        EFprefix= self.EFStartAfter
        theobjects = s3client.list_objects_v2(Bucket=bucket, StartAfter=EFprefix)
        for object in theobjects['Contents']:
            if object['Key'].lower().endswith(self.suffix):
                keyString = object["Key"]
                q.put(keyString)
        return q


    def get_keys_from_s3_page(self):
        """
        This function gets all the keys that needs to be processed.
        :param s3: holds the s3 connection
        :return: list of filenames
        """
        s3client = s3
        bucket = self.bucket_name
        q = multiprocessing.JoinableQueue()
        #flow_keys = []
        EFprefix= self.EFStartAfter
        paginator = s3client.get_paginator("list_objects")
        page_iterator = paginator.paginate(Bucket=bucket, Prefix=EFprefix)
        for page in page_iterator:
            if "Contents" in page:
                for key in page[ "Contents" ]:
                    if key[ "Key" ].lower().endswith(self.suffix): # and key[ "Key" ].startswith(EFprefix):
                        keyString = key[ "Key" ]
                        q.put(keyString)
        """
        flow_keys = list(q.queue)
        # Empty queue
        with q.mutex:
            q.queue.clear()
        """
        return q


class S3WriteTask:
    def __init__(self, dataframe):
        self.dataframe = dataframe

class S3Writer(multiprocessing.Process):
    def __init__(self, _type, write_queue, s3_conn, s3_dir):
        multiprocessing.Process.__init__(self)
        self.write_queue = write_queue
        self.s3_conn = s3_conn
        self.s3_dir = s3_dir
        self.type = _type
        self.outputTable = self.s3_dir['s3_ensekflow_key'][self.type]['outputTable']

    def run(self):
        while True:
            if self.write_queue.empty():
                # print(self.name + " sleeping")
                sleep(1)
            else:
                write_task = self.write_queue.get()
                if write_task is None:
                    self.write_queue.task_done()
                    break
                else:
                    self.write_queue.task_done()
                    # print(self.name + " writing")
        print(self.name + " exiting")


if __name__ == '__main__':

    freeze_support()

    dir_s3 = util.get_dir()
    bucket_name = dir_s3['s3_bucket']

    s3 = db.get_S3_Connections_client()
    flowtype = ""
    prc = 2
    while prc <= 2:
        if prc == 1:
            flowtype = "outbound"
        else:
            flowtype = "inbound"

        # Ensek Internal Estimates Ensek Extract
        print("{0}: Ensek flows {1} extract Jobs running...".format(datetime.now().strftime('%H:%M:%S'), flowtype))

        # p.process_flow_data(ef_keys_s3) ##### Enable this to test without multiprocessing

        ######### multiprocessing starts  ##########
        env = util.get_env()
        # total_processes = util.get_multiprocess('total_ensek_processes')

        max_ensek_callers = util.get_multiprocess('ensek_flow_processes')
        max_total_threads = util.get_multiprocess('max_total_threads')  # 48 is the limit but 1 taken up by the main execution thread

        n_callers = max_ensek_callers
        n_writers = max_total_threads - n_callers

        # Establish communication queues
        write_queue = multiprocessing.JoinableQueue()
        output_queue = multiprocessing.JoinableQueue()

        p2 = GetKeys(flowtype)

        print("Extracting Keys.....")
        #ef_keys_s3 = p2.get_keys_from_s3_page()
        ef_keys_s3 = p2.get_keys_from_s3(s3)
        #noKeys = ef_keys_s3.qsize()

        p = [ExtractEnsekFiles(flowtype, ef_keys_s3, output_queue)
             for i in range(max_ensek_callers)]

        # Extract all keys required
        start = timeit.default_timer()
        print("Extracting Keys.....")


        for t in p:
            t.start()

        """
        # Start writers
        writers = [S3Writer(flowtype, output_queue, s3_con(bucket_name), dir_s3)
                   for i in range(n_writers)]
        for w in writers:
            w.start()
        """

        # Enqueue jobs
        while not ef_keys_s3.empty():
            #print("Keys in queue: {0}".format(ef_keys_s3.qsize()))
            sleep(10)



        df_flow_app = pd.DataFrame()
        while not output_queue.empty():
            dataframe = output_queue.get().dataframe
            output_queue.task_done()
            df_flow_app.append(dataframe)
        p[0].redshift_upsert(df=df_flow_app, crud_type='i')

        # Add a poison pill for each writer
        for i in range(max_ensek_callers):
            ef_keys_s3.put(None)

        # wait for writers to close
        ef_keys_s3.join()



        print("Process completed in " + str(timeit.default_timer() - start) + ' seconds')


        prc += 1
