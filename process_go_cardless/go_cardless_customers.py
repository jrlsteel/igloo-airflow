import os
import gocardless_pro
import timeit

import pandas as pd
import numpy as np
import requests
import json
from multiprocessing import freeze_support
from datetime import datetime, date, time, timedelta
import math

from queue import Queue
from pandas.io.json import json_normalize
from pathlib import Path

import sys

sys.path.append('..')

from common import utils as util
from conf import config as con
from connections.connect_db import get_finance_S3_Connections as s3_con
from connections import connect_db as db

client = gocardless_pro.Client(access_token=con.go_cardless['access_token'],
                               environment=con.go_cardless['environment'])
clients = client.customers

class GoCardlessClients(object):

    def __init__(self, _execStartDate=None, _execEndDate=None):
        self.env = util.get_env()
        self.dir = util.get_dir()
        self.bucket_name = self.dir['s3_finance_bucket']
        self.s3 = s3_con(self.bucket_name)
        self.fileDirectory = self.dir['s3_finance_goCardless_key']['Clients']
        self.clients = clients
        self.toDay = datetime.today().strftime('%Y-%m-%d')
        if _execStartDate is None:
            _execStartDate = self.get_date(self.toDay, _addDays=-1)
        self.execStartDate = datetime.strptime(_execStartDate, '%Y-%m-%d')
        if _execEndDate is None:
            _execEndDate = self.toDay
        self.execEndDate = datetime.strptime(_execEndDate, '%Y-%m-%d')

    def is_json(self, myjson):
        try:
            json_object = json.loads(myjson)
        except ValueError as e:
            return False
        return True

    def get_date(self, _date, _addDays=None, dateFormat="%Y-%m-%d"):
        dateStart = _date
        dateStart = datetime.strptime(dateStart, '%Y-%m-%d')
        if _addDays is None:
            _addDays = 1  ###self.noDays
        addDays = _addDays
        if (addDays != 0):
            dateEnd = dateStart + timedelta(days=addDays)
        else:
            dateEnd = dateStart

        return dateEnd.strftime(dateFormat)

    def daterange(self, dateFormat="%Y-%m-%d"):
        start_date = self.execStartDate
        end_date = self.execEndDate
        for n in range(int((end_date - start_date).days)):
            seq_date = start_date + timedelta(n)
            yield seq_date.strftime(dateFormat)
        return seq_date


    def process_Clients(self, _StartDate=None, _EndDate=None):
        fileDirectory = self.fileDirectory
        s3 = self.s3
        if _StartDate is None:
            _StartDate = '{:%Y-%m-%d}'.format(self.execStartDate)
        if _EndDate is None:
            _EndDate = '{:%Y-%m-%d}'.format(self.execEndDate)
        startdatetime = datetime.strptime(_StartDate, '%Y-%m-%d')
        clients = self.clients
        filename = 'go_cardless_clients_' + _StartDate + '_' + _EndDate + '.csv'
        qtr = math.ceil(startdatetime.month / 3.)
        yr = math.ceil(startdatetime.year)
        s3key = 'timestamp=' + str(yr) + '-Q' + str(qtr)
        print('Listing clients.......')
        # Loop through a page
        q = Queue()
        df_out = pd.DataFrame()
        datalist = []
        ls = []
        StartDate = _StartDate + "T00:00:00.000Z"
        EndDate = _EndDate + "T00:00:00.000Z"
        print(_StartDate, _EndDate)

        for client in clients.all(
                params={"created_at[gte]": StartDate , "created_at[lte]": EndDate }):
            EnsekAccountId = None
            if client.metadata:
                if 'ensekAccountId' in client.metadata:
                    EnsekAccountId = client.metadata['ensekAccountId']
            ## print(client.id)
            client_id = client.id
            created_at = client.created_at
            email = client.email
            given_name = client.given_name
            family_name = client.family_name
            company_name = client.company_name
            country_code = client.country_code
            EnsekID = EnsekAccountId
            listRow = [client_id, created_at, email, given_name, family_name, company_name, country_code, EnsekID]
            q.put(listRow)
            data = q.queue
        for d in data:
            datalist.append(d)
        # Empty queue
        with q.mutex:
            q.queue.clear()

        df = pd.DataFrame(datalist, columns=['client_id', 'created_at', 'email', 'given_name', 'family_name',
                                             'company_name', 'country_code', 'EnsekID'])

        print(df.head(5))
        df_string = df.to_csv(None, index=False)
        # print(df_account_transactions_string)

        ## s3.key = fileDirectory + os.sep + s3key + os.sep + filename
        s3.key = Path(fileDirectory, s3key, filename)
        print(s3.key)
        s3.set_contents_from_string(df_string)

        return df

    def runDailyFiles(self):
        for single_date in self.daterange():
            start = single_date
            end = self.get_date(start)
            ## print(start, end)
            ### Execute Job ###
            self.process_Clients(start, end)

if __name__ == "__main__":
    freeze_support()
    s3 = db.get_finance_S3_Connections_client()
    ### StartDate & EndDate in YYYY-MM-DD format ###
    ### When StartDate & EndDate is not provided it defaults to SysDate and Sysdate + 1 respectively ###
    ### 2019-05-29 2019-05-30 ###
    p = GoCardlessClients('2020-04-01', '2020-07-01')
    ##p = GoCardlessClients()

    p1 = p.process_Clients()
    ### Extract return single Daily Files from Date Range Provided ###
    ##p2 = p.runDailyFiles()


