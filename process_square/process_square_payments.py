import os
import boto3
from square.client import Client
from square.configuration import Configuration
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

client = Client(access_token=con.square['access_token'],
                    environment=con.square['environment'], )
payments_api = client.payments


class PaymentsApi(object):

    def __init__(self, execStartDate, execEndDate):
        self.env = util.get_env()
        self.dir = util.get_dir()
        self.bucket_name = self.dir['s3_finance_bucket']
        self.s3 = s3_con(self.bucket_name)
        self.fileDirectory = self.dir['s3_finance_square_key']['Payments']
        self.payments_api = client.payments
        self.execStartDate = datetime.strptime(execStartDate, '%Y-%m-%d')
        self.execEndDate = datetime.strptime(execEndDate, '%Y-%m-%d')
        self.qtr = math.ceil(self.execStartDate.month / 3.)
        self.yr = math.ceil(self.execStartDate.year)
        self.fkey = 'timestamp=' + str(self.yr) + '-Q' + str(self.qtr) + '/'
        self.filename = 'square_payments_' + '{:%Y%m}'.format(self.execStartDate) + '_' + '{:%Y%m}'.format(self.execEndDate) + '.csv'

    def get_date(self, _date, dateFormat="%Y-%m-%d"):
        dateStart = _date
        dateStart = datetime.strptime(dateStart, '%Y-%m-%d')
        addDays = 1  ###self.noDays
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

    def transactions(self, _StartDate, _EndDate):
        StartDate = _StartDate + "T00:00:00.000Z"
        EndDate = _EndDate + "T00:00:00.000Z"
        # print(StartDate, EndDate)
        pay = ''
        result = payments_api.list_payments(
            begin_time=StartDate,
            end_time=EndDate,
            sort_order='DESC',
            cursor=None,
            location_id=None,
            total=None,
            last_4=None,
            card_brand=None)
        if result.is_success():
            pay = result.body
        elif result.is_error():
            pay = result.errors
        return pay

    def Normalise_payments(self):
        fileDirectory = self.fileDirectory
        s3 = self.s3
        # Loop through a page
        q = Queue()
        df_out = pd.DataFrame()
        datalist = []
        ls = []
        for single_date in self.daterange():
            # print(single_date)
            start = single_date
            end = self.get_date(start)
            print(start, end)
            k1 = self.transactions(start, end)
            ## print JSON
            print(k1)
            if k1.get("payments"):
                df = pd.DataFrame.from_dict(json_normalize(k1, record_path=['payments']))
                k2 = df.loc[:, df.columns.isin(['status', 'amount_money', 'note', 'created_at', ])]
                '''
                if 'note' in df.columns:
                    k2 = df[['status', 'amount_money', 'note', 'created_at']]
                else:
                    k2 = df[['status', 'amount_money', 'created_at']]
                '''
                # print(k2.head(5))
                for row in k2.itertuples(index=True, name='Pandas'):
                    EnsekID = None
                    currency = None
                    amount = None
                    status = getattr(row, "status")
                    if 'amount_money' in k2.columns:
                        amount_money = getattr(row, "amount_money")
                        if amount_money['amount']:
                            amount = amount_money['amount']
                        if amount_money['currency']:
                            currency = amount_money['currency']
                    if 'note' in k2.columns:
                        EnsekID = getattr(row, "note")
                    created_at = getattr(row, "created_at")
                    listRow = [status, currency, amount, EnsekID, created_at]
                    q.put(listRow)

        while not q.empty():
            datalist.append(q.get())
        df_out = pd.DataFrame(datalist, columns=['status', 'currency', 'amount', 'EnsekID', 'created_at'])

        print(df_out[['currency', 'amount', 'EnsekID']].head(200))

        ### WRITE TO CSV
        #df_out.to_csv('square_payments.csv', encoding='utf-8', index=False)
        '''
        df_string = df_out.to_csv(None, index=False) 

        s3.key = fileDirectory + self.fkey + self.filename
        print(s3.key)
        s3.set_contents_from_string(df_string)
        '''



    def sampletest(self):
        for single_date in self.daterange():
            print(single_date)


if __name__ == "__main__":
    freeze_support()
    s3 = db.get_finance_S3_Connections_client()
    ### StartDate & EndDate in YYYY-MM-DD format ###
    p = PaymentsApi('2020-04-01', '2020-04-24')

    p1 = p.Normalise_payments()
    #print(p1[['EnsekID', 'status', 'amount', 'created_at']])






