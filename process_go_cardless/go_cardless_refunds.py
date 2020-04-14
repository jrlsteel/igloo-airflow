import timeit

import boto3
import glob
import pandas as pd
import numpy as np
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
from datetime import datetime, date, time, timedelta
import math

import os
import gocardless_pro
from queue import Queue
import queue
import requests

import sys
import os

sys.path.append('..')

from common import utils as util
from conf import config as con
from connections.connect_db import get_boto_S3_Connections as s3_con
from connections import connect_db as db


class GoCardlessRefunds(object):

    def __init__(self, execDate=datetime.now(), noDays=1):
        self.env = util.get_env()
        self.dir = util.get_dir()
        self.bucket_name = self.dir['s3_finance_bucket']
        self.s3 = s3_con(self.bucket_name)
        self.now = datetime.now()
        self.execDate = datetime.strptime(execDate, '%Y-%m-%d')
        self.qtr = math.ceil(self.execDate.month / 3.)
        self.yr = math.ceil(self.execDate.year)
        self.s3key = 'timestamp=' + str(self.yr) + '-Q' + str(self.qtr)
        self.filename = 'go_cardless_Refunds_' + '{:%Y-%m-%d}'.format(self.execDate) + '.csv'
        self.noDays = noDays
        self.fileDirectory = self.dir['s3_finance_goCardless_key']['Refunds']

    def is_json(myjson):
        try:
            json_object = json.loads(myjson)
        except ValueError as e:
            return False
        return True

    def get_date(self, dateFormat="%Y-%m-%d"):
        dateStart = self.execDate
        addDays = self.noDays
        if (addDays != 0):
            dateEnd = dateStart + timedelta(days=addDays)
        else:
            dateEnd = dateStart

        return dateEnd.strftime(dateFormat)

    def process_Refunds(self):
        bucket_name = self.bucket_name
        execStartDate = '{:%Y-%m-%d}'.format(self.execDate) + "T00:00:00.000Z"
        execEndDate = self.get_date() + "T00:00:00.000Z"
        s3 = self.s3
        dir_s3 = self.dir
        fileDirectory = self.fileDirectory

        client = gocardless_pro.Client(access_token=con.go_cardless['access_token'],
                                       environment=con.go_cardless['environment'])

        # Loop through a page of payments, printing each payment's amount
        q = Queue()
        datalist = []
        refund = client.refunds

        # Loop through a page of payments, printing each payment's amount
        df = pd.DataFrame()
        print('.....listing refunds')
        for refund in client.refunds.all(
                params={"created_at[gte]": execStartDate, "created_at[lte]": execEndDate }):
            EnsekAccountId = ''
            if 'AccountId' in refund.metadata:
                EnsekAccountId = refund.metadata['AccountId']

            print(refund.id)
            id = refund.id
            amount = refund.amount
            created_at = refund.created_at
            currency = refund.currency
            reference = refund.reference
            status = refund.status
            metadata = refund.metadata
            payment = refund.links.payment
            mandate = refund.links.mandate
            EnsekID = EnsekAccountId

            listRow = [id, amount, created_at, currency, reference, status, metadata,
                       payment, mandate, EnsekID]
            q.put(listRow)

            while not q.empty():
                datalist.append(q.get())

            df = pd.DataFrame(datalist, columns=['id', 'amount', 'created_at', 'currency',
                                                 'reference', 'status', 'metadata',
                                                 'payment', 'mandate', 'EnsekID'])

            df_string = df.to_csv(None, index=False)
            # print(df_account_transactions_string)

            s3.key = fileDirectory + os.sep + self.s3key + os.sep + self.filename
            print(s3.key)
            s3.set_contents_from_string(df_string)


if __name__ == "__main__":
    freeze_support()
    s3 = db.get_S3_Connections_client()

    p = GoCardlessRefunds('2020-01-01', 1)

    p.process_Refunds()
