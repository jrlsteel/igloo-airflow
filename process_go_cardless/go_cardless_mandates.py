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


class GoCardlessMandates(object):

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
        self.filename = 'go_cardless_Mandates_' + '{:%Y-%m-%d}'.format(self.execDate) + '.csv'
        self.noDays = noDays
        self.fileDirectory = self.dir['s3_finance_goCardless_key']['Mandates']

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

    def process_Mandates(self):
        bucket_name = self.bucket_name
        execStartDate = '{:%Y-%m-%d}'.format(self.execDate) + "T00:00:00.000Z"
        execEndDate = self.get_date() + "T00:00:00.000Z"
        s3 = self.s3
        dir_s3 = self.dir
        fileDirectory = self.fileDirectory

        client = gocardless_pro.Client(access_token=con.go_cardless['access_token'],
                                       environment=con.go_cardless['environment'])
        # Loop through a page
        q = Queue()
        datalist = []

        # Fetch a mandate by its ID
        mandate = client.mandates

        # Loop through a page of payments, printing each payment's amount
        print('.....listing mandates')
        df_mandate = pd.DataFrame()
        for mandate in client.mandates.all(
                params={"created_at[gte]": "2020-01-01T00:00:00.000Z", "created_at[lte]": "2020-03-01T00:00:00.000Z"}):
            EnsekAccountId = ''
            StatementId = ''
            print(mandate.id)
            if 'AccountId' in mandate.metadata:
                EnsekAccountId = mandate.metadata['AccountId']
            if 'StatementId' in mandate.metadata:
                StatementId = mandate.metadata['StatementId']

            mandate_id = mandate.id
            CustomerId = mandate.links.customer
            new_mandate_id = mandate.links.new_mandate
            created_at = mandate.created_at
            next_possible_charge_date = mandate.next_possible_charge_date
            payments_require_approval = mandate.payments_require_approval
            reference = mandate.reference
            scheme = mandate.scheme
            status = mandate.status
            creditor = mandate.links.creditor
            customer_bank_account = mandate.links.customer_bank_account
            EnsekID = EnsekAccountId
            EnsekStatementId = StatementId

            listRow = [mandate_id, CustomerId, new_mandate_id, created_at, next_possible_charge_date,
                       payments_require_approval,
                       reference, scheme, status, creditor, customer_bank_account, EnsekID, EnsekStatementId]
            q.put(listRow)

        while not q.empty():
            datalist.append(q.get())
        df = pd.DataFrame(datalist, columns=['mandate_id', 'CustomerId', 'new_mandate_id', 'created_at',
                                             'next_possible_charge_date',
                                             'payments_require_approval', 'reference', 'scheme', 'status', 'creditor',
                                             'customer_bank_account',
                                             'EnsekID', 'EnsekStatementId'])

        df_string = df.to_csv(None, index=False)
        # print(df_account_transactions_string)

        s3.key = fileDirectory + os.sep + self.s3key + os.sep + self.filename
        print(s3.key)
        s3.set_contents_from_string(df_string)


if __name__ == "__main__":
    freeze_support()
    s3 = db.get_S3_Connections_client()

    p = GoCardlessMandates('2020-01-01', 1)

    p.process_Mandates()