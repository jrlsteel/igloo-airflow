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


class GoCardlessPayments(object):

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
        self.filename = 'go_cardless_Payments_' + '{:%Y-%m-%d}'.format(self.execDate) + '.csv'
        self.noDays = noDays
        self.fileDirectory = self.dir['s3_finance_goCardless_key']['Payments']

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

    def process_Payments(self):
        bucket_name = self.bucket_name
        execStartDate = '{:%Y-%m-%d}'.format(self.execDate)
        execEndDate = self.get_date()
        s3 = self.s3
        dir_s3 = self.dir
        fileDirectory = self.fileDirectory

        client = gocardless_pro.Client(access_token=con.go_cardless['access_token'], environment=con.go_cardless['environment'])
        # Loop through a page of payments, printing each payment's amount
        q = Queue()
        datalist = []

        # Fetch a payment by its ID
        payment = client.payments

        # Loop through a page of payments, printing each payment's amount
        df = pd.DataFrame()
        print('.....listing payments')
        for payment in client.payments.all(params={"charge_date[gte]": execStartDate , "charge_date[lte]": execEndDate }):
            EnsekAccountId = ''
            StatementId = ''
            if self.is_json(payment.id):
                '''
                if payment.metadata:
                    if 'AccountId' in payment.metadata:
                        EnsekAccountId = json.loads(payment.metadata['AccountId'].decode("utf-8"))
                    if 'StatementId' in payment.metadata:
                        StatementId = json.loads(payment.metadata['StatementId'].decode("utf-8"))
                '''
                id_js = json.loads(payment.id.decode("utf-8"))
                amount_js = json.loads(payment.amount.decode("utf-8"))
                amount_refunded_js = json.loads(payment.amount_refunded.decode("utf-8"))
                charge_date_js = json.loads(payment.charge_date.decode("utf-8"))
                created_at_js = json.loads(payment.created_at.decode("utf-8"))
                currency_js = json.loads(payment.currency.decode("utf-8"))
                description_js = json.loads(payment.description.decode("utf-8"))
                reference_js = json.loads(payment.reference.decode("utf-8"))
                status_js = json.loads(payment.status.decode("utf-8"))
                payout_js = json.loads(payment.links.payout.decode("utf-8"))
                Payment_js = json.loads(payment.links.Payment.decode("utf-8"))
                subscription_js = json.loads(payment.links.subscription.decode("utf-8"))
                EnsekID_js = EnsekAccountId
                EnsekStatementId_js = StatementId
                listRow = [id_js, amount_js, amount_refunded_js, charge_date_js, created_at_js, currency_js, description_js,
                           reference_js, status_js, payout_js, Payment_js, subscription_js, EnsekID_js, EnsekStatementId_js ]
                q.put(listRow)
            else:
                if payment.metadata:
                    if 'AccountId' in payment.metadata:
                        EnsekAccountId = payment.metadata['AccountId']
                    if 'StatementId' in payment.metadata:
                        StatementId = payment.metadata['StatementId']
                ## print(payment.id)
                id = payment.id
                amount = payment.amount
                amount_refunded = payment.amount_refunded
                charge_date = payment.charge_date
                created_at = payment.created_at
                currency = payment.currency
                description = payment.description
                reference = payment.reference
                status = payment.status
                payout = payment.links.payout
                Payment = payment.links.Payment
                subscription = payment.links.subscription
                EnsekID = EnsekAccountId
                EnsekStatementId = StatementId
                listRow = [id, amount, amount_refunded, charge_date, created_at, currency, description,
                           reference, status, payout, Payment, subscription, EnsekID, EnsekStatementId]
                q.put(listRow)
            data = q.queue
            for d in data:
                datalist.append(d)
            # Empty queue
            with q.mutex:
                q.queue.clear()


        df = pd.DataFrame(datalist, columns=['id','amount', 'amount_refunded', 'charge_date','created_at',
                                             'currency', 'description', 'reference', 'status', 'payout', 'Payment',
                                             'subscription','EnsekID', 'StatementId'])

        df_string = df.to_csv(None, index=False)
        # print(df_account_transactions_string)

        s3.key = fileDirectory + os.sep + self.s3key + os.sep + self.filename
        print(s3.key)
        s3.set_contents_from_string(df_string)


if __name__ == "__main__":
    freeze_support()
    s3 = db.get_S3_Connections_client()

    p = GoCardlessPayments('2020-01-01', 1)

    p.process_Payments()



