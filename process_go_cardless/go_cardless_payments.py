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
payments = client.payments


class GoCardlessPayments(object):

    def __init__(self, _execStartDate=None, _execEndDate=None):
        self.env = util.get_env()
        self.dir = util.get_dir()
        self.bucket_name = self.dir['s3_finance_bucket']
        self.s3 = s3_con(self.bucket_name)
        self.fileDirectory = self.dir['s3_finance_goCardless_key']['Payments']
        self.payments = payments
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

    def process_Payments(self, _StartDate=None, _EndDate=None):
        fileDirectory = self.fileDirectory
        s3 = self.s3
        if _StartDate is None:
            _StartDate = '{:%Y-%m-%d}'.format(self.execStartDate)
        if _EndDate is None:
            _EndDate = '{:%Y-%m-%d}'.format(self.execEndDate)
        startdatetime = datetime.strptime(_StartDate, '%Y-%m-%d')
        payments = self.payments
        filename = 'go_cardless_payments_' + _StartDate + '_' + _EndDate + '.csv'
        qtr = math.ceil(startdatetime.month / 3.)
        yr = math.ceil(startdatetime.year)
        fkey = 'timestamp=' + str(yr) + '-Q' + str(qtr) + '/'
        # Loop through a page
        q = Queue()
        df_out = pd.DataFrame()
        datalist = []
        ls = []
        StartDate = _StartDate ### + "T00:00:00.000Z"
        EndDate = _EndDate ### + "T00:00:00.000Z"
        print(_StartDate, _EndDate)

        print('.....listing payments')
        for payment in client.payments.all(params={"charge_date[gte]": StartDate, "charge_date[lte]": EndDate}):
            EnsekAccountId = None
            StatementId = None
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
                mandate_js = json.loads(payment.links.mandate.decode("utf-8"))
                subscription_js = json.loads(payment.links.subscription.decode("utf-8"))
                EnsekID_js = EnsekAccountId
                EnsekStatementId_js = StatementId
                listRow = [id_js, amount_js, amount_refunded_js, charge_date_js, created_at_js, currency_js,
                           description_js,
                           reference_js, status_js, payout_js, mandate_js, subscription_js, EnsekID_js,
                           EnsekStatementId_js]
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
                mandate = payment.links.mandate
                subscription = payment.links.subscription
                EnsekID = EnsekAccountId
                EnsekStatementId = StatementId
                listRow = [id, amount, amount_refunded, charge_date, created_at, currency, description,
                           reference, status, payout, mandate, subscription, EnsekID, EnsekStatementId]
                q.put(listRow)
            data = q.queue
            for d in data:
                datalist.append(d)
            # Empty queue
            with q.mutex:
                q.queue.clear()

        df = pd.DataFrame(datalist, columns=['id', 'amount', 'amount_refunded', 'charge_date', 'created_at',
                                             'currency', 'description', 'reference', 'status', 'payout', 'mandate',
                                             'subscription', 'EnsekID', 'StatementId'])

        print(df.head(5))

        df_string = df.to_csv(None, index=False)
        # print(df_account_transactions_string)

        ## s3.key = fileDirectory + os.sep + s3key + os.sep + filename
        ## s3.key = Path(fileDirectory, s3key, filename)
        s3.key = fileDirectory + fkey + filename
        print(s3.key)
        s3.set_contents_from_string(df_string)

        # df.to_csv('go_cardless_payments.csv', encoding='utf-8', index=False)

        return df

    def runDailyFiles(self):
        for single_date in self.daterange():
            start = single_date
            end = self.get_date(start)
            ## print(start, end)
            ### Execute Job ###
            self.process_Payments(start, end)


if __name__ == "__main__":
    freeze_support()
    s3 = db.get_finance_S3_Connections_client()
    ### StartDate & EndDate in YYYY-MM-DD format ###
    ### When StartDate & EndDate is not provided it defaults to SysDate and Sysdate + 1 respectively ###
    ### 2019-05-29 2019-05-30 ###
    ## p = GoCardlessPayments('2020-04-01', '2020-04-14')
    p = GoCardlessPayments()

    p1 = p.process_Payments()
    ### Extract return single Daily Files from Date Range Provided ###
    ## p2 = p.runDailyFiles()







