import os
import gocardless_pro
import timeit

import pandas as pd
import numpy as np
import requests
import json
import multiprocessing
from multiprocessing import freeze_support
from datetime import datetime, date, time, timedelta
import math
from ratelimit import limits, sleep_and_retry
import time

from queue import Queue
from pandas.io.json import json_normalize
from pathlib import Path

import sys

sys.path.append('..')

from common import utils as util
from conf import config as con
from connections.connect_db import get_finance_s3_connections as s3_con
from connections import connect_db as db

client = gocardless_pro.Client(access_token=con.go_cardless['access_token'],
                               environment=con.go_cardless['environment'])
Events = client.events
Refunds = client.refunds
Mandates = client.mandates
Subscriptions = client.subscriptions
Payments = client.payments


class GoCardlessPayments(object):

    def __init__(self, _execStartDate=None, _execEndDate=None):
        self.env = util.get_env()
        self.dir = util.get_dir()
        self.bucket_name = self.dir['s3_finance_bucket']
        self.s3 = s3_con(self.bucket_name)
        self.EventsFileDirectory = self.dir['s3_finance_goCardless_key']['Events']
        self.MandatesFileDirectory = self.dir['s3_finance_goCardless_key']['Mandates-Files']
        self.SubscriptionsFileDirectory = self.dir['s3_finance_goCardless_key']['Subscriptions-Files']
        self.PaymentsFileDirectory = self.dir['s3_finance_goCardless_key']['Payments-Files']
        self.RefundsFileDirectory = self.dir['s3_finance_goCardless_key']['Refunds-Files']
        self.Events = Events
        self.Mandates = Mandates
        self.Subscriptions = Subscriptions
        self.Payments = Payments
        self.Refunds = Refunds
        self.toDay = datetime.today().strftime('%Y-%m-%d')
        if _execStartDate is None:
            _execStartDate = self.get_date(self.toDay, _addDays = -1)
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
        EventsfileDirectory = self.EventsFileDirectory
        PaymentsfileDirectory = self.PaymentsFileDirectory
        s3 = self.s3
        if _StartDate is None:
            _StartDate = '{:%Y-%m-%d}'.format(self.execStartDate)
        if _EndDate is None:
            _EndDate = '{:%Y-%m-%d}'.format(self.execEndDate)
        startdatetime = datetime.strptime(_StartDate, '%Y-%m-%d')
        Events = self.Events
        Payments = self.Payments
        filenameEvents = 'go_cardless_events_' + _StartDate + '_' + _EndDate + '.csv'
        qtr = math.ceil(startdatetime.month / 3.)
        yr = math.ceil(startdatetime.year)
        fkey = 'timestamp=' + str(yr) + '-Q' + str(qtr) + '/'
        print('Listing Payments.......')
        # Loop through a page
        q = Queue()
        q_payment = Queue()
        payment_datalist = []
        StartDate = _StartDate + "T00:00:00.000Z"
        EndDate = _EndDate + "T00:00:00.000Z"
        print(_StartDate, _EndDate)
        print('.....listing payments')
        try:
            for payment in client.payments.all(params={"created_at[gte]": StartDate, "created_at[lte]": EndDate}):
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
                    ##q.put(listRow)
                else:
                    if payment.metadata:
                        if 'AccountId' in payment.metadata:
                            EnsekAccountId = payment.metadata['AccountId']
                        if 'StatementId' in payment.metadata:
                            StatementId = payment.metadata['StatementId']
                    print(payment.id)
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
                    payment_listRow = [id, amount, amount_refunded, charge_date, created_at, currency, description,
                                       reference, status, payout, mandate, subscription, EnsekID, EnsekStatementId]
                    q_payment.put(payment_listRow)

        except (json.decoder.JSONDecodeError, gocardless_pro.errors.GoCardlessInternalError,
                gocardless_pro.errors.MalformedResponseError) as e:
            pass

        # Empty queue
        while not q_payment.empty():
            payment_datalist.append(q_payment.get())

        df = pd.DataFrame(payment_datalist, columns=['id', 'amount', 'amount_refunded', 'charge_date', 'created_at',
                                                     'currency', 'description', 'reference', 'status', 'payout',
                                                     'mandate',
                                                     'subscription', 'EnsekID', 'StatementId'])

        return df

    def writeCSVs_Payments(self, df):
        PaymentsfileDirectory = self.PaymentsFileDirectory
        s3 = self.s3

        for row in df.itertuples(index=True, name='Pandas'):
            id = row.id
            r_1 = [row.id, row.amount, row.amount_refunded, row.charge_date, row.created_at, row.currency,
                   row.description, row.reference, row.status, row.payout, row.mandate,
                   row.subscription, row.EnsekID, row.StatementId]

            print(r_1)
            df_1 = pd.DataFrame([r_1],
                                columns=['id', 'amount', 'amount_refunded', 'charge_date', 'created_at',
                                         'currency', 'description', 'reference', 'status', 'payout', 'mandate',
                                         'subscription', 'EnsekID', 'StatementId'])

            filename = 'go_cardless_Payments_' + id + '.csv'
            df_string = df_1.to_csv(None, index=False)
            s3.key = PaymentsfileDirectory + filename
            print(s3.key)
            s3.set_contents_from_string(df_string)



    def WriteToS3(self, df):
        s3=self.s3
        pdf =  df

        filename = 'go_cardless_Payments_Merged.csv'
        df_string = pdf.to_csv(None, index=False)
        s3.key = "/go-cardless-api-paymentsMerged-files/go_cardless_Payments_Merged.csv"
        print(s3.key)
        s3.set_contents_from_string(df_string)


    def runDailyFiles(self):
        for single_date in self.daterange():
            start = single_date
            end = self.get_date(start)
            ## print(start, end)
            ### Execute Job ###
            self.process_Payments(start, end)




    def Multiprocess_Event(self, df, method):
        env = util.get_env()
        if env == 'uat':
            n = 12  # number of process to run in parallel
        else:
            n = 12

        k = int(len(df) / n)  # get equal no of files for each process

        print(len(df))
        print(k)

        processes = []
        lv = 0
        start = timeit.default_timer()

        for i in range(n + 1):
            print(i)
            uv = i * k
            if i == n:
                t = multiprocessing.Process(target= method, args=(df[lv:],))
            else:
                t = multiprocessing.Process(target= method, args=(df[lv:uv],))
            lv = uv

            processes.append(t)

        for p in processes:
            p.start()
            time.sleep(2)

        for process in processes:
            process.join()
        ####### Multiprocessing Ends #########

        print("Process completed in " + str(timeit.default_timer() - start) + ' seconds')



if __name__ == "__main__":
    freeze_support()
    s3 = db.get_finance_s3_connections_client()
    ### StartDate & EndDate in YYYY-MM-DD format ###
    ### When StartDate & EndDate is not provided it defaults to SysDate and Sysdate + 1 respectively ###
    ### 2019-05-29 2019-05-30 ###
    p = GoCardlessPayments('2017-03-01', '2020-06-17')
    ##p = GoCardlessPayments()


    ### PAYMENTS ###
    df_payments = p.process_Payments()
    AllPay = p.WriteToS3(df_payments)
    ##pPayments = p.Multiprocess_Event(df=df_payments, method=p.writeCSVs_Payments)












