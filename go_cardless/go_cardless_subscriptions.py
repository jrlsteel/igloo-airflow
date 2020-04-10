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

import sys

sys.path.append('..')

from common import utils as util
from conf import config as con
from connections.connect_db import get_finance_S3_Connections as s3_con
from connections import connect_db as db

client = gocardless_pro.Client(access_token='live_yk11i3mKMHN454DYUjIR0Mw6y34ePfW1GJoEXHaS', environment='live')
subscriptions = client.subscriptions


class GoCardlessSubscriptions(object):

    def __init__(self, execStartDate, execEndDate):
        self.env = util.get_env()
        self.dir = util.get_dir()
        self.bucket_name = self.dir['s3_finance_bucket']
        self.s3 = s3_con(self.bucket_name)
        self.fileDirectory = self.dir['s3_finance_goCardless_key']['Subscriptions']
        self.subscriptions = subscriptions
        self.execStartDate = datetime.strptime(execStartDate, '%Y-%m-%d')
        self.execEndDate = datetime.strptime(execEndDate, '%Y-%m-%d')
        self.qtr = math.ceil(self.execStartDate.month / 3.)
        self.yr = math.ceil(self.execStartDate.year)
        self.s3key = 'timestamp=' + str(self.yr) + '-Q' + str(self.qtr)
        self.filename = 'go_cardless_subscriptions_' + '{:%Y%m}'.format(self.execStartDate) + '_' + '{:%Y%m}'.format(
            self.execEndDate) + '.csv'

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

    def process_Subscriptions(self):
        fileDirectory = self.fileDirectory
        s3 = self.s3
        subscriptions = self.subscriptions
        # Loop through a page
        q = Queue()
        df_out = pd.DataFrame()
        datalist = []
        ls = []
        StartDate = '{:%Y-%m-%d}'.format(self.execStartDate) + "T00:00:00.000Z"
        EndDate = '{:%Y-%m-%d}'.format(self.execEndDate) + "T00:00:00.000Z"
        for subscription in subscriptions.all(
                params={"created_at[gte]": StartDate, "created_at[lte]": EndDate}):
            test = []
            charge_date = None
            amount_subscription = None
            mandate = None
            id = subscription.id
            upcoming_payments = subscription.upcoming_payments
            if len(upcoming_payments) > 0:
                charge_date = upcoming_payments[0]['charge_date']
                amount_subscription = upcoming_payments[0]['amount']
            created_at = subscription.created_at
            amount = subscription.amount
            currency = subscription.currency
            status = subscription.status
            name = subscription.name
            start_date = subscription.start_date
            end_date = subscription.end_date
            interval = subscription.interval
            interval_unit = subscription.interval_unit
            day_of_month = subscription.day_of_month
            month = subscription.month
            count_no = subscription.count
            payment_reference = subscription.payment_reference
            app_fee = subscription.app_fee
            retry_if_possible = subscription.retry_if_possible
            # earliest_charge_date_after_resume = subscription.earliest_charge_date_after_resume
            if subscription.links.mandate:
                mandate = subscription.links.mandate

            listRow = [id, created_at, amount, currency, status, name, start_date,
                       end_date, interval, interval_unit, day_of_month, month,
                       count_no, payment_reference, app_fee, retry_if_possible, mandate, charge_date,
                       amount_subscription]
            q.put(listRow)

        while not q.empty():
            datalist.append(q.get())

        df = pd.DataFrame(datalist, columns=['id', 'created_at', 'amount', 'currency', 'status', 'name', 'start_date',
                                             'end_date', 'interval', 'interval_unit', 'day_of_month', 'month',
                                             'count_no', 'payment_reference', 'app_fee', 'retry_if_possible', 'mandate',
                                             'charge_date', 'amount_subscription'])

        print(df.head(5))

        df_string = df.to_csv(None, index=False)
        # print(df_account_transactions_string)

        s3.key = fileDirectory + os.sep + self.s3key + os.sep + self.filename
        print(s3.key)
        s3.set_contents_from_string(df_string)

        # df.to_csv('go_cardless_subscriptions.csv', encoding='utf-8', index=False)
        return df

    def sampletest(self):
        for single_date in self.daterange():
            print(single_date)


if __name__ == "__main__":
    freeze_support()
    s3 = db.get_finance_S3_Connections_client()
    ### StartDate & EndDate in YYYY-MM-DD format ###
    p = GoCardlessSubscriptions('2017-07-01', '2017-10-01')

    p1 = p.process_Subscriptions()







