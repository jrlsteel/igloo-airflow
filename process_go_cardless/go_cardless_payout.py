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
payouts = client.payouts


class GoCardlessPayouts(object):

    def __init__(self, _execStartDate = None, _execEndDate = None):
        self.env = util.get_env()
        self.dir = util.get_dir()
        self.bucket_name = self.dir['s3_finance_bucket']
        self.s3 = s3_con(self.bucket_name)
        self.sql = 'select max(created_at) as lastRun from aws_fin_stage1_extracts.fin_go_cardless_api_payouts '
        self.fileDirectory = self.dir['s3_finance_goCardless_key']['Payouts']
        self.payouts = payouts
        self.execEndDate = datetime.now().replace(microsecond=0).isoformat() ##datetime.today().strftime('%Y-%m-%d')
        self.toDay = datetime.today().strftime('%Y-%m-%d')

    def is_json(self, myjson):
        try:
            json_object = json.loads(myjson)
        except ValueError as e:
            return False
        return True


    def get_date(self, _date, _addDays = None, dateFormat="%Y-%m-%d"):
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

    def daterange(self, start_date, dateFormat="%Y-%m-%d"):
        end_date = datetime.strptime(self.execEndDate, '%Y-%m-%d')   ##self.execEndDate
        for n in range(int((end_date - start_date).days)):
            seq_date = start_date + timedelta(n)
            yield seq_date.strftime(dateFormat)
        return seq_date

    def process_Payouts(self, _StartDate, _EndDate):
        fileDirectory = self.fileDirectory
        s3 = self.s3
        RpStartDate = _StartDate
        RpEndDate = _EndDate
        payouts = self.payouts
        filename = 'go_cardless_payouts_' + _StartDate + '_' + _EndDate + '.csv'
        fileDate = datetime.strptime(self.toDay, '%Y-%m-%d')
        qtr = math.ceil(fileDate.month / 3.)
        yr = math.ceil(fileDate.year)
        fkey = 'timestamp=' + str(yr) + '-Q' + str(qtr) + '/'
        print('Listing payouts.......')
        # Loop through a page
        q = Queue()
        df_out = pd.DataFrame()
        datalist = []
        ls = []
        print(RpStartDate, _EndDate)

        # Loop through a page of Payouts, printing each Payout's amount
        print('.....listing payouts')
        for payout in client.payouts.all(params={"created_at[gt]": RpStartDate, "created_at[lte]": RpEndDate}):
            payout_id = payout.id
            amount = payout.amount
            arrival_date = payout.arrival_date
            created_at = payout.created_at
            deducted_fees = payout.deducted_fees
            payout_type = payout.payout_type
            reference = payout.reference
            status = payout.status
            creditor = payout.links.creditor
            creditor_bank_account = payout.links.creditor_bank_account

            listRow = [payout_id, amount, arrival_date, created_at, deducted_fees, payout_type,
                       reference, status, creditor, creditor_bank_account]
            q.put(listRow)

        while not q.empty():
            datalist.append(q.get())
        df = pd.DataFrame(datalist, columns=['payout_id', 'amount', 'arrival_date', 'created_at', 'deducted_fees',
                                             'payout_type', 'reference', 'status', 'creditor', 'creditor_bank_account'
                                             ])

        print(df.head(5))

        df_string = df.to_csv(None, index=False)
        # print(df_account_transactions_string)
        s3.key = fileDirectory + fkey + filename
        print(s3.key)
        s3.set_contents_from_string(df_string)

        return df

if __name__ == "__main__":
    freeze_support()
    s3 = db.get_finance_S3_Connections_client()
    p = GoCardlessPayouts()
    startdateDF = util.execute_query(p.sql)
    ReportEndDate = str(p.execEndDate) + str(".000Z")
    ReportStartDate = str(startdateDF.iat[0,0])
    print('ReportStartDate:  {0}'.format(ReportStartDate))
    print('ReportEndDate:  {0}'.format(ReportEndDate))

    p1 = p.process_Payouts(_StartDate=ReportStartDate, _EndDate=ReportEndDate)



