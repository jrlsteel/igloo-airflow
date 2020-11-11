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
from connections.connect_db import get_finance_s3_connections as s3_con
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

    def process_Payouts(self, _StartDate, _EndDate, _Qtr):
        fileDirectory = self.fileDirectory
        s3 = self.s3
        RpStartDate = _StartDate
        RpEndDate = _EndDate
        payouts = self.payouts
        filename = 'go_cardless_payouts_' + _StartDate + '_' + _EndDate + '.csv'
        fileDate = datetime.strptime(self.toDay, '%Y-%m-%d')
        qtr = _Qtr  ##math.ceil(fileDate.month / 3.)
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

        column_list = util.get_common_info('go_cardless_column_order', 'payouts')
        df_string = df.to_csv(None, columns=column_list, index=False)
        # print(df_account_transactions_string)
        s3.key = fileDirectory + fkey + filename
        print(s3.key)
        s3.set_contents_from_string(df_string)

        return df

if __name__ == "__main__":
    freeze_support()
    s3 = db.get_finance_s3_connections_client()
    p = GoCardlessPayouts()

    ### Get latest record from aws_fin_stage1_extracts.fin_go_cardless_api_payouts
    startdateDF = util.execute_query(p.sql)

    ### Assign Report End Date as Script Execution Timestamp
    ReportEndDate = str(p.execEndDate) + str(".000Z")

    ### Assign Report Start  Date After latest record
    ### from aws_fin_stage1_extracts.fin_go_cardless_api_payouts
    ReportStartDate = str(startdateDF.iat[0,0])

    print('Most Recent Report StartDate:  {0}'.format(ReportStartDate))
    tz_start = '-01T00:00:00.000Z'
    tz_stop = '-01T00:00:00.000Z'

    ### Dictionary to determine Quarter from Report StartDate
    dict_runtime = {1:['01','04'],
                    2: ['04', '07'],
                    3: ['07', '10'],
                    4: ['10', '01']
                    }

    ReportStartDate = datetime.strptime(ReportStartDate, '%Y-%m-%dT%H:%M:%S.%fZ')
    ReportEndDate = datetime.strptime(ReportEndDate, '%Y-%m-%dT%H:%M:%S.%fZ')

    ### Create a List of ReportStartDate, ReportEndDate
    lsd  = [ReportStartDate, ReportEndDate]

    ### Derive Report Quarter Start
    date_time_obj = min(lsd) ##datetime.strptime(min(lsd), '%Y-%m-%dT%H:%M:%S.%fZ')
    qtr = math.ceil(date_time_obj.month / 3.)
    yr = math.ceil(date_time_obj.year)

    ### Derive Report Quarter End
    qtr_rs = math.ceil(ReportEndDate.month / 3.)
    yr_rs = math.ceil(ReportEndDate.year)
    ReportEndYr = None
    if qtr <= 3:
        ReportEndYr = yr
    else:
        ReportEndYr = yr + 1

    ### List of Report Quarters to process
    qtrList = [qtr, qtr_rs]

    ### List of Report Years to process
    yrList = [yr, yr_rs]

    reportQtr = list(set(qtrList))
    reportYr = list(set(yrList))
    noQtrs = len(reportQtr)
    noYrs = len(reportYr)

    ### Loop through set of Report Quarters to process
    n = 0
    while n < noQtrs:
        lkpkey = reportQtr[n]
        dateList = dict_runtime[lkpkey]
        ### IF Report Year overlaps, Set Report Year plus one
        if dateList[0] > dateList[1]:
            ReportEndYr = ReportEndYr + 1
        if n == 0: ## and noYrs < 2:
            rptStart =  str(yr)+'-'+dateList[0]+tz_start
            rptEnd =  str(ReportEndYr)+'-'+dateList[1]+tz_stop
            print('ReportStartDate:  {0}'.format(rptStart))
            print('ReportEndDate:  {0}'.format(rptEnd))
            p1 = p.process_Payouts(_StartDate=rptStart, _EndDate=rptEnd, _Qtr= lkpkey)
        else:
            rptStart = str(reportYr[0]) + '-' + dateList[0] + tz_start
            rptEnd = str(ReportEndYr) + '-' + dateList[1] + tz_stop
            print('ReportStartDate:  {0}'.format(rptStart))
            print('ReportEndDate:  {0}'.format(rptEnd))
            p1 = p.process_Payouts(_StartDate=rptStart, _EndDate=rptEnd, _Qtr= lkpkey)
        n+=1





