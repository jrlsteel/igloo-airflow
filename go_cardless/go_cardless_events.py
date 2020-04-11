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

client = gocardless_pro.Client(access_token=con.go_cardless['access_token'],
                                       environment=con.go_cardless['environment'])
Events = client.events


class GoCardlessEvents(object):

    def __init__(self, execStartDate, execEndDate):
        self.env = util.get_env()
        self.dir = util.get_dir()
        self.bucket_name = self.dir['s3_finance_bucket']
        self.s3 = s3_con(self.bucket_name)
        self.fileDirectory = self.dir['s3_finance_goCardless_key']['Events']
        self.Events = Events
        self.execStartDate = datetime.strptime(execStartDate, '%Y-%m-%d')
        self.execEndDate = datetime.strptime(execEndDate, '%Y-%m-%d')
        self.qtr = math.ceil(self.execStartDate.month / 3.)
        self.yr = math.ceil(self.execStartDate.year)
        self.s3key = 'timestamp=' + str(self.yr) + '-Q' + str(self.qtr)
        self.filename = 'go_cardless_events_' + '{:%Y%m%d}'.format(self.execStartDate) + '_' + '{:%Y%m%d}'.format(self.execEndDate) + '.csv'

    def is_json(self, myjson):
        try:
            json_object = json.loads(myjson)
        except ValueError as e:
            return False
        return True


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

    def process_Events(self, _StartDate = None, _EndDate = None):
        fileDirectory = self.fileDirectory
        s3 = self.s3
        if _StartDate is None:
            _StartDate = self.execStartDate
        if _EndDate is None:
            _EndDate = self.execEndDate
        Events = self.Events
        # Loop through a page
        q = Queue()
        df_out = pd.DataFrame()
        datalist = []
        ls = []
        StartDate = '{:%Y-%m-%d}'.format(_StartDate) + "T00:00:00.000Z"
        EndDate = '{:%Y-%m-%d}'.format(_EndDate) + "T00:00:00.000Z"
        try:
            for event in Events.all(
                    params={"created_at[gte]": StartDate, "created_at[lte]": EndDate}):
                test = []
                id = None

                mandate = None
                new_customer_bank_account = None
                new_mandate = None
                organisation = None
                parent_event = None
                payment = None
                payout = None
                previous_customer_bank_account = None
                refund = None
                subscription = None

                created_at = None
                resource_type = None
                action = None
                customer_notifications = None
                cause = None
                description = None
                origin = None
                reason_code = None
                scheme = None
                will_attempt_retry = None

                if self.is_json(event.id):
                        id_js = json.loads(event.id.decode("utf-8"))
                elif len(event.id) > 0:
                    id = event.id
                created_at = event.created_at

                if event.links.mandate and len(event.links.mandate) != 0:
                    mandate = event.links.mandate
                if event.links.new_customer_bank_account and len(event.links.new_customer_bank_account) != 0:
                    new_customer_bank_account = event.links.new_customer_bank_account
                if event.links.new_mandate and len(event.links.new_mandate) != 0:
                    new_mandate = event.links.new_mandate
                if event.links.organisation and len(event.links.organisation) != 0:
                    organisation = event.links.organisation
                if event.links.parent_event and len(event.links.parent_event) != 0:
                    parent_event = event.links.parent_event
                if event.links.payment and len(event.links.payment) != 0:
                    payment = event.links.payment
                if event.links.payout and len(event.links.payout) != 0:
                    payout = event.links.payout
                if event.links.previous_customer_bank_account and len(event.links.previous_customer_bank_account) != 0:
                    previous_customer_bank_account = event.links.previous_customer_bank_account
                if event.links.refund and len(event.links.refund) != 0:
                    refund = event.links.refund
                if event.links.subscription and len(event.links.subscription) != 0:
                    subscription = event.links.subscription

                if event.resource_type and len(event.resource_type) != 0:
                    resource_type = event.resource_type
                if event.action and len(event.action) != 0:
                    action = event.action
                if event.customer_notifications and len(event.customer_notifications) != 0:
                    customer_notifications = event.customer_notifications
                if event.details.cause and len(event.details.cause) != 0:
                    cause = event.details.cause
                if event.details.description and len(event.details.description) != 0:
                    description = event.details.description
                if event.details.origin and len(event.details.origin) != 0:
                    origin = event.details.origin
                if event.details.reason_code and len(event.details.reason_code) != 0:
                    reason_code = event.details.reason_code
                if event.details.scheme and len(event.details.scheme) != 0:
                    scheme = event.details.scheme
                if event.details.will_attempt_retry and len(event.details.will_attempt_retry) != 0:
                    will_attempt_retry = event.details.will_attempt_retry

                listRow = [id, created_at, resource_type, action, customer_notifications, cause, description, origin,
                               reason_code, scheme, will_attempt_retry,
                               mandate , new_customer_bank_account , new_mandate ,organisation , parent_event , payment ,
                               payout, previous_customer_bank_account ,refund ,subscription]
                q.put(listRow)

        except (json.decoder.JSONDecodeError, gocardless_pro.errors.GoCardlessInternalError,
                gocardless_pro.errors.MalformedResponseError) as e:
            pass

        while not q.empty():
            datalist.append(q.get())


        df = pd.DataFrame(datalist, columns=['id',  'created_at', 'resource_type', 'action', 'customer_notifications',
                                             'cause', 'description', 'origin', 'reason_code', 'scheme', 'will_attempt_retry',
                                             'mandate' , 'new_customer_bank_account' , 'new_mandate' , 'organisation' ,
                                             'parent_event' , 'payment' , 'payout', 'previous_customer_bank_account' , 'refund' ,
                                             'subscription'])

        print(df.head(5))


        df_string = df.to_csv(None, index=False)
        # print(df_account_transactions_string)

        s3.key = fileDirectory + os.sep + self.s3key + os.sep + self.filename
        print(s3.key)
        s3.set_contents_from_string(df_string)

        # df.to_csv('go_cardless_Events.csv', encoding='utf-8', index=False)

        return df

    def runDailyFiles(self):
        for single_date in self.daterange():
            start = single_date
            end = self.get_date(start)
            print(start, end)
            ### Execute Job ###
            self.process_Events(start, end)


if __name__ == "__main__":
    freeze_support()
    s3 = db.get_finance_S3_Connections_client()
    ### StartDate & EndDate in YYYY-MM-DD format ###
    p = GoCardlessEvents('2020-01-01', '2020-04-01')

    ## p1 = p.process_Events()
    p2 = p.runDailyFiles()







