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


class GoCardlessEvents(object):

    def __init__(self, execDate = datetime.now(), noDays=1 ):
        self.env = util.get_env()
        self.dir = util.get_dir()
        self.bucket_name = self.dir['s3_finance_bucket']
        self.s3 = s3_con(self.bucket_name)
        self.now = datetime.now()
        self.execDate = datetime.strptime(execDate, '%Y-%m-%d')
        self.qtr = math.ceil(self.execDate.month/3.)
        self.yr = math.ceil(self.execDate.year)
        self.s3key = 'timestamp=' + str(self.yr) + '-Q'+ str(self.qtr)
        self.filename = 'go_cardless_events_' + '{:%Y-%m-%d}'.format(self.execDate) + '.csv'
        self.noDays = noDays
        self.fileDirectory = self.dir['s3_finance_goCardless_key']['Events']


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


    def process_Events(self):
        bucket_name = self.bucket_name
        execStartDate = '{:%Y-%m-%d}'.format(self.execDate) + "T00:00:00.000Z"
        execEndDate = self.get_date() + "T00:00:00.000Z"
        s3 = self.s3
        dir_s3 = self.dir
        fileDirectory = self.fileDirectory

        client = gocardless_pro.Client(access_token= con.go_cardless['access_token'], environment= con.go_cardless['environment'])
        # Loop through a page of payments, printing each payment's amount
        q = Queue()
        datalist = []

        # Fetch a payment by its ID
        events = client.events

        # Loop through a page of payments, printing each payment's amount
        df = pd.DataFrame()
        print('.....listing events')
        for event in events.all(params={"created_at[gt]": execStartDate, "created_at[lt]": execEndDate}):
            try:
                EnsekAccountId = ''
                StatementId = ''
                mandate = ''
                new_customer_bank_account = ''
                new_mandate = ''
                organisation = ''
                parent_event = ''
                payment = ''
                payout = ''
                previous_customer_bank_account = ''
                refund  = ''
                subscription = ''

                id = ''
                created_at = ''
                resource_type = ''
                action = ''
                customer_notifications = ''
                cause = ''
                description = ''
                origin = ''
                reason_code = ''
                scheme = ''
                will_attempt_retry = ''
                if len(event.id) != 0:
                    ## print(event.id)
                    '''
                    if event.metadata:
                        if 'AccountId' in event.metadata:
                            EnsekAccountId = event.metadata['AccountId']
                        if 'StatementId' in event.metadata:
                            StatementId = event.metadata['StatementId']
                    '''
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

                    if event.id and len(event.id) != 0:
                        id = event.id
                    if event.created_at and len(event.created_at) != 0:
                        created_at = event.created_at
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

                    EnsekID = EnsekAccountId
                    EnsekStatementId = StatementId
                    ##print(event.id)
                    listRow = [id, created_at, resource_type, action, customer_notifications, cause, description, origin,
                               reason_code, scheme, will_attempt_retry,
                               mandate , new_customer_bank_account , new_mandate ,organisation , parent_event , payment ,
                               payout, previous_customer_bank_account ,refund ,subscription]
                               ##EnsekID, EnsekStatementId]
                    q.put(listRow)
            except (json.decoder.JSONDecodeError, gocardless_pro.errors.GoCardlessInternalError, gocardless_pro.errors.MalformedResponseError) as e:
                pass
            data = q.queue
            for d in data:
                datalist.append(d)
            # Empty queue
            with q.mutex:
                q.queue.clear()

        df = pd.DataFrame(datalist, columns=['id',  'created_at', 'resource_type', 'action', 'customer_notifications',
                                             'cause', 'description', 'origin', 'reason_code', 'scheme', 'will_attempt_retry',
                                             'mandate' , 'new_customer_bank_account' , 'new_mandate' , 'organisation' ,
                                             'parent_event' , 'payment' , 'payout', 'previous_customer_bank_account' , 'refund' ,
                                             'subscription']) ### , 'EnsekID', 'StatementId'])

        print(df.head(5))


        #df.to_csv('go_cardless_events_202001.csv', encoding='utf-8', index=False)
        df_string = df.to_csv(None, index=False)
        # print(df_account_transactions_string)

        s3.key = fileDirectory + os.sep + self.s3key + os.sep + self.filename
        print(s3.key)
        s3.set_contents_from_string(df_string)


if __name__ == "__main__":

    freeze_support()
    s3 = db.get_S3_Connections_client()

    p = GoCardlessEvents('2020-01-01', 1)

    p.process_Events()
    ###print(fre + "T00:00:00.000Z")



