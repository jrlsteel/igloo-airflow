import os
import gocardless_pro
import timeit
from io import StringIO
import pandas as pd
import numpy as np
import requests
import json
import multiprocessing
from multiprocessing import freeze_support
from datetime import datetime, date, time, timedelta
from time import sleep
import math
from ratelimit import limits, sleep_and_retry
import time

from queue import Queue
import queue
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
Events = client.events
Refunds = client.refunds
Mandates = client.mandates
Subscriptions = client.subscriptions
Payments = client.payments


class IterableQueue():
    def __init__(self,source_queue):
            self.source_queue = source_queue
    def __iter__(self):
        while True:
            try:
               yield self.source_queue.get_nowait()
            except queue.Empty:
               return


class GoCardlessEvents(object):

    def __init__(self):
        self.env = util.get_env()
        self.dir = util.get_dir()
        self.bucket_name = self.dir['s3_finance_bucket']
        self.s3 = s3_con(self.bucket_name)
        self.sql = 'select max(created_at) as lastRun from aws_fin_stage1_extracts.fin_go_cardless_api_events '
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




    ''' EVENTS '''
    def process_Events(self, _StartDate, _EndDate):
        EventsfileDirectory = self.EventsFileDirectory
        SubscriptionsfileDirectory = self.SubscriptionsFileDirectory
        MandatesfileDirectory = self.MandatesFileDirectory
        s3 = self.s3
        RpStartDate = _StartDate
        RpEndDate = _EndDate
        Events = self.Events
        Mandates = self.Mandates
        Subscriptions = self.Subscriptions
        filenameEvents = 'go_cardless_events_' + _StartDate + '_' + _EndDate + '.csv'
        fileDate = datetime.strptime(self.toDay, '%Y-%m-%d')
        qtr = math.ceil(fileDate.month / 3.)  ## math.ceil(startdatetime.month / 3.)
        yr = math.ceil(fileDate.year) ## math.ceil(startdatetime.year)
        fkey = 'timestamp=' + str(yr) + '-Q' + str(qtr) + '/'
        print('Listing Events.......')
        # Loop through a page
        q_Event = Queue()
        event_datalist = []
        print(RpStartDate, _EndDate)
        try:
            for event in Events.all(
                    params={"created_at[gt]": RpStartDate, "created_at[lte]": RpEndDate }):

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
                print(id)

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
                           mandate, new_customer_bank_account, new_mandate, organisation, parent_event, payment,
                           payout, previous_customer_bank_account, refund, subscription]
                q_Event.put(listRow)


        except (json.decoder.JSONDecodeError, gocardless_pro.errors.GoCardlessInternalError,
                gocardless_pro.errors.MalformedResponseError) as e:
            pass

        while not q_Event.empty():
            event_datalist.append(q_Event.get())

        df_event = pd.DataFrame(event_datalist, columns=['id',  'created_at', 'resource_type', 'action', 'customer_notifications',
                                             'cause', 'description', 'origin', 'reason_code', 'scheme', 'will_attempt_retry',
                                             'mandate' , 'new_customer_bank_account' , 'new_mandate' , 'organisation' ,
                                             'parent_event' , 'payment' , 'payout', 'previous_customer_bank_account' , 'refund' ,
                                             'subscription'])

        print(df_event.head(5))


        ### EVENTS ####
        df_string = df_event.to_csv(None, index=False)
        s3.key = EventsfileDirectory + fkey + filenameEvents
        print(s3.key)
        s3.set_contents_from_string(df_string)
        return df_event



    def ResourceType(self, df, resource):
        resource_df = df.copy()  ##df[df['mandate'].notnull()]
        resource_sr = resource_df[resource].dropna().unique()
        resource_ToList = resource_sr.tolist()
        return resource_ToList



    def f_put(self, method, df, q):
        print("f_put start")
        data = method(df)
        q.put(data)

    def f_get(self, q):
        #print("f_get start")
        while (1):
            data = pd.DataFrame()
            loop = 0
            while not q.empty():
                data = q.get()
                print("get (loop: %s)" % loop)
                time.sleep(0)
                loop += 1
            time.sleep(1.)
            # self.redshift_upsert(df=data, crud_type='i')










    ''' MANDATES '''
    def process_Mandates(self, df, q, _StartDate = None, _EndDate = None ):
        EventsfileDirectory = self.EventsFileDirectory
        SubscriptionsfileDirectory = self.SubscriptionsFileDirectory
        MandatesfileDirectory = self.MandatesFileDirectory
        Events = self.Events
        Mandates = self.Mandates
        Subscriptions = self.Subscriptions
        s3 = self.s3
        print('Listing Mandates.......')
        # Loop through a page
        q_Mandate = Queue()
        datalist = []
        mandateToList = df
        try:
            for rec in mandateToList:
                mandate = client.mandates.get(rec)
                EnsekAccountId = None
                StatementId = None
                # print(mandate.id)
                if 'AccountId' in mandate.metadata:
                    EnsekAccountId = mandate.metadata['AccountId']
                if 'StatementId' in mandate.metadata:
                    StatementId = mandate.metadata['StatementId']

                mandate_id = mandate.id
                print(mandate_id)

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

                filenameMandates = 'go_cardless_mandates_' + mandate_id + '.csv'

                mandate_listRow = [mandate_id, CustomerId, new_mandate_id, created_at, next_possible_charge_date,
                                   payments_require_approval,
                                   reference, scheme, status, creditor, customer_bank_account, EnsekID,
                                   EnsekStatementId]
                ##print(mandate_listRow)
                q_Mandate.put(mandate_listRow)

        except (json.decoder.JSONDecodeError, gocardless_pro.errors.GoCardlessInternalError,
                gocardless_pro.errors.MalformedResponseError) as e:
            pass

        while not q_Mandate.empty():
            datalist.append(q_Mandate.get())
        df = pd.DataFrame(datalist, columns=['mandate_id', 'CustomerId', 'new_mandate_id', 'created_at',
                                             'next_possible_charge_date',
                                             'payments_require_approval', 'reference', 'scheme', 'status', 'creditor',
                                             'customer_bank_account',
                                             'EnsekID', 'EnsekStatementId'])

        ##print(df.head(5))
        q.put(df)
        return df

    def writeCSVs_Mandates(self, df):
        MandatesFileDirectory = self.MandatesFileDirectory
        s3 = self.s3

        for row in df.itertuples(index=True, name='Pandas'):
            id = row.mandate_id
            r_1 = [row.mandate_id, row.CustomerId, row.new_mandate_id, row.created_at, row.next_possible_charge_date,
                    row.payments_require_approval,
                    row.reference, row.scheme, row.status, row.creditor, row.customer_bank_account, row.EnsekID, row.EnsekStatementId]

            print(r_1)
            df_1 = pd.DataFrame([r_1], columns=['mandate_id', 'CustomerId', 'new_mandate_id', 'created_at',
                                             'next_possible_charge_date',
                                             'payments_require_approval', 'reference', 'scheme', 'status', 'creditor',
                                             'customer_bank_account',
                                             'EnsekID', 'EnsekStatementId'] )

            filename = 'go_cardless_mandates_' + id + '.csv'

            df_string = df_1.to_csv(None, index=False)
            s3.key = MandatesFileDirectory + filename
            print(s3.key)
            s3.set_contents_from_string(df_string)













    ''' SUBSCRIPTIONS '''

    def process_Subscriptions(self, df, q, _StartDate=None, _EndDate=None):
        EventsfileDirectory = self.EventsFileDirectory
        SubscriptionsfileDirectory = self.SubscriptionsFileDirectory
        MandatesfileDirectory = self.MandatesFileDirectory
        s3 = self.s3
        Events = self.Events
        Mandates = self.Mandates
        Subscriptions = self.Subscriptions
        print('Listing Subscriptions.......')
        # Loop through a page
        q_Subscription = Queue()
        datalist = []
        subscriptionToList = df
        try:
            for rec in subscriptionToList:
                subscription = client.subscriptions.get(rec)
                charge_date = None
                amount_subscription = None
                mandate = None
                id = subscription.id
                print(id)

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

                subscription_listRow = [id, created_at, amount, currency, status, name, start_date,
                                        end_date, interval, interval_unit, day_of_month, month,
                                        count_no, payment_reference, app_fee, retry_if_possible, mandate,
                                        charge_date,
                                        amount_subscription]

                q_Subscription.put(subscription_listRow)

        except (json.decoder.JSONDecodeError, gocardless_pro.errors.GoCardlessInternalError,
                gocardless_pro.errors.MalformedResponseError) as e:
            pass

        while not q_Subscription.empty():
            datalist.append(q_Subscription.get())

        df = pd.DataFrame(datalist,
                          columns=['id', 'created_at', 'amount', 'currency', 'status', 'name', 'start_date',
                                   'end_date', 'interval', 'interval_unit', 'day_of_month', 'month',
                                   'count_no', 'payment_reference', 'app_fee', 'retry_if_possible', 'mandate',
                                   'charge_date', 'amount_subscription'])
        q.put(df)
        return df

    def writeCSVs_Subscriptions(self, df):
        SubscriptionsfileDirectory = self.SubscriptionsFileDirectory
        s3 = self.s3

        for row in df.itertuples(index=True, name='Pandas'):
            id = row.id
            r_1 = [row.id, row.created_at, row.amount, row.currency, row.status, row.name, row.start_date,
                   row.end_date, row.interval, row.interval_unit, row.day_of_month, row.month,
                   row.count_no, row.payment_reference, row.app_fee, row.retry_if_possible, row.mandate,
                   row.charge_date,
                   row.amount_subscription]

            print(r_1)
            df_1 = pd.DataFrame([r_1],
                                columns=['id', 'created_at', 'amount', 'currency', 'status', 'name', 'start_date',
                                         'end_date', 'interval', 'interval_unit', 'day_of_month', 'month',
                                         'count_no', 'payment_reference', 'app_fee', 'retry_if_possible', 'mandate',
                                         'charge_date', 'amount_subscription'])

            filename = 'go_cardless_subscriptions_' + id + '.csv'
            df_string = df_1.to_csv(None, index=False)
            s3.key = SubscriptionsfileDirectory + filename
            print(s3.key)
            s3.set_contents_from_string(df_string)











    ''' PAYMENTS '''

    def process_Payments(self, df, q, _StartDate=None, _EndDate=None):
        EventsfileDirectory = self.EventsFileDirectory
        PaymentsfileDirectory = self.PaymentsFileDirectory
        s3 = self.s3
        Events = self.Events
        Payments = self.Payments
        print('Listing Payments.......')
        # Loop through a page
        q_payment = Queue()
        payment_datalist = []
        paymentToList = df
        try:
            for rec in paymentToList:
                payment = client.payments.get(rec)
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

        while not q_payment.empty():
            payment_datalist.append(q_payment.get())

        df = pd.DataFrame(payment_datalist, columns=['id', 'amount', 'amount_refunded', 'charge_date', 'created_at',
                                                     'currency', 'description', 'reference', 'status', 'payout',
                                                     'mandate',
                                                     'subscription', 'EnsekID', 'StatementId'])

        remove_duplicates_df = df.drop_duplicates(subset=['id'], keep='first')
        q.put(df)
        return remove_duplicates_df

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

    def Update_Events_Driven_Payments(self, df2):
        s3 = db.get_finance_S3_Connections_client()

        s31 = db.get_finance_S3_Connections_resources()
        dir_s3 = util.get_dir()
        bucket = dir_s3['s3_finance_bucket']

        ##file1 = 'go-cardless-api-paymentsnewtesting-files/go_cardless_Payments_file.csv'
        ##file2 = 'go-cardless-api-paymentsMerged-files/go_cardless_Payments_Update_20200618.csv'

        paymentsFileName = 'go-cardless-api-payments-files/go_cardless_Payments_file.csv'

        # archive File
        print('....Archiving File.....')
        keypath = 'go-cardless-api-Archived_Files/Payment_Files/go_cardless_Payments_file.csv'
        copy_source = {
            'Bucket': bucket,
            'Key': paymentsFileName
        }

        s3.copy(copy_source, bucket, keypath)
        print('....Archiving Completed.....')

        ## Previous Payments
        df1 = self.read_files_to_df(paymentsFileName)

        ## Today's Event Driven Payments
        ##df2 = self.read_files_to_df(file2)

        ## SELECT NEW or UPDATED Payment IDs
        df2_ID = df2[['id']].copy()

        ## Merge Previous Payments with (Updated or New) Payments
        df_all = df1.merge(df2_ID.drop_duplicates(), on=['id'],
                           how='left', indicator=True)
        df_all = df_all[df_all['_merge'] == 'left_only']

        df_update = df2.copy()
        df_Right = df_all.drop(['_merge'], axis=1).copy()
        df_All_Payments = pd.concat([df_Right, df_update], sort=False)
        df_All_Payments[["amount", "EnsekID", "StatementId"]] = df_All_Payments[
            ["amount", "EnsekID", "StatementId"]].apply(pd.to_numeric)
        cols_to_use = df_All_Payments.columns

        ## WRITE All Payments TO s3
        self.WritePaymentFilesToS3(df_All_Payments)

    def read_files_to_df(self, files):

        ##prefix = 'go-cardless-api-payments-files'
        prefix = ''
        s31 = db.get_finance_S3_Connections_resources()
        s3 = self.s3
        bucket = s31.Bucket(self.bucket_name)
        prefix_df = pd.DataFrame()
        df = pd.DataFrame()
        prefix = files
        prefix_objs = bucket.objects.filter(Prefix=prefix)

        for obj in prefix_objs:
            key = obj.key
            print(key)
            body = obj.get()['Body'].read()
            ### CONVERT Bytes To String
            s = str(body, 'utf-8')
            data = StringIO(s)

            df = pd.read_csv(data, low_memory=False)
            prefix_df = prefix_df.append(df)
        return prefix_df

    def WritePaymentFilesToS3(self, df):
        PaymentsfileDirectory = self.PaymentsFileDirectory
        s3 = self.s3
        pdf = df

        filename = 'go_cardless_Payments_file.csv'
        s3.key = PaymentsfileDirectory + filename
        df_string = pdf.to_csv(None, index=False)
        s3.key = "/go-cardless-api-payments-files/go_cardless_Payments_file.csv"
        print(s3.key)
        s3.set_contents_from_string(df_string)









    ''' REFUNDS '''

    def process_Refunds(self, df, q, _StartDate=None, _EndDate=None):
        EventsfileDirectory = self.EventsFileDirectory
        RefundsfileDirectory = self.RefundsFileDirectory
        s3 = self.s3
        Events = self.Events
        Refunds = self.Refunds
        print('Listing Refunds.......')
        # Loop through a page
        q_refund = Queue()
        refund_datalist = []
        refundToList = df
        try:
            for rec in refundToList:
                refund = client.refunds.get(rec)
                EnsekAccountId = None
                if 'AccountId' in refund.metadata:
                    EnsekAccountId = refund.metadata['AccountId']

                ## print(refund.id)
                id = refund.id
                print(id)

                amount = refund.amount
                created_at = refund.created_at
                currency = refund.currency
                reference = refund.reference
                status = refund.status
                metadata = refund.metadata
                payment = refund.links.payment
                mandate = refund.links.mandate
                EnsekID = EnsekAccountId

                Refund_listRow = [EnsekID, amount, created_at, currency, id,
                                  mandate, metadata, payment, reference, status]

                q_refund.put(Refund_listRow)

        except (json.decoder.JSONDecodeError, gocardless_pro.errors.GoCardlessInternalError,
                gocardless_pro.errors.MalformedResponseError) as e:
            pass

        while not q_refund.empty():
            refund_datalist.append(q_refund.get())

        df = pd.DataFrame(refund_datalist, columns=['EnsekID', 'amount', 'created_at', 'currency', 'id',
                                                    'mandate', 'metadata', 'payment', 'reference', 'status'
                                                    ])
        q.put(df)
        return df

    def writeCSVs_Refunds(self, df):
        RefundsfileDirectory = self.RefundsFileDirectory
        s3 = self.s3

        for row in df.itertuples(index=True, name='Pandas'):
            id = row.id
            r_1 = [row.EnsekID, row.amount, row.created_at, row.currency, row.id,
                   row.mandate, row.metadata, row.payment, row.reference, row.status]

            print(r_1)
            df_1 = pd.DataFrame([r_1],
                                columns=['EnsekID', 'amount', 'created_at', 'currency', 'id',
                                         'mandate', 'metadata', 'payment', 'reference', 'status'
                                         ])

            filename = 'go_cardless_Refunds_' + id + '.csv'
            df_string = df_1.to_csv(None, index=False)
            s3.key = RefundsfileDirectory + filename
            print(s3.key)
            s3.set_contents_from_string(df_string)






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
                t = multiprocessing.Process(target=method, args=(df[lv:],))
            else:
                t = multiprocessing.Process(target=method, args=(df[lv:uv],))
            lv = uv

            processes.append(t)

        for p in processes:
            p.start()
            time.sleep(2)

        for process in processes:
            process.join()
        ####### Multiprocessing Ends #########

        print("Process completed in " + str(timeit.default_timer() - start) + ' seconds')


    def MultiProcessDataframe(self, df, method):
        df_All = pd.DataFrame()
        start = timeit.default_timer()
        ######### multiprocessing starts  ##########
        itr = IterableQueue
        env = util.get_env()
        if env == 'uat':
            n = 6  # number of process to run in parallel
        else:
            n = 6
        print(len(df))
        k = int(len(df) / n)  # get equal no of files for each process
        print(k)
        processes = []

        lv = 0

        # start = timeit.default_timer()

        m = multiprocessing.Manager()
        q = m.Queue()
        q2 = m.Queue()
        try:
            for i in range(n + 1):
                ##pkf1 = ExtractEnsekFiles(flowtype)
                print(i)
                uv = i * k
                if i == n:
                    # print(ef_keys_s3[lv:])
                    t = multiprocessing.Process(target=method, args=(df[lv:], q))
                else:
                    # print(ef_keys_s3[lv:uv])
                    t = multiprocessing.Process(target=method, args=(df[lv:uv], q))
                lv = uv

                processes.append(t)

            data_df = pd.DataFrame()
            for p in processes:
                p.start()
                sleep(2)

            for process in processes:
                while not q.empty():
                    data = q.get()
                    sleep(5)
                    q2.put(data)
                process.join()

            # Completed Parallel Processes
            print(q2.qsize())


            # Concatenate Dataframes
            for dfIQ in IterableQueue(q2):
                df_All = pd.concat([df_All, dfIQ], sort=False)
        except gocardless_pro.errors.InvalidApiUsageError as e:
            print('Error: {0}'.format(e))


        ####### multiprocessing Ends #########

        print("Process completed in " + str(timeit.default_timer() - start) + ' seconds')

        return df_All


    def MultiProcessDataframe_v2(self, df, method):
        df_All = pd.DataFrame()
        start = timeit.default_timer()
        ######### multiprocessing starts  ##########
        itr = IterableQueue
        env = util.get_env()
        if env == 'uat':
            n = 8  # number of process to run in parallel
        else:
            n = 8
        print(len(df))
        k = int(len(df) / n)  # get equal no of files for each process
        print(k)
        processes = []

        lv = 0

        # start = timeit.default_timer()

        m = multiprocessing.Manager()
        q = m.Queue()
        q2 = m.Queue()
        for i in range(n + 1):
            ##pkf1 = ExtractEnsekFiles(flowtype)
            print(i)
            uv = i * k
            if i == n:
                # print(ef_keys_s3[lv:])
                t = multiprocessing.Process(target=method, args=(df[lv:], q))
            else:
                # print(ef_keys_s3[lv:uv])
                t = multiprocessing.Process(target=method, args=(df[lv:uv], q))
            lv = uv

            processes.append(t)

        data_df = pd.DataFrame()
        for p in processes:
            p.start()
            sleep(2)

        for process in processes:
            while not q.empty():
                data = q.get()
                sleep(5)
                q2.put(data)
            process.join()

        # Completed Parallel Processes
        print(q2.qsize())


        # Concatenate Dataframes
        for dfIQ in IterableQueue(q2):
            df_All = pd.concat([df_All, dfIQ], sort=False)

        ##print(vars(q2))

        ####### multiprocessing Ends #########

        print("Process completed in " + str(timeit.default_timer() - start) + ' seconds')

        return df_All










if __name__ == "__main__":
    freeze_support()
    s3 = db.get_finance_S3_Connections_client()

    p = GoCardlessEvents()
    startdateDF = util.execute_query(p.sql)
    ReportEndDate = str(p.execEndDate) + str(".000Z")
    ReportStartDate = str(startdateDF.iat[0,0])
    print('ReportStartDate:  {0}'.format(ReportStartDate))
    print('ReportEndDate:  {0}'.format(ReportEndDate))

    ### EVENTS ###
    eventsDF = p.process_Events(_StartDate=ReportStartDate, _EndDate=ReportEndDate)


    ### MANDATES ###
    mandateList = p.ResourceType(df=eventsDF, resource='mandate')
    df_mandates = p.MultiProcessDataframe(df=mandateList, method=p.process_Mandates)
    pMandates = p.Multiprocess_Event(df=df_mandates, method=p.writeCSVs_Mandates)

    ### SUBSCRIPTIONS ###
    subscriptionList = p.ResourceType(df=eventsDF, resource='subscription')
    df_subscriptions = p.MultiProcessDataframe(df=subscriptionList, method=p.process_Subscriptions)
    pSubscriptions = p.Multiprocess_Event(df=df_subscriptions, method=p.writeCSVs_Subscriptions)


    ### PAYMENTS ###
    paymentList = p.ResourceType(df=eventsDF, resource='payment')
    df_payments = p.MultiProcessDataframe(df=paymentList, method=p.process_Payments) 
    pPaymentsFiles = p.Update_Events_Driven_Payments(df2=df_payments)


    ## REFUNDS ##
    refundList = p.ResourceType(df=eventsDF, resource='refund')
    df_refunds = p.MultiProcessDataframe(df=refundList, method=p.process_Refunds)
    pRefunds = p.Multiprocess_Event(df=df_refunds, method=p.writeCSVs_Refunds)

