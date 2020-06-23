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
from connections.connect_db import get_finance_S3_Connections as s3_con
from connections import connect_db as db

client = gocardless_pro.Client(access_token=con.go_cardless['access_token'],
                                       environment=con.go_cardless['environment'])
Events = client.events
Refunds = client.refunds
Mandates = client.mandates
Subscriptions = client.subscriptions
Payments = client.payments


class GoCardlessEvents(object):

    def __init__(self, _execStartDate = None, _execEndDate = None):
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

    def daterange(self, dateFormat="%Y-%m-%d"):
        start_date = self.execStartDate
        end_date = self.execEndDate
        for n in range(int((end_date - start_date).days)):
            seq_date = start_date + timedelta(n)
            yield seq_date.strftime(dateFormat)
        return seq_date




    ''' EVENTS '''
    def process_Events(self, _StartDate = None, _EndDate = None):
        EventsfileDirectory = self.EventsFileDirectory
        SubscriptionsfileDirectory = self.SubscriptionsFileDirectory
        MandatesfileDirectory = self.MandatesFileDirectory
        s3 = self.s3
        if _StartDate is None:
            _StartDate = '{:%Y-%m-%d}'.format(self.execStartDate)
        if _EndDate is None:
            _EndDate = '{:%Y-%m-%d}'.format(self.execEndDate)
        startdatetime = datetime.strptime(_StartDate, '%Y-%m-%d')
        Events = self.Events
        Mandates = self.Mandates
        Subscriptions = self.Subscriptions
        filenameEvents = 'go_cardless_events_' + _StartDate + '_' + _EndDate + '.csv'
        qtr = math.ceil(startdatetime.month / 3.)
        yr = math.ceil(startdatetime.year)
        fkey = 'timestamp=' + str(yr) + '-Q' + str(qtr) + '/'
        print('Listing Events.......')
        # Loop through a page
        q_Event = Queue()
        event_datalist = []
        StartDate = _StartDate + "T00:00:00.000Z"
        EndDate = _EndDate + "T00:00:00.000Z"
        print(_StartDate, _EndDate)
        try:
            for event in Events.all(
                    params={"created_at[gte]": StartDate, "created_at[lte]": EndDate }):

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




    ''' MANDATES '''
    def process_Mandates(self, _StartDate = None, _EndDate = None):
        EventsfileDirectory = self.EventsFileDirectory
        SubscriptionsfileDirectory = self.SubscriptionsFileDirectory
        MandatesfileDirectory = self.MandatesFileDirectory
        s3 = self.s3
        if _StartDate is None:
            _StartDate = '{:%Y-%m-%d}'.format(self.execStartDate)
        if _EndDate is None:
            _EndDate = '{:%Y-%m-%d}'.format(self.execEndDate)
        startdatetime = datetime.strptime(_StartDate, '%Y-%m-%d')
        Events = self.Events
        Mandates = self.Mandates
        Subscriptions = self.Subscriptions
        filenameEvents = 'go_cardless_events_' + _StartDate + '_' + _EndDate + '.csv'
        qtr = math.ceil(startdatetime.month / 3.)
        yr = math.ceil(startdatetime.year)
        fkey = 'timestamp=' + str(yr) + '-Q' + str(qtr) + '/'
        print('Listing Mandates.......')
        # Loop through a page
        q_Mandate = Queue()
        datalist = []
        StartDate = _StartDate + "T00:00:00.000Z"
        EndDate = _EndDate + "T00:00:00.000Z"
        print(_StartDate, _EndDate)
        try:
            for event in Events.all(
                    params={"created_at[gte]": StartDate, "created_at[lte]": EndDate, "resource_type": "mandates"}):

                #### MANDATES #####
                if event.resource_type == 'mandates':
                    if event.links.mandate and len(event.links.mandate) != 0:
                        mandate = Mandates.get(event.links.mandate)
                        mandate_1 = (vars(mandate))
                        mandate_2 = mandate_1['attributes']

                        EnsekAccountId = None
                        StatementId = None
                        if 'AccountId' in mandate_2['metadata'].keys():
                            EnsekAccountId = mandate_2['metadata']['AccountId']
                        if 'StatementId' in mandate_2['metadata'].keys():
                            StatementId = mandate_2['metadata']['StatementId']

                        print(mandate_2['id'])
                        mandate_id = mandate_2['id']
                        CustomerId = mandate_2['links']['customer']
                        new_mandate_id = None
                        if 'new_mandate' in mandate_2['links'].keys():
                            new_mandate_id = mandate_2['links']['new_mandate']
                        created_at = mandate_2['created_at']
                        next_possible_charge_date = mandate_2['next_possible_charge_date']
                        payments_require_approval = mandate_2['payments_require_approval']
                        reference = mandate_2['reference']
                        scheme = mandate_2['scheme']
                        status = mandate_2['status']
                        creditor = mandate_2['links']['creditor']
                        customer_bank_account = mandate_2['links']['customer_bank_account']
                        mandate_update = event.created_at
                        EnsekID = EnsekAccountId
                        EnsekStatementId = StatementId

                        filenameMandates = 'go_cardless_mandates_' + mandate_id + '.csv'

                        mandate_listRow = [mandate_id, CustomerId, new_mandate_id, created_at, next_possible_charge_date,
                                   payments_require_approval,
                                   reference, scheme, status, creditor, customer_bank_account, EnsekID, EnsekStatementId]


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
    def process_Subscriptions(self, _StartDate = None, _EndDate = None):
        EventsfileDirectory = self.EventsFileDirectory
        SubscriptionsfileDirectory = self.SubscriptionsFileDirectory
        MandatesfileDirectory = self.MandatesFileDirectory
        s3 = self.s3
        if _StartDate is None:
            _StartDate = '{:%Y-%m-%d}'.format(self.execStartDate)
        if _EndDate is None:
            _EndDate = '{:%Y-%m-%d}'.format(self.execEndDate)
        startdatetime = datetime.strptime(_StartDate, '%Y-%m-%d')
        Events = self.Events
        Mandates = self.Mandates
        Subscriptions = self.Subscriptions
        filenameEvents = 'go_cardless_events_' + _StartDate + '_' + _EndDate + '.csv'
        qtr = math.ceil(startdatetime.month / 3.)
        yr = math.ceil(startdatetime.year)
        fkey = 'timestamp=' + str(yr) + '-Q' + str(qtr) + '/'
        print('Listing Subscriptions.......')
        # Loop through a page
        q_Subscription = Queue()
        subscription_datalist = []
        StartDate = _StartDate + "T00:00:00.000Z"
        EndDate = _EndDate + "T00:00:00.000Z"
        print(_StartDate, _EndDate)
        try:
            for event in Events.all(
                    params={"created_at[gte]": StartDate, "created_at[lte]": EndDate, "resource_type": "subscriptions" }):

                #### SUBSCRIPTIONS #####
                if event.resource_type == 'subscriptions':
                    if event.links.subscription and len(event.links.subscription) != 0:
                        subscription = Subscriptions.get(event.links.subscription)
                        subscription_1 = (vars(subscription))
                        subscription_2 = subscription_1['attributes']

                        charge_date = None
                        amount_subscription = None
                        mandate = None

                        print(subscription_2['id'])
                        id = subscription_2['id']
                        upcoming_payments = subscription_2['upcoming_payments']
                        if len(upcoming_payments) > 0:
                            charge_date = upcoming_payments[0]['charge_date']
                            amount_subscription = upcoming_payments[0]['amount']
                        created_at = subscription_2['created_at']
                        amount = subscription_2['amount']
                        currency = subscription_2['currency']
                        status = subscription_2['status']
                        name = subscription_2['name']
                        start_date = subscription_2['start_date']
                        end_date = subscription_2['end_date']
                        interval = subscription_2['interval']
                        interval_unit = subscription_2['interval_unit']
                        day_of_month = subscription_2['day_of_month']
                        month = subscription_2['month']
                        count_no = subscription_2['count']
                        payment_reference = subscription_2['payment_reference']
                        app_fee = subscription_2['app_fee']
                        retry_if_possible = subscription_2['retry_if_possible']
                        # earliest_charge_date_after_resume = subscription.earliest_charge_date_after_resume
                        if subscription_2['links']['mandate']:
                            mandate = subscription_2['links']['mandate']
                        subscription_update = event.created_at

                        subscription_listRow = [id, created_at, amount, currency, status, name, start_date,
                                   end_date, interval, interval_unit, day_of_month, month,
                                   count_no, payment_reference, app_fee, retry_if_possible, mandate, charge_date,
                                   amount_subscription ]

                        q_Subscription.put(subscription_listRow)

        except (json.decoder.JSONDecodeError, gocardless_pro.errors.GoCardlessInternalError,
                gocardless_pro.errors.MalformedResponseError) as e:
            pass

        while not q_Subscription.empty():
            subscription_datalist.append(q_Subscription.get())

        df = pd.DataFrame(subscription_datalist,
                          columns=['id', 'created_at', 'amount', 'currency', 'status', 'name', 'start_date',
                                   'end_date', 'interval', 'interval_unit', 'day_of_month', 'month',
                                   'count_no', 'payment_reference', 'app_fee', 'retry_if_possible', 'mandate',
                                   'charge_date', 'amount_subscription'])

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
        q_payment = Queue()
        payment_datalist = []
        StartDate = _StartDate + "T00:00:00.000Z"
        EndDate = _EndDate + "T00:00:00.000Z"
        print(_StartDate, _EndDate)
        try:
            for event in Events.all(
                    params={"created_at[gte]": StartDate, "created_at[lte]": EndDate, "resource_type": "payments"}):

                #### Payments #####
                if event.resource_type == 'payments':
                    if event.links.payment and len(event.links.payment) != 0:
                        payment = Payments.get(event.links.payment)
                        payment_1 = (vars(payment))
                        payment_2 = payment_1['attributes']

                        EnsekAccountId= None
                        StatementId = None
                        amount_refunded = None
                        charge_date = None
                        currency= None
                        description = None
                        reference = None
                        status = None
                        payout = None
                        mandate = None
                        subscription = None

                        try:
                            EnsekAccountId = payment_2['metadata']['AccountId']
                        except KeyError:
                            pass
                        try:
                            StatementId = payment_2['metadata']['StatementId']
                        except KeyError:
                            pass
                        print(payment_2['id'])
                        id = payment_2['id']
                        amount = payment_2['amount']
                        try:
                            amount_refunded = payment_2['amount_refunded']
                        except KeyError:
                            pass
                        try:
                            charge_date = payment_2['charge_date']
                        except KeyError:
                            pass
                        try:
                            currency = payment_2['currency']
                        except KeyError:
                            pass
                        try:
                            description = payment_2['description']
                        except KeyError:
                            pass
                        try:
                            reference = payment_2['reference']
                        except KeyError:
                            pass
                        try:
                            status = payment_2['status']
                        except KeyError:
                            pass
                        try:
                            payout = payment_2['links']['payout']
                        except KeyError:
                            pass
                        try:
                            mandate = payment_2['links']['mandate']
                        except KeyError:
                            pass
                        try:
                            subscription = payment_2['links']['subscription']
                        except KeyError:
                            pass

                        created_at = payment_2['created_at']

                        EnsekID = EnsekAccountId
                        EnsekStatementId = StatementId
                        payment_listRow =[id, amount, amount_refunded, charge_date, created_at, currency, description,
                        reference, status, payout, mandate, subscription, EnsekID, EnsekStatementId]
                        q_payment.put(payment_listRow)

        except (json.decoder.JSONDecodeError, gocardless_pro.errors.GoCardlessInternalError,
                gocardless_pro.errors.MalformedResponseError) as e:
            pass

        while not q_payment.empty():
            payment_datalist.append(q_payment.get())

        df = pd.DataFrame(payment_datalist, columns=['id', 'amount', 'amount_refunded', 'charge_date', 'created_at',
                                             'currency', 'description', 'reference', 'status', 'payout', 'mandate',
                                             'subscription', 'EnsekID', 'StatementId'])

        remove_duplicates_df = df.drop_duplicates(subset=['id'], keep='first')
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
        s3 =  self.s3
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
            prefix_df=prefix_df.append(df)
        return prefix_df


    def WritePaymentFilesToS3(self, df):
        PaymentsfileDirectory = self.PaymentsFileDirectory
        s3=self.s3
        pdf =  df

        filename = 'go_cardless_Payments_file.csv'
        s3.key = PaymentsfileDirectory + filename
        df_string = pdf.to_csv(None, index=False)
        s3.key = "/go-cardless-api-payments-files/go_cardless_Payments_file.csv"
        print(s3.key)
        s3.set_contents_from_string(df_string)









    ''' REFUNDS '''
    def process_Refunds(self, _StartDate=None, _EndDate=None):
        EventsfileDirectory = self.EventsFileDirectory
        RefundsfileDirectory = self.RefundsFileDirectory
        s3 = self.s3
        if _StartDate is None:
            _StartDate = '{:%Y-%m-%d}'.format(self.execStartDate)
        if _EndDate is None:
            _EndDate = '{:%Y-%m-%d}'.format(self.execEndDate)
        startdatetime = datetime.strptime(_StartDate, '%Y-%m-%d')
        Events = self.Events
        Refunds = self.Refunds
        filenameEvents = 'go_cardless_events_' + _StartDate + '_' + _EndDate + '.csv'
        qtr = math.ceil(startdatetime.month / 3.)
        yr = math.ceil(startdatetime.year)
        fkey = 'timestamp=' + str(yr) + '-Q' + str(qtr) + '/'
        print('Listing Refunds.......')
        # Loop through a page
        q_refund = Queue()
        refund_datalist = []
        StartDate = _StartDate + "T00:00:00.000Z"
        EndDate = _EndDate + "T00:00:00.000Z"
        print(_StartDate, _EndDate)
        try:
            for event in Events.all(
                    params={"created_at[gte]": StartDate, "created_at[lte]": EndDate, "resource_type": "refunds"}):

                #### Refunds #####
                if event.resource_type == 'refunds':
                    if event.links.refund and len(event.links.refund) != 0:
                        refund = Refunds.get(event.links.refund)
                        refund_1 = (vars(refund))
                        refund_2 = refund_1['attributes']

                        EnsekID = None
                        EnsekStatementId = None
                        amount = None
                        created_at = None
                        currency = None
                        id = None
                        mandate = None
                        metadata = None
                        payment = None
                        reference = None
                        status = None

                        try:
                            EnsekID = refund_2['metadata']['AccountId']
                        except KeyError:
                            pass

                        print(refund_2['id'])
                        id = refund_2['id']
                        amount = refund_2['amount']
                        created_at = refund_2['created_at']

                        try:
                            amount_refunded = refund_2['amount_refunded']
                        except KeyError:
                            pass
                        try:
                            currency = refund_2['currency']
                        except KeyError:
                            pass
                        try:
                            mandate = refund_2['links']['mandate']
                        except KeyError:
                            pass
                        try:
                            metadata = refund_2['metadata']
                        except KeyError:
                            pass
                        try:
                            payment = refund_2['links']['payment']
                        except KeyError:
                            pass
                        try:
                            reference = refund_2['reference']
                        except KeyError:
                            pass
                        try:
                            status = refund_2['status']
                        except KeyError:
                            pass

                        Refund_listRow = [EnsekID, amount, created_at, currency, id,
                                               mandate, metadata, payment, reference, status]

                        q_refund.put(Refund_listRow)

        except (json.decoder.JSONDecodeError, gocardless_pro.errors.GoCardlessInternalError,
                gocardless_pro.errors.MalformedResponseError) as e:
            pass

        while not q_refund.empty():
            refund_datalist.append(q_refund.get())

        df = pd.DataFrame(refund_datalist, columns=['EnsekID', 'amount', 'created_at', 'currency','id',
                                                      'mandate','metadata', 'payment','reference', 'status'
                                                   ])

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





    def runDailyFiles(self):
        for single_date in self.daterange():
            start = single_date
            end = self.get_date(start)
            ## print(start, end)
            ### Execute Job ###
            self.process_Events(start, end)


if __name__ == "__main__":
    freeze_support()
    s3 = db.get_finance_S3_Connections_client()
    ### StartDate & EndDate in YYYY-MM-DD format ###
    ### When StartDate & EndDate is not provided it defaults to SysDate and Sysdate + 1 respectively ###
    ### 2019-05-29 2019-05-30 ###
    ## p = GoCardlessMandatesSubscriptions('2020-04-18', '2020-04-19')
    p = GoCardlessEvents()

    ### EVENTS ###
    p1 = p.process_Events()

    ### MANDATES ###
    df_mandates = p.process_Mandates()
    pMandates = p.Multiprocess_Event(df=df_mandates, method=p.writeCSVs_Mandates)

    ### SUBSCRIPTIONS ###
    df_subscriptions = p.process_Subscriptions()
    pSubscriptions = p.Multiprocess_Event(df=df_subscriptions, method=p.writeCSVs_Subscriptions)

    ### PAYMENTS ###
    df_payments = p.process_Payments()
    pPaymentsFiles = p.Update_Events_Driven_Payments(df2=df_payments)


    ## REFUNDS ##
    df_refunds = p.process_Refunds()
    pRefunds = p.Multiprocess_Event(df=df_refunds, method=p.writeCSVs_Refunds)









