import os
import gocardless_pro
import pandas as pd
from queue import Queue
import queue
import numpy as np
import json
import requests

def is_json(myjson):
  try:
    json_object = json.loads(myjson)
  except ValueError as e:
    return False
  return True


client = gocardless_pro.Client(access_token='live_yk11i3mKMHN454DYUjIR0Mw6y34ePfW1GJoEXHaS', environment='live')
# Loop through a page of payments, printing each payment's amount
q = Queue()
datalist = []

# Fetch a payment by its ID
events = client.events

# Loop through a page of payments, printing each payment's amount
df = pd.DataFrame()
print('.....listing events')
for event in events.all(params={"created_at[gt]": "2020-01-01T00:00:00.000Z", "created_at[lt]": "2020-04-01T00:00:00.000Z"}):
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


df.to_csv('go_cardless_events_202001_202003.csv', encoding='utf-8', index=False)



