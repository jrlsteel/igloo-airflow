import os
import gocardless_pro
import pandas as pd
from queue import Queue
import queue
import numpy as np
import json

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
for event in events.all(params={"created_at[gte]": "2020-01-01T00:00:00.000Z", "created_at[lte]": "2020-01-03T00:00:00.000Z"}):
    ## print(event.id)
    if event.metadata:
        if 'AccountId' in event.metadata:
            EnsekAccountId = event.metadata['AccountId']
        if 'StatementId' in event.metadata:
            StatementId = event.metadata['StatementId']

    if event.links.mandate:
        mandate = event.links.mandate
    if event.links.new_customer_bank_account:
        new_customer_bank_account = event.links.new_customer_bank_account
    if event.links.new_mandate:
        new_mandate = event.links.new_mandate
    if event.links.organisation:
        organisation = event.links.organisation
    if event.links.parent_event:
        parent_event = event.links.parent_event
    if event.links.payment:
        payment = event.links.payment
    if event.links.payout:
        payout = event.links.payout
    if event.links.previous_customer_bank_account:
        previous_customer_bank_account = event.links.previous_customer_bank_account
    if event.links.refund:
        refund = event.links.refund
    if event.links.subscription:
        subscription = event.links.subscription

    id = event.id
    created_at = event.created_at
    resource_type = event.resource_type
    action = event.action
    customer_notifications = event.customer_notifications
    cause = event.details.cause
    description = event.details.description
    origin = event.details.origin
    reason_code = event.details.reason_code
    scheme = event.details.scheme
    will_attempt_retry = event.details.will_attempt_retry
    EnsekID = EnsekAccountId
    EnsekStatementId = StatementId
    listRow = [id, created_at, resource_type, action, customer_notifications, cause, description, origin,
               reason_code, scheme, will_attempt_retry,
               mandate , new_customer_bank_account , new_mandate ,organisation , parent_event , payment ,
               payout, previous_customer_bank_account ,refund ,subscription ,
               EnsekID, EnsekStatementId]
    q.put(listRow)
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
                                     'subscription' , 'EnsekID', 'StatementId'])

print(df.head(5))



