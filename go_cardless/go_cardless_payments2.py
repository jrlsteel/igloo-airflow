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
payment = client.payments

# Loop through a page of payments, printing each payment's amount
df = pd.DataFrame()
print('.....listing payments')
for payment in client.payments.all(params={"created_at[gte]": "2020-01-01T00:00:00.000Z", "created_at[lte]": "2020-02-01T00:00:00.000Z"}):
    EnsekAccountId = ''
    StatementId = ''
    if is_json(payment.id):
        '''
        if payment.metadata:
            if 'AccountId' in payment.metadata:
                EnsekAccountId = json.loads(payment.metadata['AccountId'].decode("utf-8"))
            if 'StatementId' in payment.metadata:
                StatementId = json.loads(payment.metadata['StatementId'].decode("utf-8"))
        '''
        id_js = json.loads(payment.id.decode("utf-8"))
        print(payment.id.decode("utf-8"))

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
        '''
        listRow = [id_js, amount_js, amount_refunded_js, charge_date_js, created_at_js, currency_js, description_js,
                   reference_js, status_js, payout_js, mandate_js, subscription_js, EnsekID_js, EnsekStatementId_js ]
        q.put(listRow)
        '''
    else:
        if payment.metadata:
            if 'AccountId' in payment.metadata:
                EnsekAccountId = payment.metadata['AccountId']
            if 'StatementId' in payment.metadata:
                StatementId = payment.metadata['StatementId']
        ## print(payment.id)
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
        listRow = [id, amount, amount_refunded, charge_date, created_at, currency, description,
                   reference, status, payout, mandate, subscription, EnsekID, EnsekStatementId ]
        q.put(listRow)

while not q.empty():
    datalist.append(q.get())

df = pd.DataFrame(datalist, columns=['id','amount', 'amount_refunded', 'charge_date','created_at',
                                     'currency', 'description', 'reference', 'status', 'payout', 'mandate',
                                     'subscription','EnsekID', 'StatementId'])

print(df.head(5))
df.to_csv('go_cardless_payments_2020_01.csv', encoding='utf-8', index=False)

