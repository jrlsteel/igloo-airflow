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
for payment in client.payments.all(params={"charge_date[gte]": "2020-03-01", "charge_date[lte]": "2020-03-31"}):
    EnsekAccountId = ''
    StatementId = ''
    try:

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
                   reference, status, payout, mandate, subscription, EnsekID, EnsekStatementId]
        q.put(listRow)
    except (json.decoder.JSONDecodeError, gocardless_pro.errors.GoCardlessInternalError,
            gocardless_pro.errors.MalformedResponseError) as e:
        pass
data = q.queue
for d in data:
    datalist.append(d)
# Empty queue
with q.mutex:
    q.queue.clear()


df = pd.DataFrame(datalist, columns=['id','amount', 'amount_refunded', 'charge_date','created_at',
                                     'currency', 'description', 'reference', 'status', 'payout', 'mandate',
                                     'subscription','EnsekID', 'StatementId'])

print(df.head(5))


df.to_csv('go_cardless_payments_202003.csv', encoding='utf-8', index=False)
##df.to_csv('go_cardless_payments_202003_202003.csv', encoding='utf-8', index=False)



