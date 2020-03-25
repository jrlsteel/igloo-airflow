import os
import gocardless_pro
import pandas as pd
import numpy as np
from queue import Queue
import queue


client = gocardless_pro.Client(access_token='live_yk11i3mKMHN454DYUjIR0Mw6y34ePfW1GJoEXHaS', environment='live')
# Loop through a page
q = Queue()
datalist = []

# Fetch a mandate by its ID
mandate = client.mandates

# Loop through a page of payments, printing each payment's amount
print('.....listing mandates')
df_mandate = pd.DataFrame()
for mandate in client.mandates.all():
    EnsekAccountId = ''
    StatementId = ''
    print(mandate.id)
    if 'AccountId' in mandate.metadata:
        EnsekAccountId = mandate.metadata['AccountId']
    if 'StatementId' in mandate.metadata:
        StatementId = mandate.metadata['StatementId']
    mandate_id = mandate.id
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

    listRow = [mandate_id, CustomerId, new_mandate_id, created_at, next_possible_charge_date, payments_require_approval,
               reference, scheme, status, creditor, customer_bank_account, EnsekID, EnsekStatementId]
    q.put(listRow)

while not q.empty():
    datalist.append(q.get())
df = pd.DataFrame(datalist, columns=['mandate_id', 'CustomerId', 'new_mandate_id', 'created_at', 'next_possible_charge_date',
                                     'payments_require_approval', 'reference', 'scheme', 'status', 'creditor', 'customer_bank_account',
                                     'EnsekID', 'EnsekStatementId'])


df.to_csv('go_cardless_mandates2.csv', encoding='utf-8', index=False)