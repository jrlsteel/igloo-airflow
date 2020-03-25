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

# Fetch a payout by its ID
payout = client.payouts

# Loop through a page of payments, printing each payment's amount
print('.....listing payouts')
df_payout = pd.DataFrame()
for payout in client.payouts.all(params={"created_at[gte]": "2017-04-01T00:00:00.000Z", "created_at[lte]": "2017-07-01T00:00:00.000Z"}):

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
                                      'payout_type','reference', 'status', 'creditor', 'creditor_bank_account'
                                     ])


df.to_csv('go_cardless_payouts_201704_201706.csv', encoding='utf-8', index=False)