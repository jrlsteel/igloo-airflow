import os
import gocardless_pro
import pandas as pd
from queue import Queue
import queue
import numpy as np
import json



client = gocardless_pro.Client(access_token='live_yk11i3mKMHN454DYUjIR0Mw6y34ePfW1GJoEXHaS', environment='live')
# Loop through a page of payments, printing each payment's amount
q = Queue()
datalist = []

# Fetch a payment by its ID
subscriptions = client.subscriptions

# Loop through a page of payments, printing each payment's amount
df = pd.DataFrame()
print('.....listing subscriptions')
for subscription in subscriptions.all(params={"created_at[gte]": "2020-04-01T00:00:00.000Z", "created_at[lte]": "2020-04-06T00:00:00.000Z"}):
    test =  []
    charge_date = ''
    amount_subscription = ''
    mandate = ''
    id = subscription.id
    upcoming_payments = subscription.upcoming_payments
    if len(upcoming_payments) > 0:
        charge_date = upcoming_payments[0]['charge_date']
        amount_subscription = upcoming_payments[0]['amount']
    created_at = subscription.created_at
    amount =  subscription.amount
    currency = subscription.currency
    status =  subscription.status
    name =  subscription.name
    start_date = subscription.start_date
    end_date = subscription.end_date
    interval  = subscription.interval
    interval_unit = subscription.interval_unit
    day_of_month =  subscription.day_of_month
    month = subscription.month
    count_no =  subscription.count
    payment_reference = subscription.payment_reference
    app_fee = subscription.app_fee
    retry_if_possible = subscription.retry_if_possible
    #earliest_charge_date_after_resume = subscription.earliest_charge_date_after_resume
    if subscription.links.mandate:
        mandate = subscription.links.mandate

    listRow = [id , created_at ,amount , currency ,status, name ,start_date ,
                end_date, interval ,interval_unit ,day_of_month ,month,
                count_no , payment_reference, app_fee ,retry_if_possible , mandate, charge_date, amount_subscription]
    q.put(listRow)

while not q.empty():
    datalist.append(q.get())

df = pd.DataFrame(datalist, columns=['id' , 'created_at' ,'amount' , 'currency' ,'status', 'name' ,'start_date' ,
                                        'end_date', 'interval' ,'interval_unit' ,'day_of_month' ,'month',
                                        'count_no' , 'payment_reference', 'app_fee' , 'retry_if_possible' , 'mandate',
                                        'charge_date', 'amount_subscription' ])

print(df.head(5))

df.to_csv('go_cardless_subscriptions_2020_04_01_06.csv', encoding='utf-8', index=False)

