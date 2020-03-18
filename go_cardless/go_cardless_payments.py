import os
import gocardless_pro
import pandas as pd
import numpy as np


client = gocardless_pro.Client(access_token='live_yk11i3mKMHN454DYUjIR0Mw6y34ePfW1GJoEXHaS', environment='live')
# Loop through a page of payments, printing each payment's amount

# Fetch a payment by its ID
payment = client.payments

# Loop through a page of payments, printing each payment's amount
df = pd.DataFrame()
print('.....listing payments')
for payment in client.payments.all(params={"created_at[gte]": "2020-02-10T00:00:00.000Z", "created_at[lte]": "2020-02-11T00:00:00.000Z"}):
    df = df.append({'id': payment.id, 'amount': payment.amount, 'amount_refunded': payment.amount_refunded,
                   'charge_date': payment.charge_date, 'created_at' : payment.created_at,
                    'currency': payment.currency, 'description': payment.description, 'charge_date': payment.charge_date,
                   'reference': payment.reference, 'status': payment.status,
                   'payout': payment.links.payout, 'mandate': payment.links.mandate, 'subscription': payment.links.subscription
                    }, ignore_index=True)

print(df.head(5))






