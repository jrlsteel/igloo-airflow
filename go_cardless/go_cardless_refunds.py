import os
import gocardless_pro
import pandas as pd
import numpy as np

client = gocardless_pro.Client(access_token='live_yk11i3mKMHN454DYUjIR0Mw6y34ePfW1GJoEXHaS', environment='live')
# Loop through a page of refunds, printing each payment's amount
# Fetch a refund by its ID
refund = client.refunds

# Loop through a page of payments, printing each payment's amount
df = pd.DataFrame()
print('.....listing refunds')
for refund in client.refunds.all(params={"created_at[gte]": "2020-02-10T00:00:00.000Z", "created_at[lte]": "2020-02-20T00:00:00.000Z"}):
    df = df.append({'id': refund.id, 'amount': refund.amount, 'created_at': refund.created_at, 'currency': refund.currency,
                     'reference': refund.reference, 'status': refund.status, 'metadata':refund.metadata,
                     'payment': refund.links.payment, 'mandate': refund.links.mandate
                    }, ignore_index=True)

print(df.head(5))



