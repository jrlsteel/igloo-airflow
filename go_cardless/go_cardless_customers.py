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

# Fetch a client by its ID
client1 = client.customers

# Loop through a page of clients, printing each client's amount
print('.....listing clients')
df = pd.DataFrame()

for client in client.customers.all(params={"created_at[gte]": "2020-01-01T00:00:00.000Z", "created_at[lte]": "2020-03-01T00:00:00.000Z"}):
    EnsekAccountId = ''
    if client.metadata:
        if 'ensekAccountId' in client.metadata:
            EnsekAccountId = client.metadata['ensekAccountId']
    ## print(client.id)
    client_id = client.id
    created_at = client.created_at
    email = client.email
    given_name = client.given_name
    family_name = client.family_name
    company_name = client.company_name
    country_code = client.country_code
    EnsekID = EnsekAccountId
    listRow = [client_id, created_at, email, given_name, family_name, company_name, country_code, EnsekID]
    q.put(listRow)
    data = q.queue
for d in data:
    datalist.append(d)
# Empty queue
with q.mutex:
    q.queue.clear()


df = pd.DataFrame(datalist, columns=['client_id', 'created_at', 'email', 'given_name', 'family_name',
                                     'company_name', 'country_code', 'EnsekID'])

print(df.head(50))


df.to_csv('go_cardless_clients_202001_202002.csv', encoding='utf-8', index=False)

