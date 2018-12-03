# coding: utf-8

# In[5]:


import psycopg2
import requests
import pandas as pd
from StringIO import StringIO
import json

# info at http://landregistry.data.gov.uk/api-config
# and better at - http://landregistry.data.gov.uk/app/root/doc/ppd

import json

with open('/etc/igloo_connect.txt') as filename:
    PASSWORD = filename.read().replace('\n', '')

global DIR
DIR = '~/Dropbox/notebooks/Igloo/datastore'
# DIR = '/'

global SEARCH_URL
# SEARCH_URL = 'http://landregistry.data.gov.uk/data/ppi/address'
SEARCH_URL = "http://landregistry.data.gov.uk/data/ppi/transaction-record"  # &propertyAddress.county=DERBYSHIRE&newBuild=false

# In[3]:


HOST = 'localhost'
PORT = 22  # redshift default
# USER = 'igloo'
# DATABASE = 'igloosense'

# HOST = 'igloowarehouse.cqnmyrrkwt1y.eu-west-1.redshift.amazonaws.com'
# PORT = 5439 # redshift default
USER = 'igloo'
DATABASE = 'igloosense-uat'


def db_connection():
    conn = psycopg2.connect(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=DATABASE,
    )
    return conn


# In[4]:


if __name__ == '__main__':
    # GRAB ALL THE DATA

    conn = db_connection()

    # ref_addresses
    query = 'select * from ref_cdb_addresses'  # limit 2000'
    address_data = pd.read_sql_query(query, conn)

    conn.close()


# In[34]:


def search_lr(street=False, postcode=False, paon=False, saon=False, county=False):
    '''
        street = e.g. Cranbrook Road
        postcode = FULL POSTCODE ONLY
        PAON - primary address - e.g. house number or building
        SAON - secondary address e.g. flat number
        county - county

    '''

    if not any([street, postcode]):
        print
        'Please Supply a Street or Postcode'
        return

    query = {}

    if street:
        query['propertyAddress.street'] = street.upper()  # .replace(' ', '')

    if postcode:
        query['propertyAddress.postcode'] = postcode

    if paon:
        query['propertyAddress.paon'] = paon

    if saon:
        query['propertyAddress.saon'] = saon

    if county:
        query['propertyAddress.county'] = county.upper()

    headers = {'accept': "application/json"}

    req = requests.Request("GET", SEARCH_URL, headers=headers, params=query)
    response = requests.request("GET", SEARCH_URL, headers=headers, params=query)

    print
    'Query: ', req.prepare().url

    if response.status_code == 200:

        js = response.json()
        # for k in js.keys():
        #    print k, js[k], '\n'

        return response.json()

    else:
        print
        'Problem Grabbing Data: ', response.status_code
        return {}


if __name__ == '_main__':
    # search_epc()
    # search_epc(address = 'Poole')
    # search_epc(address = 'Poole', postcode = 'BH12')
    # search_epc(postcode = 'BH12')

    price_paid_data = search_lr(street="Cranbrook Road", postcode='BH123HZ', paon='161')

    print
    json.dumps(price_paid_data, indent=2)


# http://landregistry.data.gov.uk/data/ppi/address?&street=PEAR%20TREE%20STREET&county=DERBYSHIRE


# In[83]:


# In[85]:


def search_igloo_addresses(address_data, res_in):
    res = res_in

    c = 0
    for row, address in address_data[:2].iterrows():
        # print customer

        # street = e.g. Cranbrook Road
        # postcode = FULL POSTCODE ONLY
        # PAON - primary address - e.g. house number or building
        # SAON - secondary address e.g. flat number#

        # uprn
        uprn = address['uprn']

        if uprn is None:
            uprn = pc + '_no_uprn'

        if uprn in res_in.keys():
            c += 1
            continue

        # grab the address data and search the land registry
        street = address['thoroughfare']

        # add a space into the postcode
        # ARE ALL POSTCODES X + 3 digits?
        postcode = address['postcode'][:-3] + ' ' + address['postcode'][-3:]

        # the the number
        paon = address['building_name_number']

        # get any sub number e.g. address
        saon = address['sub_building_name_number']

        # extract the estate type, property type and transaction date

        print
        'Searching: ', saon, paon, street, postcode

        price_paid_data = search_lr(street=street, postcode=postcode, paon=paon, saon=saon)

        price_paid_result = price_paid_data['result']
        items = price_paid_result['items']

        print
        'Items: ', len(items)

        if len(items) == 0:
            print
            '### No Result ###'

            res[uprn] = [{'uprn': uprn,
                          'transactionDate': None,
                          'newBuild': None,
                          'pricePaid': None,
                          'propertyType': None,
                          'recordStatus': None,
                          'transactionId': None,
                          'transactionCategory': None}]


        elif len(items) == 1:
            print
            '### One Record ###'

        elif len(items) > 1:
            print
            '### Multiple Records ###'

        proc_items = []
        for pp_record in items:
            rec = {}
            rec['urpn'] = uprn
            rec['transactionDate'] = pp_record['transactionDate']
            rec['newBuild'] = pp_record['newBuild']
            rec['pricePaid'] = pp_record['pricePaid']
            rec['propertyType'] = pp_record['propertyType']['prefLabel'][0]['_value']
            rec['recordStatus'] = pp_record['recordStatus']['prefLabel'][0]['_value']
            rec['transactionId'] = pp_record['transactionId']
            rec['transactionCategory'] = pp_record['transactionCategory']['prefLabel'][0]['_value']
            proc_items.append(rec)

        res[uprn] = proc_items

        c += 1

        # update and save data every 50
        if c % 50 == 0:
            print
            '---------', c, ' COMPLETE -------'
            json.dump(res, open(DIR + '/land_reg_matches.json', 'w'))

        # for k in price_paid_data.keys():
        #    print '####', k, '####'
        #    print price_paid_data[k]


if __name__ == '__main__':

    # res_clean = json.load(open(DIR + '/epc_matches.json','r'))
    # print len(res_clean)
    try:
        res_in = json.load(res, open(DIR + '/land_reg_matches.json', 'r'))
    except:
        res_in = {}

    res = search_igloo_addresses(address_data, res_in)


# price_paid_data = search_lr(street = "Cranbrook Road", postcode = 'BH12 3HZ', paon = '161')


# In[63]:


# In[16]:


# price_paid_data['result']['items'] is all the addresses
# for key in price_paid_data['result']['items']:
#    print json.dumps(key, indent=5)
#    print


# In[7]:


def search_lr_add(street=False, postcode=False, paon=False, saon=False):
    '''
        street = e.g. Cranbrook Road
        postcode = FULL POSTCODE ONLY
        PAON - primary address - e.g. house number or building
        SAON - secondary address e.g. flat number

    '''

    if not any([street, postcode]):
        print
        'Please Supply a Street or Postcode'
        return

    query = {}

    if street:
        query['street'] = street.upper()  # .replace(' ', '')

    if postcode:
        query['postcode'] = postcode

    if paon:
        query['paon'] = paon

    if saon:
        query['saon'] = saon

    headers = {'accept': "application/json"}

    req = requests.Request("GET", SEARCH_URL, headers=headers, params=query)
    response = requests.request("GET", SEARCH_URL, headers=headers, params=query)

    print
    req.prepare().url

    if response.status_code == 200:

        js = response.json()
        for k in js.keys():
            print
            k, js[k], '\n'

        return response.json()
    else:
        print
        'Problem Grabbing Data: ', response.status_code
        return {}


if __name__ == '_main__':
    # search_epc()
    # search_epc(address = 'Poole')
    # search_epc(address = 'Poole', postcode = 'BH12')
    # search_epc(postcode = 'BH12')

    price_paid_data = search_lr(street="Cranbrook Road", postcode='BH12 3HZ', paon='161')

# http://landregistry.data.gov.uk/data/ppi/address?&street=PEAR%20TREE%20STREET&county=DERBYSHIRE


# In[8]:


# In[ ]:


if __name__ == '__main__':


