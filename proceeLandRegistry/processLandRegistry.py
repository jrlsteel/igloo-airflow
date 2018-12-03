import os
import sys
import pandas_redshift as pr
import psycopg2
import requests
import json
from pandas.io.json import json_normalize

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from conf import config as con


global SEARCH_URL
# SEARCH_URL = 'http://landregistry.data.gov.uk/data/ppi/address'
SEARCH_URL = "http://landregistry.data.gov.uk/data/ppi/transaction-record"  # &propertyAddress.county=DERBYSHIRE&newBuild=false



def get_addresses(table_name, schema):
    # sql to count
    sql = '''SELECT id, sub_building_name_number, building_name_number, dependent_thoroughfare, thoroughfare, double_dependent_locality, dependent_locality, post_town, county, postcode, uprn, created_at, updated_at FROM ref_cdb_addresses limit 10''' #+ schema + table_name
    addresses_df = pr.redshift_to_pandas(sql)
    #print(addresses_df)
    return addresses_df


def search_lr(street=False, postcode=False, paon=False, saon=False, county=False):
    '''
        street = e.g. Cranbrook Road
        postcode = FULL POSTCODE ONLY
        PAON - primary address - e.g. house number or building
        SAON - secondary address e.g. flat number
        county - county
    '''

    if not any([street, postcode]):
        print('Please Supply a Street or Postcode')
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

    #print('Query: ', req.prepare().url)

    if response.status_code == 200:

        js = response.json()
        # for k in js.keys():
        #    print k, js[k], '\n'
        #print(response.json())
        return response.json()

    else:
        print('Problem Grabbing Data: ', response.status_code)
        return {}


def search_igloo_addresses(address_data):
    # print(type(address_data))
    landreg_list = []
    for row, address in address_data.iterrows():
        # print customer
        # street = e.g. Cranbrook Road
        # postcode = FULL POSTCODE ONLY
        # PAON - primary address - e.g. house number or building
        # SAON - secondary address e.g. flat number#
        # uprn
        uprn = address['uprn']


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
        price_paid_data = search_lr(street=street, postcode=postcode, paon=paon, saon=saon)
        print(type(price_paid_data))
        price_paid_result = price_paid_data['result']
        items = price_paid_result['items']
        if items:
            landreg_df = json_normalize(items)
            landreg_list.append(landreg_df)

    return landreg_list


def get_redshift_connection():
    try:
        pr.connect_to_redshift(host=con.redshift_config['host'], port=con.redshift_config['port'],
                               user=con.redshift_config['user'], password=con.redshift_config['pwd'],
                               dbname=con.redshift_config['db'])
        print("Connected to Redshift")
    except ConnectionError as e:
        sys.exit("Error : " + str(e))


def process_addresses():

    get_redshift_connection()
    # list of staging table names to get count
    ref_tables = [
        "ref_cdb_addresses"
    ]
    stage_schema = "igloosense-uat"
    stage_counts = []

    try:
        address_dfs = []
        for table in ref_tables:
            address_df = get_addresses(table, stage_schema)
            # address_dfs.append()
            print(address_df)
        return address_df

    except ConnectionError as e:
        raise Exception("Connection Error : " + str(e))

    except:
        raise

    finally:
        pr.close_up_shop()
        print("Connection to Redshift Closed")


if __name__ == '__main__':

    process_addresses_df = process_addresses()
    print(process_addresses_df)
    # print(type(process_addresses_df))
    landregistry_df = search_igloo_addresses(process_addresses_df)

    with open('landreg', 'w') as f:
        for landregistry in landregistry_df:
            landreg_str = landregistry.to_string()
            f.write(landreg_str)
