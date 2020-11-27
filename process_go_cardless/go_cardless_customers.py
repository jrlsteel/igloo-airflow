import os
import timeit
import pandas as pd
import numpy as np
import requests
import math
from datetime import datetime, date, time, timedelta

import gocardless_pro
from multiprocessing import freeze_support

from queue import Queue
from pathlib import Path

import sys

sys.path.append('..')

from common import utils as util
from conf import config as con
from connections.connect_db import get_finance_s3_connections as s3_con
from connections import connect_db as db

client = gocardless_pro.Client(access_token=con.go_cardless['access_token'],
                               environment=con.go_cardless['environment'])
clients = client.customers

class GoCardlessCustomers(object):

    def __init__(self, logger=None):

        if logger is None:
            self.iglog = util.IglooLogger()
        else:
            self.iglog = logger

        dir = util.get_dir()

        self.file_directory = dir['s3_finance_goCardless_key']['Clients']
        self.s3_key = self.file_directory
        self.bucket_name = dir['s3_finance_bucket']
        self.s3 = s3_con(self.bucket_name)
        self.sql = 'select max(created_at) as lastRun from aws_fin_stage1_extracts.fin_go_cardless_api_clients '
        self.exec_end_date = datetime.now().replace(microsecond=0).isoformat()
        self.customer_columns = util.get_common_info('go_cardless_column_order', 'customers')

    def get_customers_by_date(self, start_date, end_date):

        return clients.all(params={"created_at[gt]": start_date , "created_at[lte]": end_date })

    def store_customers(self, customers):

        for customer in customers:
            customer_data = {
                "client_id": customer.id,
                "created_at": customer.created_at,
                "email": customer.email,
                "given_name": customer.given_name,
                "family_name": customer.family_name,
                "company_name": customer.company_name,
                "country_code": customer.country_code,
                "EnsekID": customer.metadata.get('ensekAccountId')
            }

            customer_object_name = customer.id + '.csv'
            customer_df = pd.DataFrame(data=[customer_data], columns=self.customer_columns)
            customer_csv_string = customer_df.to_csv(None, columns=self.customer_columns, index=False)

            s3_path = self.s3_key + customer_object_name
            self.iglog.in_prod_env("writing data to {}".format(s3_path))
            self.s3.key = s3_path
            self.s3.set_contents_from_string(customer_csv_string)

if __name__ == "__main__":
    freeze_support()
    s3 = db.get_finance_s3_connections_client()
    p = GoCardlessCustomers()
    start_date_df = util.execute_query(p.sql)
    end_date = str(p.exec_end_date) + str(".000Z")
    start_date = str(start_date_df.iat[0,0])

    customers = p.get_customers_by_date(start_date=start_date, end_date=end_date)
    p.store_customers(customers)


