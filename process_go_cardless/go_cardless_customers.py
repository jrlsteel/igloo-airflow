import sys
import traceback
import pandas as pd

import gocardless_pro

sys.path.append('..')

from common import utils as util
from conf import config as con
from connections.connect_db import get_finance_s3_connections as s3_con
from connections import connect_db as db

api = gocardless_pro.Client(access_token=con.go_cardless['access_token'],
                               environment=con.go_cardless['environment'])
gc_customer_api = api.customers

class GoCardlessCustomers(object):

    def __init__(self, logger=None):

        if logger is None:
            self.iglog = util.IglooLogger(source='GoCardlessCustomers')
        else:
            self.iglog = logger

        dir = util.get_dir()

        self.file_directory = dir['s3_finance_goCardless_key']['Clients']
        self.s3_key = self.file_directory
        self.bucket_name = dir['s3_finance_bucket']
        self.s3 = s3_con(self.bucket_name)
        self.customer_columns = util.get_common_info('go_cardless_column_order', 'customers')

    def get_customer_ids_from_view(self):

        return util.get_ids_from_redshift(entity_type='customer', job_name='go_cardless')

    def get_customer_from_id(self, customer_id, thread_name=None):

        self.iglog.in_test_env("Thread {}: Getting customer {}".format(thread_name, customer_id))

        try:
            return gc_customer_api.get(customer_id)
        except (
            json.decoder.JSONDecodeError,
            gocardless_pro.errors.GoCardlessInternalError,
            gocardless_pro.errors.MalformedResponseError,
            gocardless_pro.errors.InvalidApiUsageError,
            AttributeError):

            self.iglog.in_prod_env("error getting customer with id {customer_id}".format(customer_id))
            self.iglog.in_prod_env(traceback.format_exc())

    def store_customer(self, customer, thread_name=None):

        customer_data = [
            customer.id,
            customer.created_at,
            customer.email,
            customer.given_name,
            customer.family_name,
            customer.company_name,
            customer.country_code,
            customer.metadata.get('ensekAccountId')
        ]

        customer_object_name = customer.id + '.csv'
        customer_df = pd.DataFrame(data=[customer_data], columns=self.customer_columns)
        customer_csv_string = customer_df.to_csv(None, columns=self.customer_columns, index=False)

        s3_path = self.s3_key + customer_object_name
        self.iglog.in_test_env("Thread: {}: Writing data to {}".format(thread_name, s3_path))
        self.s3.key = s3_path
        self.s3.set_contents_from_string(customer_csv_string)

    def process_customers(self, customer_ids, thread_name=None):

        self.iglog.in_prod_env('Thread {}: Processing {} customer IDs'.format(thread_name, len(customer_ids)))

        for customer_id in customer_ids:
            customer = self.get_customer_from_id(customer_id, thread_name)
            if customer:
                self.store_customer(customer, thread_name)

        self.iglog.in_prod_env('Thread {}: Processing completed'.format(thread_name))

if __name__ == "__main__":

    num_processes = 2

    gc_processor = GoCardlessCustomers()

    customer_ids = gc_processor.get_customer_ids_from_view()
    # p.process_customers(customer_ids)
    util.run_api_extract_multithreaded(id_list=customer_ids, method=gc_processor.process_customers, num_processes=num_processes)


