import sys
import unittest
from unittest.mock import MagicMock, patch

from moto import mock_s3_deprecated
from freezegun import freeze_time
import pandas as pd

import go_cardless_customers as gcc_module
from go_cardless_customers import GoCardlessCustomers

# class for mocking the response from the go_cardless API
class Customer():
    def __init__(self, id, created_at, email, given_name, family_name, company_name, country_code, ensek_id):

        self.id = id
        self.created_at = created_at
        self.email = email
        self.given_name = given_name
        self.family_name = family_name
        self.company_name = company_name
        self.country_code = country_code
        self.metadata = {"ensekAccountId": ensek_id} if ensek_id else {}

# anonymised test data
test_customer = {
    'CU001111111111': Customer('CU001111111111', '2020-11-25T13:49:07.255Z', 'test.one@gmail.com', 'Abbie', 'Walker', None, 'GB', None),
    'CU002222222222': Customer('CU002222222222', '2020-11-25T13:48:47.149Z','test2@hotmail.com', 'James', 'Wilsom', None ,'GB', 123456),
    'CU003333333333': Customer('CU003333333333', '2020-11-25T13:45:07.025Z', 'testingtesting@yahoo.co.uk', 'Fred', 'Harris', None , 'GB', None)
}

csv_heading = 'client_id,created_at,email,given_name,family_name,company_name,country_code,EnsekID'
csv_outputs = [
    'CU001111111111,2020-11-25T13:49:07.255Z,test.one@gmail.com,Abbie,Walker,,GB,',
    'CU002222222222,2020-11-25T13:48:47.149Z,test2@hotmail.com,James,Wilsom,,GB,123456',
    'CU003333333333,2020-11-25T13:45:07.025Z,testingtesting@yahoo.co.uk,Fred,Harris,,GB,'
]

customer_ids = list(test_customer.keys())

def get_customer(customer_id, thread_name=None):

    return test_customer[customer_id]


class TestGoCardlessCustomers(unittest.TestCase):

    @mock_s3_deprecated
    def test_process_customers_by_ids(self):

        bucket_name = 'igloo-data-warehouse-uat-finance'

        boto = gcc_module.db.boto
        s3 = boto.connect_s3(aws_access_key_id='XXXX', aws_secret_access_key='XXXX')

        bucket = s3.create_bucket(bucket_name)
        s3_key = boto.s3.key.Key(bucket)

        gc_processor = GoCardlessCustomers()
        gc_processor.get_customer_from_id = MagicMock(side_effect=get_customer)

        gc_processor.process_customers(customer_ids)

        output_s3_key = gc_processor.s3_key

        for index, customer in enumerate(test_customer.values()):
            s3_key.key = output_s3_key + customer.id + '.csv'
            s3_object = s3_key.get_contents_as_string().decode('utf-8')
            csv_lines = s3_object.split('\n')
            self.assertEqual(csv_lines[0], csv_heading, '{} heading matches'.format(customer.id))
            self.assertEqual(csv_lines[1], csv_outputs[index], '{} data matches'.format(customer.id))

if __name__ == '__main__':
    unittest.main()
