import sys

sys.path.append('..')

from common import utils as util

import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import numpy as np
import json
from datetime import datetime

from process_smart.process_smart_reads_billing import SmartReadsBillings

from freezegun import freeze_time



# These are the columns that appear in vw_etl_smart_billing_reads_elec and vw_etl_smart_billing_reads_gas.
elec_data_columns = ['manualmeterreadingid', 'accountid', 'meterreadingdatetime', 'metertype',
                     'meterpointnumber', 'meter', 'register', 'reading', 'source', 'createdby', 'next_bill_date']

# Create some simple sample data - simulate a single row in the elec view.
elec_data = [
    ['30-EB-5A-FF-FF-0C-54-AB', 1831, datetime.strptime('2020-10-11 00:00:00.000000', '%Y-%m-%d %H:%M:%S.%f'), 'Electricity',
        2102, 115976, 125544, 8006.472, 'SMART', None, datetime.strptime('2020-10-17 00:00:00.000000', '%Y-%m-%d %H:%M:%S.%f')],
]


# Create some mocks for util functions that the SmartReadsBillings class depends on
def mock_util_get_api_info(api=None, auth_type=None, token_required=False, header_type=None):
    return '', '', ''


def mock_util_get_smart_read_billing_api_info(api):
    return 'https://example.org', {}


class TestSmartReadsBillings(unittest.TestCase):

    # Patch in our mocks for the util functions
    @patch("common.utils.get_api_info", mock_util_get_api_info)
    @patch("common.utils.get_smart_read_billing_api_info", mock_util_get_smart_read_billing_api_info)

    # And freeze the time to make unit testing easier
    @freeze_time("2018-04-03T12:30:00.123456+01:00")
    def test_process_accounts_1(self):
        srb = SmartReadsBillings()

        df = pd.DataFrame(elec_data, columns=elec_data_columns)

        # Install a MagicMock for the post_api_response method. This allows us to intercept
        # calls to it, return a specific value, and then later verify exactly the arguments
        # it was called with.
        srb.post_api_response = MagicMock(return_value=(201, {}))

        # Make our call to processAccounts, and then verify that it calls post_api_response
        # the expected number of times, with the expected arguments.
        srb.processAccounts(df)

        srb.post_api_response.assert_called_once()

        kargs = srb.post_api_response.call_args[0]

        self.assertEqual(kargs[0], 'https://example.org')
        self.assertDictEqual(json.loads(kargs[1]), {
            'accountId': 1831,
            'createdBy': None,
            'dateCreated': '2018-04-03T11:30:00.123456+00:00',
            'meter': 115976,
            'meterPointNumber': 2102,
            'meterReadingDateTime': '2020-10-11T00:00:00+00:00',
            'meterType': 'Electricity',
            'reading': 8006.472,
            'register': 125544,
            'source': 'SMART'
        })


if __name__ == '__main__':
    unittest.main()
