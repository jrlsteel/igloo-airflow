from datetime import datetime, timedelta
from lxml import etree
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from moto import mock_s3_deprecated
import csv
import json
import os
import sys
import pandas
from unittest.mock import Mock, patch
import process_ensek_accounts
import statistics
from collections import defaultdict
from pandas.io.json import json_normalize
import requests
import responses


sys.path.append("../../")


from connections.connect_db import get_boto_S3_Connections as s3_con
from common import utils as util


@responses.activate
def test_api_response_one_page():
    with open(os.path.join(os.path.dirname(__file__), "fixtures", "data.json")) as f_in:
        data_json_1 = f_in.read()

    metrics = [
        {
            "api_error_codes": [],
            "api_method_time": [],
        },
    ]
    ensekaccounts = process_ensek_accounts.Accounts()
    url = "https://test.com"
    head = {"Content-Type": "application/json", "Authorization": "Bearer {0}".format("1")}
    account_id = "1865"

    responses.add(responses.GET, "{}".format(url), body=data_json_1, status=200)

    response = ensekaccounts.get_api_response(url, head, account_id, metrics)
    print(response)
    assert response == json.loads(data_json_1)
