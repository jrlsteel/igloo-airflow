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
from process_ensek_internal_readings import InternalReadings
import statistics
from collections import defaultdict
from pandas.io.json import json_normalize
import requests
import responses

sys.path.append("../../")
from connections.connect_db import get_boto_S3_Connections as s3_con
from common import utils as util


@mock_s3_deprecated
def test_extract_data():
    s3_bucket_name = "simulated-bucket"

    s3_connection = S3Connection()
    s3_bucket = s3_connection.create_bucket(s3_bucket_name)

    api_error_codes = []
    connection_error_counter = 0
    number_of_retries_total = 0
    retries_per_account = defaultdict(int)
    api_method_time = []


    metrics = {}

    internalreadings = InternalReadings()

    # Get the test data
    data_file = open(
        os.path.join(os.path.dirname(__file__), "fixtures", "response-1831-page1.json")
    )
    json_string = data_file.read()

    data_file.close()
    data_json = json.loads(json_string)["items"]
    bucket_name = s3_con(s3_bucket_name)

    dir_s3 = {
        "s3_key": {
            "ReadingsInternal": "stage1/ReadingsInternal/",
        },
    }
    account_id = "test_id"


    internalreadings.extract_internal_data_response(
        data_json, account_id, bucket_name, dir_s3, metrics
    )

    expected_internal_readings = "stage1/ReadingsInternal/internal_readings_{}.csv".format(account_id)
    
    # Verify that the files created in S3 have the correct contents
    k = Key(s3_bucket)
    k.key = expected_internal_readings
    csv_ines_internalreadings = k.get_contents_as_string(encoding="utf-8")
    # Construct a CSV reader to parse the file data for us, and verify that the
    # column headers are correct.

    internalreadings_reader = csv.reader(csv_ines_internalreadings.split("\n"), delimiter=",")

    internal_readings_header_s3 = next(internalreadings_reader)

    internal_readings_column_headers = [
        'accountId', 'billable', 'hasLiveCharge', 'hasRegisterAdvance', 'meterId', 'meterPointId', 'meterPointNumber', 'meterPointType', 'meterReadingCreatedDate', 'meterReadingDateTime', 'meterReadingId', 'meterReadingSourceUID', 'meterReadingStatusUID', 'meterReadingTypeUID', 'meterSerialNumber', 'readingValue', 'registerId', 'registerReadingId', 'registerReference', 'required'
        ]

    assert (
        internal_readings_column_headers == internal_readings_header_s3
    ), "failed internal readings headers"


    assert list(internalreadings_reader) == [['1831', 'True', 'True', 'True', '115976', '2102', '2000007794886', 'E', '2021-05-12T11:55:26.17', '2021-05-12T00:00:00', '7661068', 'SMART', 'VALID', 'ACTUAL', '19L3357433', '13955.0', '125544', '8497341', '1', 'False'], ['1831', 'True', 'True', 'True', '115976', '2102', '2000007794886', 'E', '2021-04-12T11:26:51.887', '2021-04-12T00:00:00', '7321563', 'SMART', 'VALID', 'ACTUAL', '19L3357433', '13264.0', '125544', '8128803', '1', 'False'], ['1831', 'True', 'True', 'True', '115976', '2102', '2000007794886', 'E', '2021-03-12T11:16:54.753', '2021-03-12T00:00:00', '6978932', 'SMART', 'VALID', 'ACTUAL', '19L3357433', '12506.0', '125544', '7757845', '1', 'False'], ['1831', 'True', 'True', 'True', '115976', '2102', '2000007794886', 'E', '2021-03-05T08:26:38.967', '2021-03-04T00:00:00', '6885076', 'SMART', 'VALID', 'ACTUAL', '19L3357433', '12282.0', '125544', '7656656', '1', 'False'], ['1831', 'True', 'True', 'True', '115976', '2102', '2000007794886', 'E', '2021-03-03T17:18:55.767', '2021-03-03T00:00:00', '6861055', 'SMART', 'VALID', 'ACTUAL', '19L3357433', '12250.0', '125544', '7630523', '1', 'False'], ['1831', 'True', 'True', 'True', '115976', '2102', '2000007794886', 'E', '2021-02-15T18:18:05.717', '2021-02-12T00:00:00', '6560210', 'SMART', 'VALID', 'ACTUAL', '19L3357433', '11753.0', '125544', '7307507', '1', 'False'], ['1831', 'False', 'False', 'True', '115976', '2102', '2000007794886', 'E', '2021-02-07T20:45:24.637', '2021-02-08T00:00:00', '6285238', 'ESTIMATE', 'INVALID', 'ESTIMATED', '19L3357433', '11688.9', '125544', '7014223', '1', 'True'], ['1831', 'True', 'True', 'True', '115976', '2102', '2000007794886', 'E', '2021-02-15T18:18:08.66', '2021-02-08T00:00:00', '6560211', 'ESTIMATE', 'VALID', 'ESTIMATED', '19L3357433', '11645.9', '125544', '7307508', '1', 'True'], ['1831', 'True', 'True', 'True', '115976', '2102', '2000007794886', 'E', '2021-01-12T10:03:21.37', '2021-01-12T00:00:00', '5927222', 'SMART', 'VALID', 'ACTUAL', '19L3357433', '10923.0', '125544', '6629869', '1', 'False'], ['1831', 'True', 'True', 'True', '115976', '2102', '2000007794886', 'E', '2020-12-12T10:01:21.387', '2020-12-12T00:00:00', '5612772', 'SMART', 'VALID', 'ACTUAL', '19L3357433', '10129.0', '125544', '6291590', '1', 'False'], ['1831', 'True', 'True', 'True', '115976', '2102', '2000007794886', 'E', '2020-11-12T10:03:19.987', '2020-11-12T00:00:00', '5263493', 'SMART', 'VALID', 'ACTUAL', '19L3357433', '9261.0', '125544', '5914586', '1', 'False'], ['1831', 'False', 'False', 'True', '115976', '2102', '2000007794886', 'E', '2020-10-15T11:18:44.437', '2020-10-15T00:00:00', '4787294', 'ESTIMATE', 'INVALID', 'ESTIMATED', '19L3357433', '8103.5', '125544', '5405052', '1', 'True'], ['1831', 'True', 'True', 'True', '115976', '2102', '2000007794886', 'E', '2020-11-12T10:03:20.66', '2020-10-15T00:00:00', '5263494', 'ESTIMATE', 'VALID', 'ESTIMATED', '19L3357433', '8161.8', '125544', '5914587', '1', 'True'], ['1831', 'True', 'True', 'True', '115976', '2102', '2000007794886', 'E', '2020-10-12T09:41:13.223', '2020-10-12T00:00:00', '4595067', 'SMART', 'VALID', 'ACTUAL', '19L3357433', '8044.0', '125544', '5199916', '1', 'False'], ['1831', 'False', 'False', 'True', '115976', '2102', '2000007794886', 'E', '2020-09-14T08:38:06.823', '2020-09-14T00:00:00', '4308123', 'SMART', 'INVALID', 'ACTUAL', '19L3357433', '7281.0', '125544', '4891250', '1', 'False'], ['1831', 'False', 'False', 'True', '115976', '2102', '2000007794886', 'E', '2020-09-14T08:38:36.357', '2020-09-12T00:00:00', '4308127', 'CUSTOMER', 'INVALID', 'ACTUAL', '19L3357433', '7281.0', '125544', '4891254', '1', 'False'], ['1831', 'True', 'True', 'True', '115976', '2102', '2000007794886', 'E', '2020-09-14T08:38:47.747', '2020-09-12T00:00:00', '4308131', 'SMART', 'VALID', 'ACTUAL', '19L3357433', '7281.0', '125544', '4891258', '1', 'False'], ['1831', 'True', 'True', 'True', '115976', '2102', '2000007794886', 'E', '2020-08-12T10:28:54.837', '2020-08-12T00:00:00', '3997455', 'SMART', 'VALID', 'ACTUAL', '19L3357433', '6536.0', '125544', '4557403', '1', 'False'], ['1831', 'True', 'True', 'True', '115976', '2102', '2000007794886', 'E', '2020-07-13T08:23:12.55', '2020-07-13T00:00:00', '3704800', 'SMART', 'VALID', 'ACTUAL', '19L3357433', '5979.0', '125544', '4244164', '1', 'False'], ['1831', 'True', 'True', 'True', '115976', '2102', '2000007794886', 'E', '2020-06-12T08:24:47.097', '2020-06-12T00:00:00', '3438343', 'SMART', 'VALID', 'ACTUAL', '19L3357433', '5451.0', '125544', '3956768', '1', 'False'], ['1831', 'True', 'True', 'True', '115976', '2102', '2000007794886', 'E', '2020-05-12T09:31:40.627', '2020-05-12T00:00:00', '3175888', 'SMART', 'VALID', 'ACTUAL', '19L3357433', '4862.0', '125544', '3673532', '1', 'False'], ['1831', 'False', 'False', 'True', '115976', '2102', '2000007794886', 'E', '2020-04-15T10:06:54.07', '2020-04-15T00:00:00', '2923775', 'ESTIMATE', 'INVALID', 'ESTIMATED', '19L3357433', '4285.6', '125544', '3400537', '1', 'True'], ['1831', 'True', 'True', 'True', '115976', '2102', '2000007794886', 'E', '2020-05-12T09:31:48.743', '2020-04-15T00:00:00', '3175890', 'ESTIMATE', 'VALID', 'ESTIMATED', '19L3357433', '4291.1', '125544', '3673534', '1', 'True'], ['1831', 'True', 'True', 'True', '115976', '2102', '2000007794886', 'E', '2020-04-14T07:58:12.617', '2020-04-14T00:00:00', '2775715', 'CUSTOMER', 'VALID', 'ACTUAL', '19L3357433', '4270.0', '125544', '3240392', '1', 'False'], ['1831', 'False', 'False', 'False', '115976', '2102', '2000007794886', 'E', '2020-04-14T09:49:38.86', '2020-04-12T00:00:00', '2777262', 'SMART', 'INVALID', 'ACTUAL', '19L3357433', '4222.0', '125544', '3242050', '1', 'False'], []], "failed internal_readings data content"



# @patch("process_ensek_internal_readings.requests.Session.post")
def api_request(mock_post):
    print(mock_post)
    with open (os.path.join(os.path.dirname(__file__), "fixtures", "response-1831-page1.json")) as f_in:
        data_json = f_in.read().encode()


    print(type(data_json))
    mock_post.return_value.ok = True
    mock_post.return_value.status_code = 200
    #mock_post.return_value.content = data_json
    mock_post.side_effect = [{'content' : data_json}]


    metrics = [{
        "api_error_codes" : [],
        "api_method_time" : [],
        },]   
    # What is being tested
    internalreadings = InternalReadings()
    url = "https://test.com"
    headers = {
    "page": 1,
    "itemsPerPage": 25,
    "totalPages": 3,
    "items":{"test_head": "test_value"},
        }
    head = {'Content-Type': 'application/json',
            'Authorization': 'Bearer {0}'.format("1")}
    str(headers)
    account_id = "1831"
    response = internalreadings.get_api_response(url, head, account_id, metrics)

    (passed_url,), passed_headers = mock_post.call_args
    assert response == data_json, "Incorrect response returned"

    assert url == passed_url, "Incorrect URL passed to request"
    assert {"headers": headers} == passed_headers, "Incorrect Headers passed to request"


@responses.activate
def test_api_response_one_page():
    with open (os.path.join(os.path.dirname(__file__), "fixtures", "response-1831-one-page.json")) as f_in:
        data_json_1 = f_in.read()

    metrics = [{
        "api_error_codes" : [],
        "api_method_time" : [],
        },]   
    internalreadings = InternalReadings()
    url = "https://test.com"
    head = {'Content-Type': 'application/json',
            'Authorization': 'Bearer {0}'.format("1")}
    account_id = "1831"

    responses.add(responses.POST, '{}'.format(url),
                  body=data_json_1, status=200)
    # responses.add(responses.POST, 'http://test.com/1831/2',
    #               body=data_json_1, status=200)
    # responses.add(responses.POST, 'http://test.com/1831/3',
    #               body=data_json_1, status=200)
    # responses.add(responses.POST, 'http://test.com/1831/4',
    #               body=data_json_1, status=200)

    response = internalreadings.get_api_response(url, head, account_id, metrics)
    # resp = requests.post('https://test.com/1831/?page=1')
    # assert response.content == data_json_1

    assert response == json.loads(data_json_1)["items"]

@responses.activate
def test_api_response_multiple_pages():
    with open (os.path.join(os.path.dirname(__file__), "fixtures", "response-1831-page1.json")) as f_in:
        data_json_0 = f_in.read()

    with open (os.path.join(os.path.dirname(__file__), "fixtures", "response-1831-page2.json")) as f_in:
        data_json_1 = f_in.read()

    with open (os.path.join(os.path.dirname(__file__), "fixtures", "response-1831-page3.json")) as f_in:
        data_json_2 = f_in.read()

    with open (os.path.join(os.path.dirname(__file__), "fixtures", "response-1831-page4.json")) as f_in:
        data_json_3 = f_in.read()

    metrics = [{
        "api_error_codes" : [],
        "api_method_time" : [],
        },]

    internalreadings = InternalReadings()
    url = "https://test.com"
    head = {'Content-Type': 'application/json',
            'Authorization': 'Bearer {0}'.format("1")}
    account_id = "1831"

    responses.add(responses.POST, '{}/{}?page=2'.format(url, account_id),
                  body=data_json_1, status=200)
    responses.add(responses.POST, '{}/{}?page=3'.format(url, account_id),
                  body=data_json_2, status=200)
    responses.add(responses.POST, '{}/{}?page=4'.format(url, account_id),
                  body=data_json_3, status=200)
    responses.add(responses.POST, '{}/{}'.format(url, account_id),
                  body=data_json_0, status=200)

    response = internalreadings.get_api_response('{}/{}'.format(url, account_id), head, account_id, metrics)

    complete_items = json.loads(data_json_0)["items"] + \
                       json.loads(data_json_1)["items"] + \
                       json.loads(data_json_2)["items"] + \
                       json.loads(data_json_3)["items"]

    print('{} {}'.format(len(response), len(complete_items)))
    assert response == complete_items
