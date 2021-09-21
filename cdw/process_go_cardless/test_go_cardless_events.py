from boto.s3.connection import S3Connection
from boto.s3.key import Key
from moto import mock_s3_deprecated
import csv
from freezegun import freeze_time
import pandas as pd

from cdw.process_go_cardless.go_cardless_events import GoCardlessEventProcessor

from cdw.common import utils as util
import gocardless_pro


@freeze_time("2018-04-03T12:18:00.123456+01:00")
@mock_s3_deprecated
def test_process_events_without_comma_in_description(mocker):
    s3_bucket_name = "igloo-data-warehouse-uat-finance"

    s3_connection = S3Connection()
    s3_bucket = s3_connection.create_bucket(s3_bucket_name)

    gc_event_processor = GoCardlessEventProcessor()

    start_date_df = pd.DataFrame([["2018-04-03T11:18:00.000Z"]])

    mocker.patch.object(util, "execute_query", return_value=start_date_df)

    mock_events = [
        gocardless_pro.resources.event.Event(
            {
                "id": "EV01EB6FS11A8P",
                "created_at": "2020-11-12T11:07:47.778Z",
                "resource_type": "payments",
                "action": "confirmed",
                "links": {"payment": "PM009X5MRFVPMT"},
                "details": {
                    "origin": "gocardless",
                    "cause": "payment_confirmed",
                    "bank_account_id": "BA000VMCB0FG08",
                    "description": "Enough time has passed since the payment was submitted for the banks to return an error so this payment is now confirmed.",
                },
                "metadata": {},
            },
            {},
        ),
    ]

    mocker.patch.object(gc_event_processor.events_api, "all", return_value=mock_events)
    gc_event_processor.process_events()

    expected_s3_key = "/go-cardless-api-events/timestamp=2018-Q2/go_cardless_events_2018-04-03T11:18:00.000Z_2018-04-03T11:18:00.000Z.csv"

    # Verify that the file created in S3 has the correct contents
    k = Key(s3_bucket)
    k.key = expected_s3_key
    csv_lines = k.get_contents_as_string(encoding="utf-8")

    # get the raw data row so we can check that all non-numeric values are quoted
    raw_data = csv_lines.split("\n")[1]

    # Construct a CSV reader to parse the file data for us, and verify that the
    # column headers are correct.
    reader = csv.reader(csv_lines.split("\n"), delimiter=",")

    column_headers = next(reader)

    # check all values are quoted
    assert (
        raw_data
        == '"EV01EB6FS11A8P","2020-11-12T11:07:47.778Z","payments","confirmed","","payment_confirmed","Enough time has passed since the payment was submitted for the banks to return an error so this payment is now confirmed.","gocardless","","","","","","","","","PM009X5MRFVPMT","","","",""'
    )
    assert column_headers == [
        "id",
        "created_at",
        "resource_type",
        "action",
        "customer_notifications",
        "cause",
        "description",
        "origin",
        "reason_code",
        "scheme",
        "will_attempt_retry",
        "mandate",
        "new_customer_bank_account",
        "new_mandate",
        "organisation",
        "parent_event",
        "payment",
        "payout",
        "previous_customer_bank_account",
        "refund",
        "subscription",
    ]
    assert list(reader) == [
        [
            "EV01EB6FS11A8P",
            "2020-11-12T11:07:47.778Z",
            "payments",
            "confirmed",
            "",
            "payment_confirmed",
            "Enough time has passed since the payment was submitted for the banks to return an error so this payment is now confirmed.",
            "gocardless",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "PM009X5MRFVPMT",
            "",
            "",
            "",
            "",
        ],
        [],
    ]


@freeze_time("2018-04-03T12:18:00.123456+01:00")
@mock_s3_deprecated
def test_process_events_with_comma_in_description(mocker):
    s3_bucket_name = "igloo-data-warehouse-uat-finance"

    s3_connection = S3Connection()
    s3_bucket = s3_connection.create_bucket(s3_bucket_name)

    gc_event_processor = GoCardlessEventProcessor()

    start_date_df = pd.DataFrame([["2018-04-03T11:18:00.000Z"]])

    mocker.patch.object(util, "execute_query", return_value=start_date_df)

    mock_events = [
        gocardless_pro.resources.event.Event(
            {
                "id": "EV01EB6FS11A8P",
                "created_at": "2020-11-12T11:07:47.778Z",
                "resource_type": "payments",
                "action": "confirmed",
                "links": {"payment": "PM009X5MRFVPMT"},
                "details": {
                    "origin": "gocardless",
                    "cause": "payment_confirmed",
                    "bank_account_id": "BA000VMCB0FG08",
                    "description": "Enough time has passed since the payment was submitted for the banks to return an error, so this payment is now confirmed.",
                },
                "metadata": {},
            },
            {},
        ),
    ]

    mocker.patch.object(gc_event_processor.events_api, "all", return_value=mock_events)
    gc_event_processor.process_events()

    expected_s3_key = "/go-cardless-api-events/timestamp=2018-Q2/go_cardless_events_2018-04-03T11:18:00.000Z_2018-04-03T11:18:00.000Z.csv"

    # Verify that the file created in S3 has the correct contents
    k = Key(s3_bucket)
    k.key = expected_s3_key
    csv_lines = k.get_contents_as_string(encoding="utf-8")

    # get the raw data row so we can check that all non-numeric values are quoted
    raw_data = csv_lines.split("\n")[1]

    # Construct a CSV reader to parse the file data for us, and verify that the
    # column headers are correct.
    reader = csv.reader(csv_lines.split("\n"), delimiter=",")

    column_headers = next(reader)

    assert (
        raw_data
        == '"EV01EB6FS11A8P","2020-11-12T11:07:47.778Z","payments","confirmed","","payment_confirmed","Enough time has passed since the payment was submitted for the banks to return an error, so this payment is now confirmed.","gocardless","","","","","","","","","PM009X5MRFVPMT","","","",""'
    )
    assert column_headers == [
        "id",
        "created_at",
        "resource_type",
        "action",
        "customer_notifications",
        "cause",
        "description",
        "origin",
        "reason_code",
        "scheme",
        "will_attempt_retry",
        "mandate",
        "new_customer_bank_account",
        "new_mandate",
        "organisation",
        "parent_event",
        "payment",
        "payout",
        "previous_customer_bank_account",
        "refund",
        "subscription",
    ]
    assert list(reader) == [
        [
            "EV01EB6FS11A8P",
            "2020-11-12T11:07:47.778Z",
            "payments",
            "confirmed",
            "",
            "payment_confirmed",
            "Enough time has passed since the payment was submitted for the banks to return an error, so this payment is now confirmed.",
            "gocardless",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "PM009X5MRFVPMT",
            "",
            "",
            "",
            "",
        ],
        [],
    ]
