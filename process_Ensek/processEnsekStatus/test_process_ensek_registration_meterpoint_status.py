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

from process_ensek_registration_meterpoint_status import RegistrationsMeterpointsStatus

from connections.connect_db import get_boto_S3_Connections as s3_con

@mock_s3_deprecated
def test_extract_reg_elec_json():
    s3_bucket_name = 'simulated-bucket'

    s3_connection = S3Connection()
    s3_bucket = s3_connection.create_bucket(s3_bucket_name)

    registrations_meterpoints_status = RegistrationsMeterpointsStatus()

    data = 'Tracker.Registration.Live'
    series = pandas.Series(
        {
            'account_id': 1831,
            'meter_point_id': 2102,
            'meterpointnumber': 2000007794886,
            'meterpointtype': 'E'
        }
    )

    k = s3_con(s3_bucket_name)
    dir_s3 = {
        "s3_key": {
            "RegistrationsElecMeterpoint": "stage1/RegistrationsElecMeterpoint/",
            "RegistrationsGasMeterpoint": "stage1/RegistrationsGasMeterpoint/",
        },
    }

    registrations_meterpoints_status.extract_reg_elec_json(
        data, series, k, dir_s3)

    # Verify that the files created in S3 have the correct contents
    k = Key(s3_bucket)
    k.key = 'stage1/RegistrationsElecMeterpoint/reg_elec_mp_status_1831_2000007794886.csv'
    csv_lines = k.get_contents_as_string(encoding='utf-8')

    # Construct a CSV reader to parse the file data for us, and verify that the
    # column headers are correct.
    reader = csv.reader(csv_lines.split('\n'), delimiter=',')

    column_headers = next(reader)

    assert(column_headers == ['account_id', 'meter_point_id', 'meterpointnumber', 'status'])
    assert(list(reader) == [
        ['1831', '2102', '2000007794886', 'Tracker.Registration.Live'],
        []
    ])

@mock_s3_deprecated
def test_extract_reg_gas_json():
    s3_bucket_name = 'simulated-bucket'

    s3_connection = S3Connection()
    s3_bucket = s3_connection.create_bucket(s3_bucket_name)

    registrations_meterpoints_status = RegistrationsMeterpointsStatus()

    data = 'Tracker.Registration.Gas.Cancelled.in.Cooling.Off'

    series = pandas.Series(
        {
            'account_id': 100000,
            'meter_point_id': 159413,
            'meterpointnumber': 2459924700,
            'meterpointtype': 'G'
        }
    )

    k = s3_con(s3_bucket_name)
    dir_s3 = {
        "s3_key": {
            "RegistrationsElecMeterpoint": "stage1/RegistrationsElecMeterpoint/",
            "RegistrationsGasMeterpoint": "stage1/RegistrationsGasMeterpoint/",
        },
    }

    registrations_meterpoints_status.extract_reg_gas_json(
        data, series, k, dir_s3)

    # Verify that the files created in S3 have the correct contents
    k = Key(s3_bucket)
    k.key = 'stage1/RegistrationsGasMeterpoint/reg_gas_mp_status_100000_2459924700.csv'
    csv_lines = k.get_contents_as_string(encoding='utf-8')

    # Construct a CSV reader to parse the file data for us, and verify that the
    # column headers are correct.
    reader = csv.reader(csv_lines.split('\n'), delimiter=',')

    column_headers = next(reader)

    assert(column_headers == ['account_id', 'meter_point_id', 'meterpointnumber', 'status'])
    assert(list(reader) == [
        ['100000', '159413', '2459924700', 'Tracker.Registration.Gas.Cancelled.in.Cooling.Off'],
        []
    ])