from datetime import datetime, timedelta
from lxml import etree
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from moto import mock_s3_deprecated
import csv

from processALP_CV import ALPHistoricalCV

from connections.connect_db import get_boto_S3_Connections as s3_con


xml_response_string = b'<?xml version="1.0" encoding="utf-8"?><soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema"><soap:Body><GetPublicationDataWMResponse xmlns="http://www.NationalGrid.com/MIPI/"><GetPublicationDataWMResult><CLSMIPIPublicationObjectBE><PublicationObjectName>Calorific Value, Campbeltown</PublicationObjectName><PublicationObjectData><CLSPublicationObjectDataBE><ApplicableAt>2019-01-02T10:30:19Z</ApplicableAt><ApplicableFor>2019-01-01T00:00:00Z</ApplicableFor><Value>39.5</Value><GeneratedTimeStamp>2019-01-02T10:32:01Z</GeneratedTimeStamp><QualityIndicator> </QualityIndicator><Substituted>N</Substituted><CreatedDate>2019-01-02T10:32:01Z</CreatedDate></CLSPublicationObjectDataBE><CLSPublicationObjectDataBE><ApplicableAt>2019-01-03T10:30:15Z</ApplicableAt><ApplicableFor>2019-01-02T00:00:00Z</ApplicableFor><Value>39.5</Value><GeneratedTimeStamp>2019-01-03T10:32:02Z</GeneratedTimeStamp><QualityIndicator> </QualityIndicator><Substituted>N</Substituted><CreatedDate>2019-01-03T10:32:02Z</CreatedDate></CLSPublicationObjectDataBE></PublicationObjectData></CLSMIPIPublicationObjectBE></GetPublicationDataWMResult></GetPublicationDataWMResponse></soap:Body></soap:Envelope>'


@mock_s3_deprecated
def test_extract_cv_data():
    s3_bucket_name = 'simulated-bucket'

    s3_connection = S3Connection()
    s3_bucket = s3_connection.create_bucket(s3_bucket_name)

    alp_historical_cv = ALPHistoricalCV()

    data_cv = alp_historical_cv.format_xml_response(xml_response_string)
    cv_folder = 'Calorific Value, Campbeltown'

    k = s3_con(s3_bucket_name)
    dir_s3 = {
        's3_alp_cv': {
            'AlpCV': 'stage1/ALP/AlpCV/'
        }
    }

    start_date = datetime.today().date() - timedelta(days=1)
    end_date = datetime.today().date()

    alp_historical_cv.extract_cv_data(
        data_cv, cv_folder, k, dir_s3, start_date, end_date)

    expected_s3_key = 'stage1/ALP/AlpCV/alp_cv_historical_{}_{}.csv'.format(
        cv_folder, str(start_date))

    # Verify that the file created in S3 has the correct contents
    k = Key(s3_bucket)
    k.key = expected_s3_key
    csv_lines = k.get_contents_as_string(encoding='utf-8')

    # Construct a CSV reader to parse the file data for us, and verify that the
    # column headers are correct.
    reader = csv.reader(csv_lines.split('\n'), delimiter=',')

    column_headers = next(reader)
    assert(column_headers == ['name', 'applicable_at', 'applicable_for', 'value'])
