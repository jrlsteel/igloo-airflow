from moto import mock_s3_deprecated
from boto.s3.connection import S3Connection
from boto.s3.key import Key

import csv

from cdw.process_postcodes.postcodes_etl import PostcodesETLPipeline
import requests_mock
import pandas
import pytest
from freezegun import freeze_time

import os
import sys


from cdw.common import utils

valid_csv = {
    "response_text": open(os.path.join(os.path.dirname(__file__), "fixtures", "test-short.csv"), "r").read(),
    "expected_load_df": pandas.DataFrame(
        [
            [
                "SW8 1DA",
                "SW8  1DA",
                "SW8 1DA",
                "01-1980",
                0,
                530708,
                177089,
                1,
                "E13000001",
                "Inner London",
                "E09000022",
                "Lambeth",
                "E05000429",
                "Stockwell",
                "E92000001",
                "England",
                "E12000007",
                "London",
                "E14001008",
                "Vauxhall",
                "E15000007",
                "London",
                "E16000058",
                "Lambeth",
                "E01003129",
                "Lambeth 004E",
                "E02000621",
                "Lambeth 004",
                "3D2",
                "Ethnicity central;Aspirational techies;Established tech workers",
                -0.119236,
                51.477668,
                "Postcode Level",
                "08/10/2020",
                "(51.477668, -0.119236)",
                1528193,
            ],
            [
                "L23 1AD",
                "L23  1AD",
                "L23 1AD",
                "12-2019",
                0,
                333815,
                401077,
                5,
                "E11000002",
                "Merseyside",
                "E08000014",
                "Sefton",
                "E05000944",
                "Manor",
                "E92000001",
                "England",
                "E12000002",
                "North West",
                "E14000916",
                "Sefton Central",
                "E15000002",
                "North West",
                "E16000090",
                "Sefton",
                "E01007037",
                "Sefton 021E",
                "E02001449",
                "Sefton 021",
                "6B4",
                "Suburbanites;Semi-detached suburbia;Older workers and retirement",
                -2.999281,
                53.50222,
                "Postcode Level",
                "08/10/2020",
                "(53.50222, -2.999281)",
                807099,
            ],
        ],
        columns=utils.get_common_info("postcodes", "extract_dtypes").keys(),
    ).astype(utils.get_common_info("postcodes", "extract_dtypes")),
    # Note that the DataFrame that we expect to receive from the transform() method includes
    # the etl_change field.
    "expected_transform_df": pandas.DataFrame(
        [
            [
                "SW8 1DA",
                "SW8  1DA",
                "SW8 1DA",
                "01-1980",
                0,
                530708,
                177089,
                1,
                "E13000001",
                "Inner London",
                "E09000022",
                "Lambeth",
                "E05000429",
                "Stockwell",
                "E92000001",
                "England",
                "E12000007",
                "London",
                "E14001008",
                "Vauxhall",
                "E15000007",
                "London",
                "E16000058",
                "Lambeth",
                "E01003129",
                "Lambeth 004E",
                "E02000621",
                "Lambeth 004",
                "3D2",
                "Ethnicity central;Aspirational techies;Established tech workers",
                -0.119236,
                51.477668,
                "Postcode Level",
                "08/10/2020",
                "(51.477668, -0.119236)",
                1528193,
                1522754340123456000,
            ],
            [
                "L23 1AD",
                "L23  1AD",
                "L23 1AD",
                "12-2019",
                0,
                333815,
                401077,
                5,
                "E11000002",
                "Merseyside",
                "E08000014",
                "Sefton",
                "E05000944",
                "Manor",
                "E92000001",
                "England",
                "E12000002",
                "North West",
                "E14000916",
                "Sefton Central",
                "E15000002",
                "North West",
                "E16000090",
                "Sefton",
                "E01007037",
                "Sefton 021E",
                "E02001449",
                "Sefton 021",
                "6B4",
                "Suburbanites;Semi-detached suburbia;Older workers and retirement",
                -2.999281,
                53.50222,
                "Postcode Level",
                "08/10/2020",
                "(53.50222, -2.999281)",
                807099,
                1522754340123456000,
            ],
        ],
        columns=utils.get_common_info("postcodes", "transform_dtypes").keys(),
    ).astype(utils.get_common_info("postcodes", "transform_dtypes")),
}

valid_csv_columns_reordered = {
    "response_text": open(
        os.path.join(os.path.dirname(__file__), "fixtures", "test-short-columns-reordered.csv"), "r"
    ).read(),
    "expected_load_df": pandas.DataFrame(
        [
            [
                "SW8 1DA",
                "SW8  1DA",
                "SW8 1DA",
                "01-1980",
                0,
                530708,
                177089,
                1,
                "E13000001",
                "Inner London",
                "E09000022",
                "Lambeth",
                "E05000429",
                "Stockwell",
                "E92000001",
                "England",
                "E12000007",
                "London",
                "E14001008",
                "Vauxhall",
                "E15000007",
                "London",
                "E16000058",
                "Lambeth",
                "E01003129",
                "Lambeth 004E",
                "E02000621",
                "Lambeth 004",
                "3D2",
                "Ethnicity central;Aspirational techies;Established tech workers",
                -0.119236,
                51.477668,
                "Postcode Level",
                "08/10/2020",
                "(51.477668, -0.119236)",
                1528193,
            ],
            [
                "L23 1AD",
                "L23  1AD",
                "L23 1AD",
                "12-2019",
                0,
                333815,
                401077,
                5,
                "E11000002",
                "Merseyside",
                "E08000014",
                "Sefton",
                "E05000944",
                "Manor",
                "E92000001",
                "England",
                "E12000002",
                "North West",
                "E14000916",
                "Sefton Central",
                "E15000002",
                "North West",
                "E16000090",
                "Sefton",
                "E01007037",
                "Sefton 021E",
                "E02001449",
                "Sefton 021",
                "6B4",
                "Suburbanites;Semi-detached suburbia;Older workers and retirement",
                -2.999281,
                53.50222,
                "Postcode Level",
                "08/10/2020",
                "(53.50222, -2.999281)",
                807099,
            ],
        ],
        columns=utils.get_common_info("postcodes", "extract_dtypes").keys(),
    ).astype(utils.get_common_info("postcodes", "extract_dtypes")),
    # Note that the DataFrame that we expect to receive from the transform() method includes
    # the etl_change field.
    "expected_transform_df": pandas.DataFrame(
        [
            [
                "SW8 1DA",
                "SW8  1DA",
                "SW8 1DA",
                "01-1980",
                0,
                530708,
                177089,
                1,
                "E13000001",
                "Inner London",
                "E09000022",
                "Lambeth",
                "E05000429",
                "Stockwell",
                "E92000001",
                "England",
                "E12000007",
                "London",
                "E14001008",
                "Vauxhall",
                "E15000007",
                "London",
                "E16000058",
                "Lambeth",
                "E01003129",
                "Lambeth 004E",
                "E02000621",
                "Lambeth 004",
                "3D2",
                "Ethnicity central;Aspirational techies;Established tech workers",
                -0.119236,
                51.477668,
                "Postcode Level",
                "08/10/2020",
                "(51.477668, -0.119236)",
                1528193,
                1522754340123456000,
            ],
            [
                "L23 1AD",
                "L23  1AD",
                "L23 1AD",
                "12-2019",
                0,
                333815,
                401077,
                5,
                "E11000002",
                "Merseyside",
                "E08000014",
                "Sefton",
                "E05000944",
                "Manor",
                "E92000001",
                "England",
                "E12000002",
                "North West",
                "E14000916",
                "Sefton Central",
                "E15000002",
                "North West",
                "E16000090",
                "Sefton",
                "E01007037",
                "Sefton 021E",
                "E02001449",
                "Sefton 021",
                "6B4",
                "Suburbanites;Semi-detached suburbia;Older workers and retirement",
                -2.999281,
                53.50222,
                "Postcode Level",
                "08/10/2020",
                "(53.50222, -2.999281)",
                807099,
                1522754340123456000,
            ],
        ],
        columns=utils.get_common_info("postcodes", "transform_dtypes").keys(),
    ).astype(utils.get_common_info("postcodes", "transform_dtypes")),
}

missing_column_csv = {
    "response_text": open(os.path.join(os.path.dirname(__file__), "fixtures", "missing-column.csv"), "r").read(),
    "expected_load_df": pandas.DataFrame(
        [
            [
                "SW8 1DA",
                "SW8  1DA",
                "SW8 1DA",
                "01-1980",
                0,
                530708,
                177089,
                1,
                "E13000001",
                "Inner London",
                "E09000022",
                "Lambeth",
                "E05000429",
                "Stockwell",
                "E92000001",
                "England",
                "E12000007",
                "London",
                "E14001008",
                "Vauxhall",
                "E15000007",
                "London",
                "E16000058",
                "Lambeth",
                "E01003129",
                "Lambeth 004E",
                "E02000621",
                "Lambeth 004",
                "3D2",
                "Ethnicity central;Aspirational techies;Established tech workers",
                -0.119236,
                51.477668,
                "Postcode Level",
                "08/10/2020",
                "(51.477668, -0.119236)",
                1528193,
            ],
            [
                "L23 1AD",
                "L23  1AD",
                "L23 1AD",
                "12-2019",
                0,
                333815,
                401077,
                5,
                "E11000002",
                "Merseyside",
                "E08000014",
                "Sefton",
                "E05000944",
                "Manor",
                "E92000001",
                "England",
                "E12000002",
                "North West",
                "E14000916",
                "Sefton Central",
                "E15000002",
                "North West",
                "E16000090",
                "Sefton",
                "E01007037",
                "Sefton 021E",
                "E02001449",
                "Sefton 021",
                "6B4",
                "Suburbanites;Semi-detached suburbia;Older workers and retirement",
                -2.999281,
                53.50222,
                "Postcode Level",
                "08/10/2020",
                "(53.50222, -2.999281)",
                807099,
            ],
        ],
        columns=utils.get_common_info("postcodes", "extract_dtypes").keys(),
    ).astype(utils.get_common_info("postcodes", "extract_dtypes")),
    # Note that the DataFrame that we expect to receive from the transform() method includes
    # the etl_change field.
    "expected_transform_df": pandas.DataFrame(
        [
            [
                "SW8 1DA",
                "SW8  1DA",
                "SW8 1DA",
                "01-1980",
                0,
                530708,
                177089,
                1,
                "E13000001",
                "Inner London",
                "E09000022",
                "Lambeth",
                "E05000429",
                "Stockwell",
                "E92000001",
                "England",
                "E12000007",
                "London",
                "E14001008",
                "Vauxhall",
                "E15000007",
                "London",
                "E16000058",
                "Lambeth",
                "E01003129",
                "Lambeth 004E",
                "E02000621",
                "Lambeth 004",
                "3D2",
                "Ethnicity central;Aspirational techies;Established tech workers",
                -0.119236,
                51.477668,
                "Postcode Level",
                "08/10/2020",
                "(51.477668, -0.119236)",
                1528193,
                1522754340123456000,
            ],
            [
                "L23 1AD",
                "L23  1AD",
                "L23 1AD",
                "12-2019",
                0,
                333815,
                401077,
                5,
                "E11000002",
                "Merseyside",
                "E08000014",
                "Sefton",
                "E05000944",
                "Manor",
                "E92000001",
                "England",
                "E12000002",
                "North West",
                "E14000916",
                "Sefton Central",
                "E15000002",
                "North West",
                "E16000090",
                "Sefton",
                "E01007037",
                "Sefton 021E",
                "E02001449",
                "Sefton 021",
                "6B4",
                "Suburbanites;Semi-detached suburbia;Older workers and retirement",
                -2.999281,
                53.50222,
                "Postcode Level",
                "08/10/2020",
                "(53.50222, -2.999281)",
                807099,
                1522754340123456000,
            ],
        ],
        columns=utils.get_common_info("postcodes", "transform_dtypes").keys(),
    ).astype(utils.get_common_info("postcodes", "transform_dtypes")),
}

# This is used by all tests as it's where our config parameters are stored.
# Note that it is env-specific, so the config file (conf/config.py) must be
# set up reasonably sanely.
directories = utils.get_dir()


@pytest.mark.parametrize("data", [valid_csv, valid_csv_columns_reordered])
def test_postcodes_etl_extract_ok(requests_mock, data):
    """
    Verifies that the extract() method returns the expected dataframe when
    the download succeeds.
    """

    # Mock out the GET request with a canned response
    requests_mock.get(
        "https://opendata.camden.gov.uk/api/views/tr8t-gqz7/rows.csv?accessType=DOWNLOAD",
        text=data["response_text"],
        status_code=200,
    )

    postcodes_etl = PostcodesETLPipeline(
        source={"url": utils.get_common_info("postcodes", "api_url")},
        destination={
            "s3_bucket": utils.get_dir()["s3_bucket"],
            "s3_key": utils.get_common_info("postcodes", "s3_key"),
            "extract_dtypes": utils.get_common_info("postcodes", "extract_dtypes"),
            "transform_dtypes": utils.get_common_info("postcodes", "transform_dtypes"),
        },
    )

    df = postcodes_etl.extract()

    pandas.testing.assert_frame_equal(df, data["expected_load_df"])


def test_postcodes_etl_extract_not_found(requests_mock):
    """
    Verifies that the extract() method raises the expected exception when
    the download fails due to a 404 Not Found error.
    """

    # Mock out the GET request with a canned response
    requests_mock.get(
        "https://opendata.camden.gov.uk/api/views/tr8t-gqz7/rows.csv?accessType=DOWNLOAD",
        text=valid_csv["response_text"],
        status_code=404,
    )

    postcodes_etl = PostcodesETLPipeline(
        source={"url": utils.get_common_info("postcodes", "api_url")},
        destination={
            "s3_bucket": utils.get_dir()["s3_bucket"],
            "s3_key": utils.get_common_info("postcodes", "s3_key"),
            "extract_dtypes": utils.get_common_info("postcodes", "extract_dtypes"),
            "transform_dtypes": utils.get_common_info("postcodes", "transform_dtypes"),
        },
    )

    with pytest.raises(Exception):
        postcodes_etl.extract()


def test_postcodes_etl_extract_missing_column(requests_mock):
    """
    Verifies that the extract() method raises the expected exception when
    the download is successful but the data is missing a column.
    """

    # Mock out the GET request with a canned response
    requests_mock.get(
        "https://opendata.camden.gov.uk/api/views/tr8t-gqz7/rows.csv?accessType=DOWNLOAD",
        text=missing_column_csv["response_text"],
        status_code=200,
    )

    postcodes_etl = PostcodesETLPipeline(
        source={"url": utils.get_common_info("postcodes", "api_url")},
        destination={
            "s3_bucket": utils.get_dir()["s3_bucket"],
            "s3_key": utils.get_common_info("postcodes", "s3_key"),
            "extract_dtypes": utils.get_common_info("postcodes", "extract_dtypes"),
            "transform_dtypes": utils.get_common_info("postcodes", "transform_dtypes"),
        },
    )

    with pytest.raises(KeyError):
        postcodes_etl.extract()


@freeze_time("2018-04-03T12:19:00.123456+01:00")
def test_postcodes_etl_transform():
    """
    Verifies that the transform() method performs the expected transformation.
    """

    postcodes_etl = PostcodesETLPipeline(
        source={"url": utils.get_common_info("postcodes", "api_url")},
        destination={
            "s3_bucket": utils.get_dir()["s3_bucket"],
            "s3_key": utils.get_common_info("postcodes", "s3_key"),
            "extract_dtypes": utils.get_common_info("postcodes", "extract_dtypes"),
            "transform_dtypes": utils.get_common_info("postcodes", "transform_dtypes"),
        },
    )

    transformed_df = postcodes_etl.transform(valid_csv["expected_load_df"])

    pandas.testing.assert_frame_equal(transformed_df, valid_csv["expected_transform_df"])


@mock_s3_deprecated
def test_postcodes_etl_load():
    s3_connection = S3Connection()
    s3_bucket = s3_connection.create_bucket(
        utils.get_dir()["s3_bucket"],
    )

    postcodes_etl = PostcodesETLPipeline(
        source={"url": utils.get_common_info("postcodes", "api_url")},
        destination={
            "s3_bucket": utils.get_dir()["s3_bucket"],
            "s3_key": utils.get_common_info("postcodes", "s3_key"),
            "extract_dtypes": utils.get_common_info("postcodes", "extract_dtypes"),
            "transform_dtypes": utils.get_common_info("postcodes", "transform_dtypes"),
        },
    )

    postcodes_etl.load(valid_csv["expected_transform_df"])

    # Verify that the file created in S3 has the correct contents
    k = Key(s3_bucket)
    k.key = "stage2/stage2_Postcodes/postcodes.csv"
    csv_lines = k.get_contents_as_string(encoding="utf-8")

    # Construct a CSV reader to parse the file data for us, and verify that the
    # column headers are correct.
    reader = csv.reader(csv_lines.split("\n"), delimiter=",")

    column_headers = next(reader)

    assert column_headers == [
        "Postcode 1",
        "Postcode 2",
        "Postcode 3",
        "Date Introduced",
        "User Type",
        "Easting",
        "Northing",
        "Positional Quality",
        "County Code",
        "County Name",
        "Local Authority Code",
        "Local Authority Name",
        "Ward Code",
        "Ward Name",
        "Country Code",
        "Country Name",
        "Region Code",
        "Region Name",
        "Parliamentary Constituency Code",
        "Parliamentary Constituency Name",
        "European Electoral Region Code",
        "European Electoral Region Name",
        "Primary Care Trust Code",
        "Primary Care Trust Name",
        "Lower Super Output Area Code",
        "Lower Super Output Area Name",
        "Middle Super Output Area Code",
        "Middle Super Output Area Name",
        "Output Area Classification Code",
        "Output Area Classification Name",
        "Longitude",
        "Latitude",
        "Spatial Accuracy",
        "Last Uploaded",
        "Location",
        "Socrata ID",
        "etl_change",
    ]
    assert list(reader) == [
        [
            "SW8 1DA",
            "SW8  1DA",
            "SW8 1DA",
            "01-1980",
            "0",
            "530708",
            "177089",
            "1",
            "E13000001",
            "Inner London",
            "E09000022",
            "Lambeth",
            "E05000429",
            "Stockwell",
            "E92000001",
            "England",
            "E12000007",
            "London",
            "E14001008",
            "Vauxhall",
            "E15000007",
            "London",
            "E16000058",
            "Lambeth",
            "E01003129",
            "Lambeth 004E",
            "E02000621",
            "Lambeth 004",
            "3D2",
            "Ethnicity central;Aspirational techies;Established tech workers",
            "-0.119236",
            "51.477668",
            "Postcode Level",
            "08/10/2020",
            "(51.477668, -0.119236)",
            "1528193",
            "2018-04-03 11:19:00.123456",
        ],
        [
            "L23 1AD",
            "L23  1AD",
            "L23 1AD",
            "12-2019",
            "0",
            "333815",
            "401077",
            "5",
            "E11000002",
            "Merseyside",
            "E08000014",
            "Sefton",
            "E05000944",
            "Manor",
            "E92000001",
            "England",
            "E12000002",
            "North West",
            "E14000916",
            "Sefton Central",
            "E15000002",
            "North West",
            "E16000090",
            "Sefton",
            "E01007037",
            "Sefton 021E",
            "E02001449",
            "Sefton 021",
            "6B4",
            "Suburbanites;Semi-detached suburbia;Older workers and retirement",
            "-2.999281",
            "53.50222",
            "Postcode Level",
            "08/10/2020",
            "(53.50222, -2.999281)",
            "807099",
            "2018-04-03 11:19:00.123456",
        ],
        [],
    ]
