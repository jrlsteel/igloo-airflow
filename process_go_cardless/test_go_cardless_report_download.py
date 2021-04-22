import unittest
import io
from unittest.mock import patch

import boto3
from moto import mock_s3
from freezegun import freeze_time

from go_cardless_report_download import GoCardlessReport
import config_test

bucket_name = "test-bucket"


def mock_get_sftp_connection(*args, **kwargs):
    return "connected"


def mock_list_files_on_sftp_in_date(*args, **kwargs):
    return [
        "Go_Cardless_Report_210302.csv",
        "Go_Cardless_Report_210303.csv",
        "Go_Cardless_Report_210304.csv",
    ]


def mock_list_files_on_sftp_out_of_date(*args, **kwargs):
    return [
        "Go_Cardless_Report_210302.csv",
        "Go_Cardless_Report_210303.csv",
    ]


def mock_list_files_on_sftp_no_files(*args, **kwargs):
    return []


def mock_download_file(*args, **kwargs):

    f = io.BytesIO()
    f.write(b"Test report contents")
    downloaded_file = f

    return (downloaded_file, "Go_Cardless_Report_210304.csv")


@mock_s3
@freeze_time("2021-03-04T12:18:00.123456+01:00")
@patch.object(GoCardlessReport, "get_sftp_connection", mock_get_sftp_connection)
class TestGoCardlessCustomers(unittest.TestCase):
    def setUp(self):

        self.s3 = boto3.resource(
            "s3",
            aws_access_key_id="xxxx",
            aws_secret_access_key="xxxx",
        )

        self.instance = GoCardlessReport(config_test, bucket_name)

        self.bucket = self.s3.Bucket(bucket_name)
        self.bucket.create(
            CreateBucketConfiguration={
                "LocationConstraint": "eu-west-1",
            }
        )

    def tearDown(self):

        self.bucket.objects.all().delete()
        self.bucket.delete()

    @patch.object(GoCardlessReport, "download_file", mock_download_file)
    @patch.object(
        GoCardlessReport, "list_files_on_sftp", mock_list_files_on_sftp_in_date
    )
    def test_normal_run(self):

        # create an existing file in the s3 bucket
        file_contents = "test file"
        file_bytes = file_contents.encode()

        self.bucket.put_object(
            Key="{}/Go_Cardless_Report_210101".format(self.instance.s3_key_prefix),
            Body=file_bytes,
        )

        self.instance.process()

        objects = self.bucket.objects.filter(Prefix=self.instance.s3_key_prefix)
        keys = [object.key for object in objects]

        self.assertEqual(
            keys, ["go-cardless-id-mandate-lookup/Go_Cardless_Report_210304.csv"]
        )

    @patch.object(
        GoCardlessReport, "list_files_on_sftp", mock_list_files_on_sftp_out_of_date
    )
    def test_file_out_of_date(self):
        with self.assertRaises(Exception) as context:
            self.instance.process()
        self.assertTrue("File is out-of-date" in str(context.exception))

    @patch.object(
        GoCardlessReport, "list_files_on_sftp", mock_list_files_on_sftp_no_files
    )
    def test_no_files(self):

        with self.assertRaises(Exception) as context:
            self.instance.process()
        self.assertTrue("No go-cardless reports found" in str(context.exception))


if __name__ == "__main__":
    unittest.main()
