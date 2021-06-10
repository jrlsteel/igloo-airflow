"""
This script downloads a daily file in the format Go_Cardless_Report_yymmdd.csv
from the folder fileExports on the Ensek SFTP Server.

The file is placed in the S3 location:
s3://igloo-data-warehouse-ENV-finance/go-cardless-id-mandate-lookup/
and is presented in an external schema in Redshift in the table
aws_fin_stage1_extracts.fin_go_cardless_id_mandate_lookup
"""

import sys
import io
import traceback
from datetime import datetime
from base64 import decodebytes

# import sentry_sdk
import logging
import pysftp
import boto3
import paramiko

sys.path.append("..")

logger = logging.getLogger(__name__)
logger.setLevel("DEBUG")


class GoCardlessReport:
    def __init__(self, config, bucket_name):

        self.s3_destination_bucket = bucket_name
        self.s3_key_prefix = "go-cardless-id-mandate-lookup"

        self.aws_access_key_id = config.s3_config["access_key"]
        self.aws_secret_access_key = config.s3_config["secret_key"]

        self.sftp_host = config.ensek_sftp_config["host"]
        self.sftp_username = config.ensek_sftp_config["username"]
        self.sftp_password = config.ensek_sftp_config["password"]
        self.sftp_prefix = "fileExports"
        self.go_cardless_filename_prefix = "Go_Cardless_Report_"

        key_data = b"""AAAAB3NzaC1yc2EAAAABIwAAAIEAy/xRmuLb93TkDauehP1S1fSwcUHJ5FVErE8hmJ/g/rHHPb2pwMK6ctdy4CiJck9w0iSUtXHtUWov6UbES+ZQMywLd11aN//Aq9z72xIUq2VjjnDygK1Sr7e0FQ8VuZtb4PiP5qLGbmTzystiEqL+9JPBD7wJMSDQvKrlqo27Kn0="""
        key = paramiko.RSAKey(data=decodebytes(key_data))
        self.cnopts = pysftp.CnOpts()
        self.cnopts.hostkeys.add("52.214.39.234", "ssh-rsa", key)

        self.s3 = None

        datepart = datetime.today().strftime("%y%m%d")
        self.todays_filename = "{}{}.csv".format(self.go_cardless_filename_prefix, datepart)

    def connect_s3(self):

        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
        )

        logger.info("Established S3 connection")

    def get_sftp_connection(self):

        sftp_connection = pysftp.Connection(
            self.sftp_host,
            username=self.sftp_username,
            password=self.sftp_password,
            cnopts=self.cnopts,
        )

        logger.info("Established SFTP connection")

        return sftp_connection

    def list_files_on_sftp(self):

        sftp_connection = self.get_sftp_connection()

        with sftp_connection as sftp:

            with sftp.cd(self.sftp_prefix):

                file_list = sftp.listdir()

                return file_list

    def filter_latest_gocardless_report(self, file_list):

        go_cardless_reports = [file for file in file_list if file.startswith(self.go_cardless_filename_prefix)]

        if go_cardless_reports == []:
            raise Exception("No go-cardless reports found")

        for file in go_cardless_reports:
            latest_go_cardless_report = file

        logger.info("Latest go-cardless report is %s", latest_go_cardless_report)

        if latest_go_cardless_report != self.todays_filename:
            raise Exception("File is out-of-date")

        return latest_go_cardless_report

    def list_existing_s3_files(self):

        if not self.s3:
            self.connect_s3()

        list_objects_response = self.s3.list_objects_v2(Bucket=self.s3_destination_bucket, Prefix=self.s3_key_prefix)

        existing_objects = list_objects_response.get("Contents", [])
        existing_keys = [existing_object["Key"] for existing_object in existing_objects]

        return existing_keys

    def download_file(self, filename):

        sftp_connection = self.get_sftp_connection()

        with sftp_connection as sftp:
            with sftp.cd(self.sftp_prefix):
                file_object = io.BytesIO()
                sftp.getfo(filename, file_object)
                file_object.seek(0)
                logger.info("File downloaded")
                return (file_object, filename)

    def store_in_s3(self, file_object, filename):

        if not self.s3:
            self.connect_s3()

        self.s3.put_object(
            Bucket=self.s3_destination_bucket,
            Key="{}/{}".format(self.s3_key_prefix, filename),
            Body=file_object,
        )

        logger.info("File copied to S3")

    def delete_existing_s3_files(self, existing_keys):

        for key in existing_keys:
            if key == "{}/{}".format(self.s3_key_prefix, self.todays_filename):
                continue
            self.s3.delete_object(
                Bucket=self.s3_destination_bucket,
                Key=key,
            )

            logger.info("Deleted existing object %s", key)

    def process(self):

        try:
            self.connect_s3()
            file_list = self.list_files_on_sftp()
            latest_go_cardless_report = self.filter_latest_gocardless_report(file_list)
            existing_keys = self.list_existing_s3_files()
            file_object, filename = self.download_file(latest_go_cardless_report)
            self.store_in_s3(file_object, filename)
            self.delete_existing_s3_files(existing_keys)
        except Exception as e:
            logger.error(traceback.format_exc())
            raise e


if __name__ == "__main__":

    from conf import config
    from common import utils as util

    directory = util.get_dir()
    bucket_name = directory["s3_finance_bucket"]

    instance = GoCardlessReport(config, bucket_name)
    instance.process()
