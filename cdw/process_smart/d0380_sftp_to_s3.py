import sys


import io
import datetime
import cdw.common.directories
import time
import traceback
import cdw.common.utils
import boto3
import os
import sentry_sdk
from cdw.conf import config
import smart_open
import os
import logging
from cdw.common import utils as util
import pysftp


logger = logging.getLogger("igloo.etl.d0380")
logger.setLevel("DEBUG")


"""
This script is part of the D0380 flow.

https://dtc.mrasco.com/DataFlow.aspx?FlowCounter=0380&FlowVers=1&searchMockFlows=False

As part of the D0379 flow where a file is sent to a sftp location, we are sent back a a file, D0380.
We have chosen to store the D0380 files in an s3 bucket.
This script is responsible for copying D0380 files to S3.

The process will be the same in each environment, except that the D0380 is only collected from sftp in Production.
"""


def copy_all_from_d0380():
    """
    Copies all files in D0380 directory to an S3 bucket
    """

    logger.info("Running D0380 script")

    try:
        directory = cdw.common.utils.get_dir()
        s3_destination_bucket = directory["s3_bucket"]
        s3_key_prefix = "{}/D0380/".format(common.directories.common["elective_hh"]["d0380_s3_key_prefix"])

        aws_access_key_id = config.s3_config["access_key"]
        aws_secret_access_key = config.s3_config["secret_key"]

        s3 = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )
        logger.info("Established S3 connection")

        sftp_host = config.elective_hh_sftp_server["sftp_host"]
        sftp_username = config.elective_hh_sftp_server["sftp_username"]
        sftp_password = config.elective_hh_sftp_server["sftp_password"]
        sftp_prefix = config.elective_hh_sftp_server["sftp_d0380_prefix"]

        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None

        sftp_connection_string = "sftp://{}:{}@{}".format(
            sftp_username,
            sftp_password,
            sftp_host,
        )

        logger.debug("SFTP Connection info: {}".format(sftp_connection_string.replace(sftp_password, "<redacted>")))

        sftp_object = pysftp.Connection(
            host=sftp_host,
            username=sftp_username,
            password=sftp_password,
            cnopts=cnopts,
        )
        logger.info("Established SFTP connection")

        file_list = sftp_object.listdir(sftp_prefix)
        file_list_string = str(file_list)
        logger.info("Writing files: {} to D0380 S3 bucket.".format(file_list_string))

        for filename in file_list:
            filepath = "{}/{}".format(sftp_prefix, filename)
            print("File Path: ", filepath)
            with io.BytesIO() as file_data:  # read files in memory and copy to s3
                print("file data ", file_data)
                sftp_object.getfo(filepath, file_data)
                file_data.seek(0)
                print(file_data)

                s3.put_object(
                    Bucket=s3_destination_bucket,
                    Key=s3_key_prefix + filename,
                    Body=file_data,
                )

        logger.info("All files copied")

    except Exception as e:
        logger.error(traceback.format_exc())
        raise e
