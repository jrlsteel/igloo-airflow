import sys

sys.path.append("..")

import datetime
from connections.connect_db import get_boto_S3_Connections
import common.directories
import time
import traceback
import logging
import common.utils
import boto3
import os
import smart_open
from conf import config
import sentry_sdk


logger = logging.getLogger("igloo.etl.d0379")
logger.setLevel("DEBUG")


class InsufficientHHReadingsException(Exception):
    """
    Raised when there are fewer than 48 half-hourly readings for a given MPxN
    """

    def __init__(self, account_id, mpxn, num_readings):
        self.account_id = account_id
        self.mpxn = mpxn
        self.num_readings = num_readings
        super().__init__(
            "Account ID: {}, MPxN: {}, insufficient readings: {}".format(
                account_id, mpxn, num_readings
            )
        )


class TooManyHHReadingsException(Exception):
    """
    Raised when there are more than 48 half-hourly readings for a given MPxN
    """

    def __init__(self, account_id, mpxn, num_readings):
        self.account_id = account_id
        self.mpxn = mpxn
        self.num_readings = num_readings
        super().__init__(
            "Account ID: {}, MPxN: {}, too many readings: {}".format(
                account_id, mpxn, num_readings
            )
        )


class NoD0379FileFound(Exception):
    """
    Raised when no D0379 file matching prefix is found in bucket
    """

    def __init__(self, s3_bucket, s3_prefix):
        self.account_id = account_id
        self.mpxn = mpxn
        self.num_readings = num_readings
        super().__init__(
            "Account ID: {}, MPxN: {}, too many readings: {}".format(
                account_id, mpxn, num_readings
            )
        )


class NoD0379FileFound(Exception):
    """
    Raised when no files match a given prefix are found in S3
    """

    def __init__(self, s3_bucket, s3_prefix):
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        super().__init__(
            "No D0379 files matching prefix {} found in S3 bucket {}".format(
                s3_prefix, s3_bucket
            )
        )


"""
This script is essential as part of the d3079 data flow. 
It pulls a redshift view containing half-hourly energy consumption of customers with a smart meter.
It then generates a txt file using data from the view in a format specified by a 3rd party.
The file is then placed in an s3 bucket where it will be accessed using sftp.

The process will be the same in each environment, except that the d0379 file is only collected using sftp from the s3 bucket in production.
"""


def fetch_d0379_accounts():
    """
    Fetches the set of accounts and meters (account_id + mpxn) that should be included
    in the D0379 file.
    """

    d0379_accounts_sql_query = common.directories.common["elective_hh"][
        "elective_hh_trial_participants_sql"
    ]

    logger.info("Exeuting SQL: {}".format(d0379_accounts_sql_query))

    d0379_accounts_df = common.utils.execute_query_return_df(d0379_accounts_sql_query)

    logger.debug("D0379 account data dtypes:\n{}".format(d0379_accounts_df.dtypes))

    return d0379_accounts_df.to_dict("records")


def fetch_d0379_data(d0379_accounts, d0379_date):
    """
    Executes a query to retrieve D0379 data for a given date.

    :param d0379_accounts: list of objects with account id and mpxn
    :param d0379_date: The date for which D0379 data should be retrieved, as a datetime.date
    :returns: pandas.DataFrame containing the D0379 data
    """

    # When we receive half-hourly readings, the timestamp associated with each half-hour
    # period corresponds to the end of that period. As a result, to get the complete set
    #  of data for a single day, we need all readings with timestamps from 00:30:00 on day
    #  1 to 00:00:00 on day 2.
    from_datetime = datetime.datetime.combine(
        d0379_date,
        datetime.time.fromisoformat("00:30:00"),
        tzinfo=datetime.timezone.utc,
    )

    to_datetime = datetime.datetime.combine(
        d0379_date + datetime.timedelta(days=1),
        datetime.time.fromisoformat("00:00:00"),
        tzinfo=datetime.timezone.utc,
    )

    account_ids = [x["account_id"] for x in d0379_accounts]

    sql_query = common.directories.common["elective_hh"]["sql_query"].format(
        account_ids=",".join([str(x) for x in account_ids]),
        from_datetime=from_datetime.strftime("%Y-%m-%d %H:%M:%S"),
        to_datetime=to_datetime.strftime("%Y-%m-%d %H:%M:%S"),
    )

    logger.info("Executing SQL: {}".format(sql_query))

    df = common.utils.execute_query_return_df(sql_query)

    logger.debug("D0379 data dtypes:\n{}".format(df.dtypes))

    return df.astype({"account_id": "int64", "mpxn": "int64"})


def write_to_s3(d0379_text, s3_bucket, s3_key):
    logger.info("Writing D0379 file to s3://{}/{}".format(s3_bucket, s3_key))
    try:
        k = get_boto_S3_Connections(s3_bucket)
        k.key = s3_key
        k.set_contents_from_string(d0379_text)
    except Exception as e:
        logger.error(traceback.format_exc())
        raise e


def dataframe_to_d0379(d0379_accounts, d0379_date, df, fileid):
    """
    Format dataframe as a D0379 file.

    https://www.elexon.co.uk/wp-content/uploads/2012/02/p116_req_spec.pdf
    https://dtc.mrasco.com/DataFlow.aspx?FlowCounter=0379&FlowVers=1&searchMockFlows=False

    :param d0379_date: The date that the D0379 data relates to, as a datetime.date.
    :param df: The dataframe containing the data to be included in the D0379 file.
    :param fileid: The fileid to use in the file.
    """

    # https://dtc.mrasco.com/DataItem.aspx?ItemCounter=103
    measurement_quantity_id = "AI"

    # https://dtc.mrasco.com/DataItem.aspx?ItemCounter=84
    supplier_id = "PION"

    # Number of Accounts (distinct mpxns) included in the file (integer, max 10 characters)
    flow_count = 0

    # Number of lines in the files excluding the header and footer (integer, max 8 characters)
    total_group_count = 0

    now = datetime.datetime.now(datetime.timezone.utc)
    d0379_file_creation_timestamp = now.strftime("%Y%m%d%H%M%S")

    header = "ZHV|{0}|D0379001|X|{1}|C|UDMS|{2}||||OPER|".format(
        fileid,
        supplier_id,
        d0379_file_creation_timestamp,
    )

    lines = []
    lines.append(header)

    for d0379_account in d0379_accounts:
        account_id = d0379_account["account_id"]
        mpxn = d0379_account["mpxn"]
        #  We only include the MPxN in the D0379 file if we have the complete
        #  set of 48 half-hourly readings.
        #  Note that this is likely to change, as we may need to interpolate or
        # otherwise 'fill in the blanks' if data is not available for some of
        # the periods.
        try:
            hh_readings = df.loc[df["mpxn"] == mpxn].sort_values(by=["hhdate"])

            if len(hh_readings) > 48:
                raise TooManyHHReadingsException(account_id, mpxn, len(hh_readings))
            if len(hh_readings) < 48:
                raise InsufficientHHReadingsException(
                    account_id, mpxn, len(hh_readings)
                )

            logger.info(
                "Account ID: {}, MPxN: {}, num HH readings: {}".format(
                    account_id, mpxn, len(hh_readings)
                )
            )

            # Build up the lines for this record, then add them to the master
            # `lines` list at the end. This was, if we encounter an exception
            #  during processing, we will not add an incomplete record to the
            #  D0379 file.
            record_lines = []
            record_lines.append(
                "25B|{0}|{1}|{2}|".format(mpxn, measurement_quantity_id, supplier_id)
            )
            record_lines.append("26B|{0}|".format(d0379_date.strftime("%Y%m%d")))

            for row in hh_readings.itertuples():
                # The Actual/Estimated indicator is always set to 'A' as we
                # do not currently generated estimates, the values are always
                # based on reads received from the meter.
                actual_estimated_indicator = "A"
                
                # The Smart Metered Period Consumption needs to be inserted
                # into the D0379 as kWh, however the values we get back from
                # the CDW are in Wh, hence the conversion here.
                smart_metered_period_consumption = row.primaryvalue / 1000

                record_lines.append(
                    "66L|{0}|{1:.3f}|".format(
                        actual_estimated_indicator,
                        smart_metered_period_consumption,
                    )
                )

            flow_count += 1

            lines.extend(record_lines)

        except (TooManyHHReadingsException, InsufficientHHReadingsException) as e:
            # We do not want to abandon processing if there are too many / too few
            #  HH readings for a single account, but we want to report the exception
            # to Sentry for visibility.
            logger.error(traceback.format_exc())
            sentry_sdk.capture_exception(e)

    # -1 is to remove the header from the group count as it should not be included
    total_group_count = len(lines) - 1

    footer = "ZPT|{0}|{1}||{2}|{3}|".format(
        fileid,
        total_group_count,
        flow_count,
        d0379_file_creation_timestamp,
    )

    lines.append(footer)

    return "\n".join(lines)


def generate_d0379(d0379_date):
    """
    Generates a D0379 file for the date specified and writes it to S3.

    :param d0379_date: The date for which the D0379 should be generated, as a datetime.date
    :returns: None
    :raises Exception: raises an exception if the destination S3 bucket is inaccessible
    """

    try:
        directory = common.utils.get_dir()

        s3_destination_bucket = directory["s3_bucket"]
        s3_key_prefix = common.directories.common["elective_hh"]["d0379_s3_key_prefix"]
        d0379_accounts = fetch_d0379_accounts()

        df = fetch_d0379_data(d0379_accounts, d0379_date)

        fileid = int(time.time())

        s3_key = "{}/D0379_{}_{}.txt".format(
            s3_key_prefix,
            d0379_date.strftime("%Y%m%d"),
            fileid,
        )

        d0379_text = dataframe_to_d0379(d0379_accounts, d0379_date, df, fileid)
        write_to_s3(d0379_text, s3_destination_bucket, s3_key)
    except Exception as e:
        logger.error(traceback.format_exc())
        raise e


# move file from s3 to sftp
def copy_d0379_to_sftp(d0379_date):
    """
    Copies all files in S3 that start with D0379_{d0379_date}_ to SFTP location
    """

    try:
        directory = common.utils.get_dir()

        s3_destination_bucket = directory["s3_bucket"]
        s3_key_prefix = "{}/D0379_{}_".format(
            common.directories.common["elective_hh"]["d0379_s3_key_prefix"],
            d0379_date.strftime("%Y%m%d"),
        )
        sftp_host = config.elective_hh_sftp_server["sftp_host"]
        sftp_username = config.elective_hh_sftp_server["sftp_username"]
        sftp_password = config.elective_hh_sftp_server["sftp_password"]
        sftp_prefix = config.elective_hh_sftp_server["sftp_prefix"]

        aws_access_key_id = config.s3_config["access_key"]
        aws_secret_access_key = config.s3_config["secret_key"]

        s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

        s3_response = s3_client.list_objects_v2(
            Bucket=s3_destination_bucket, Prefix=s3_key_prefix
        )

        if s3_response["KeyCount"] > 0:
            keys = [x["Key"] for x in s3_response["Contents"]]
            logger.info(
                "{} objects found in {} matching prefix {}".format(
                    len(keys), s3_destination_bucket, s3_key_prefix
                )
            )

            for key in keys:
                d0379_object = s3_client.get_object(
                    Bucket=s3_destination_bucket, Key=key
                )

                basename = os.path.basename(key)

                sftp_uri = "sftp://{}:{}@{}{}".format(
                    sftp_username,
                    sftp_password,
                    sftp_host,
                    "{}/{}".format(sftp_prefix, basename),
                )

                logger.debug(
                    "SFTP URI: {}".format(sftp_uri.replace(sftp_password, "<redacted>"))
                )

                with smart_open.open(sftp_uri, "w") as fout:
                    fout.write(d0379_object["Body"].read().decode("utf-8"))

        else:
            raise NoD0379FileFound(s3_destination_bucket, s3_key_prefix)
    except Exception as e:
        logger.error(traceback.format_exc())
        raise e
