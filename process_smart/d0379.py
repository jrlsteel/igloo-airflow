import sys

sys.path.append("..")
import datetime
from connections.connect_db import get_boto_S3_Connections

import common.directories
import time
import traceback
import logging
import common.utils

logger = logging.getLogger('igloo.etl.d0379')
logger.setLevel('DEBUG')


class InsufficientHHReadingsException(Exception):
    """
    Raised when there are fewer than 48 half-hourly readings for a given MPxN
    """

    def __init__(self, mpxn, num_readings):
        self.mpxn = mpxn
        self.num_readings = num_readings
        super().__init__(
            "Insufficient readings for MPxN {}: {}".format(mpxn, num_readings)
        )


class TooManyHHReadingsException(Exception):
    """
    Raised when there are more than 48 half-hourly readings for a given MPxN
    """

    def __init__(self, mpxn, num_readings):
        self.mpxn = mpxn
        self.num_readings = num_readings
        super().__init__("Too many readings for MPxN {}: {}".format(mpxn, num_readings))


"""
This script is essential as part of the d3079 data flow. 
It pulls a redshift view containing half-hourly energy consumption of customers with a smart meter.
It then generates a txt file using data from the view in a format specified by a 3rd party.
The file is then placed in an s3 bucket where it will be accessed using sftp.

The process will be the same in each environment, except that the d0379 file is only collected using sftp from the s3 bucket in production.
"""


def fetch_d0379_data(elective_hh_trial_account_ids, d0379_date):
    """
    Executes a query to retrieve D0379 data for a given date.

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

    sql_query = common.directories.common["d0379"]["sql_query"].format(
        elective_hh_trial_account_ids=",".join(
            [str(x) for x in elective_hh_trial_account_ids]
        ),
        from_datetime=from_datetime.strftime("%Y-%m-%d %H:%M:%S"),
        to_datetime=to_datetime.strftime("%Y-%m-%d %H:%M:%S"),
    )

    logger.info("Exeuting SQL: {}".format(sql_query))

    df = common.utils.execute_query_return_df(sql_query)
    return df


def write_to_s3(d0379_text, s3_bucket, s3_key):
    logger.info("Writing D0379 file to s3://{}/{}".format(s3_bucket, s3_key))
    try:
        k = get_boto_S3_Connections(s3_bucket)
        k.key = s3_key
        k.set_contents_from_string(d0379_text)
    except Exception as e:
        logger.error(traceback.format_exc())
        raise e


def dataframe_to_d0379(d0379_date, df, fileid):
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

    now = datetime.datetime.now(datetime.timezone.utc)
    d0379_file_creation_timestamp = now.strftime("%Y%m%d%H%M%S")

    header = "ZHV|{0}|D0379001|X|{1}|C|UDMS|{2}||||OPER|".format(
        fileid,
        supplier_id,
        d0379_file_creation_timestamp,
    )
    footer = "ZPT|{0}|49||1|{1}|".format(
        fileid,
        d0379_file_creation_timestamp,
    )

    lines = []
    lines.append(header)

    unique_mpxns = df["mpxn"].unique()
    for mpxn in unique_mpxns:
        #  We only include the MPxN in the D0379 file if we have the complete
        #  set of 48 half-hourly readings.
        #  Note that this is likely to change, as we may need to interpolate or
        # otherwise 'fill in the blanks' if data is not available for some of
        # the periods.
        try:
            hh_readings = df.loc[df["mpxn"] == mpxn].sort_values(by=["hhdate"])

            if len(hh_readings) > 48:
                raise TooManyHHReadingsException(mpxn, len(hh_readings))
            if len(hh_readings) < 48:
                raise InsufficientHHReadingsException(mpxn, len(hh_readings))

            logger.info("MPxN: {}, num HH readings: {}".format(mpxn, len(hh_readings)))

            lines.append(
                "25B|{0}|{1}|{2}|".format(mpxn, measurement_quantity_id, supplier_id)
            )
            lines.append("26B|{0}|".format(d0379_date.strftime("%Y%m%d")))

            for row in hh_readings.itertuples():
                # The Smart Metered Period Consumption needs to be inserted
                # into the D0379 as kWh, however the values we get back from
                # the CDW are in Wh, hence the conversion here.
                smart_metered_period_consumption = row.primaryvalue / 1000

                lines.append(
                    "66L|{0}|{1:.3f}|".format(
                        row.measurement_class,
                        smart_metered_period_consumption,
                    )
                )
        except (TooManyHHReadingsException, InsufficientHHReadingsException):
            logger.error(traceback.format_exc())
            #  TODO: log to sentry

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
        s3_key_prefix = common.directories.common["d0379"]["s3_key_prefix"]
        elective_hh_trial_account_ids = common.directories.common["d0379"]["elective_hh_trial_account_ids"]

        df = fetch_d0379_data(elective_hh_trial_account_ids, d0379_date)

        fileid = int(time.time())

        s3_key = "{}/D0379_{}_{}.txt".format(
            s3_key_prefix,
            d0379_date.strftime("%Y%m%d"),
            fileid,
        )

        d0379_text = dataframe_to_d0379(d0379_date, df, fileid)
        write_to_s3(d0379_text, s3_destination_bucket, s3_key)
    except Exception as e:
        logger.error(traceback.format_exc())
        #  TODO: log to sentry
        raise e


# move file from s3 to sftp
def s3_to_sftp():
    pass
