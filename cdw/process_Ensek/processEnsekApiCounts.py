import os
import sys
import pandas_redshift as pr

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from cdw.conf import config as con

import boto3
from cdw.common.secrets_manager import get_secret

client = boto3.client("secretsmanager")


def get_count(table_name, schema):
    # sql to count
    sql = """select count(1) from """ + schema + table_name
    count = pr.redshift_to_pandas(sql)

    # convert the dataframe to string
    count_str = count.to_string(header=None, index=False)

    count_details = {"table_name": table_name, "count": count_str}
    print(count_details)
    return count_details


def get_redshift_connection():
    redshift_config = get_secret(client, con.redshift_config["secret_id"])
    try:
        pr.connect_to_redshift(
            host=redshift_config["host"],
            port=redshift_config["port"],
            user=redshift_config["username"],
            password=redshift_config["password"],
            dbname=con.redshift_config["db"],
        )
        print("Connected to Redshift")
    except ConnectionError as e:
        sys.exit("Error : " + str(e))


def process_count():

    get_redshift_connection()
    # list of staging table names to get count
    stage_tables = [
        "cdb_stagemeterpoints",
        "cdb_stagemeterpointsattributes",
        "cdb_stagemeters",
        "cdb_stagemetersattributes",
        "cdb_stageregisters",
        "cdb_stageregistersattributes",
        "cdb_stagereadings",
        "cdb_stagereadingsinternal",
        "cdb_stageestimatesgasinternal",
        "cdb_stageestimateselecinternal",
        "cdb_stageregistrationsgas",
        "cdb_stageregistrationselec",
        "cdb_stageaccountstatus",
        "cdb_stagetariffhistory",
        "cdb_stagetariffHistoryElecStandCharge",
        "cdb_stagetariffHistoryElecUnitRates",
        "cdb_stagetariffHistoryGasStandCharge",
        "cdb_stagetariffHistoryGasUnitRates",
    ]
    stage_schema = "aws_s3_ensec_api_extracts."
    stage_counts = []

    try:
        for table in stage_tables:
            stage_count = get_count(table, stage_schema)
            stage_counts.append(stage_count)
        return True

    except ConnectionError as e:
        raise Exception("Connection Error : " + str(e))

    except:
        raise

    finally:
        pr.close_up_shop()
        print("Connection to Redshift Closed")


if __name__ == "__main__":

    stage_counts_main = process_count()
    print(stage_counts_main)
