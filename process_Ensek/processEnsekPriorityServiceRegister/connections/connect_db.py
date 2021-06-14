import pandas_redshift as pr
import sys
import boto3
import pymysql as psql
import pysftp
import boto
from boto.s3.key import Key
from common.secrets_manager import get_secret

# sys.path.append('../..')
from process_Ensek.processEnsekPriorityServiceRegister.conf import config as con

client = boto3.client("secretsmanager")


def get_rds_connection():
    rds_config = get_secret(client, con.rds_config["secret_id"])
    conn = psql.connect(
        host=rds_config["host"],
        port=rds_config["port"],
        user=rds_config["username"],
        passwd=rds_config["password"],
        db=con.rds_config["db"],
    )

    return conn


def close_rds_connection(cursor, connection):
    cursor.close()
    connection.close()


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

        pr.connect_to_s3(
            aws_access_key_id=con.s3_config["access_key"],
            aws_secret_access_key=con.s3_config["secret_key"],
            bucket=con.s3_config["bucket_name"],
            subdirectory="aws-glue-tempdir/",
        )
        return pr

    except ConnectionError as e:
        sys.exit("Error : " + str(e))


def get_redshift_connection_prod():
    try:
        pr.connect_to_redshift(
            host=con.redshift_config_prod["host"],
            port=con.redshift_config_prod["port"],
            user=con.redshift_config_prod["user"],
            password=con.redshift_config_prod["pwd"],
            dbname=con.redshift_config_prod["db"],
        )
        print("Connected to Redshift")
        return pr

    except ConnectionError as e:
        sys.exit("Error : " + str(e))


def close_redshift_connection():
    pr.close_up_shop()
    print("Connection to Redshift Closed")


def get_S3_Connections_resources():
    access_key = con.uat["s3_config"]["access_key"]
    secret_key = con.uat["s3_config"]["secret_key"]
    # print(access_key)
    # print(secret_key)

    s3 = boto3.resource("s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    return s3


def get_S3_Connections_client():
    access_key = con.s3_config["access_key"]
    secret_key = con.s3_config["secret_key"]
    # print(access_key)
    # print(secret_key)

    s3 = boto3.client("s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    return s3


def get_ensek_sftp_connection():
    ensek_sftp_config = get_secret(client, con.ensek_sftp_config["secret_id"])
    try:
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None

        # print(ensek_sftp)
        sftp = pysftp.Connection(
            host=ensek_sftp_config["host"],
            username=ensek_sftp_config["username"],
            password=ensek_sftp_config["password"],
            cnopts=cnopts,
        )

        return sftp
    except Exception as e:
        print("Error: " + str(e))


def get_glue_connection():
    try:
        glue_client = boto3.client(
            service_name="glue",
            region_name="eu-west-1",
            aws_access_key_id=con.s3_config["access_key"],
            aws_secret_access_key=con.s3_config["secret_key"],
        )
        return glue_client

    except Exception as e:
        print("Error: " + str(e))


def get_boto_S3_Connections(env, bucket_name):
    # global k
    access_key = env["s3_config"]["access_key"]
    secret_key = env["s3_config"]["secret_key"]
    # print(access_key)
    # print(secret_key)

    s3 = boto.connect_s3(aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    bucket = s3.get_bucket(bucket_name)
    k = Key(bucket)
    return k


if __name__ == "__main__":
    get_redshift_connection()
