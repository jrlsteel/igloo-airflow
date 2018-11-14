import pandas_redshift as pr
import sys
import os
import boto3

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from conf import config as con


def get_redshift_connection():
    try:
        pr.connect_to_redshift(host=con.redshift_config['host'], port=con.redshift_config['port'],
                               user=con.redshift_config['user'], password=con.redshift_config['pwd'],
                               dbname=con.redshift_config['db'])
        print("Connected to Redshift")
    except ConnectionError as e:
        sys.exit("Error : " + str(e))


def close_redshift_connection():
    pr.close_up_shop()
    print("Connection to Redshift Closed")


def get_S3_Connections_resources():
    access_key = con.s3_config['access_key']
    secret_key = con.s3_config['secret_key']
    # print(access_key)
    # print(secret_key)

    s3 = boto3.resource('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    return s3


def get_S3_Connections_client():
    access_key = con.s3_config['access_key']
    secret_key = con.s3_config['secret_key']
    # print(access_key)
    # print(secret_key)

    s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    return s3


if __name__ == "__main__":
    get_redshift_connection()
