import pandas_redshift as pr
import sys
import boto3
import pymysql as psql
import pysftp
import paramiko

sys.path.append('..')
from conf import config as con


def get_rds_connection():
    conn = psql.connect(host=con.rds_config['host'], port=con.rds_config['port'], user=con.rds_config['user'],
                           passwd=con.rds_config['pwd'], db=con.rds_config['db'])

    return conn

def close_rds_connection(cursor, connection):
    cursor.close()
    connection.close()



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


def get_ensek_sftp_connection():
    try:
        ensek_sftp = con.ensek_sftp_config
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None

        # print(ensek_sftp)
        sftp = pysftp.Connection(host=ensek_sftp['host'], username=ensek_sftp['username'], password=ensek_sftp['password'], cnopts=cnopts)

        # transport = paramiko.Transport((ensek_sftp['host'], 22))
        # sftp = paramiko.SFTPClient.from_transport(transport)

        return sftp
    except Exception as e:
        print("Error: " + str(e))


if __name__ == "__main__":
    get_redshift_connection()
