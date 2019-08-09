import sys
import boto3
import boto
from boto.s3.key import Key

sys.path.append('..')

from connections import connect_db as db
from utils.conf import config as con


class CopyStage2Files:
    """
    This script transfers the data from one table to another which are in different databases
        :param self._copy_from_env - copy from environment name
        :param self._copy_to_env - copy to environment name
        :param self.overwrite - True= truncates the copy_to table and then inserts the data
        :param self._copy_tables - List of tables to be copied from and copied to in key_value pairs
                {'copy_from': 'copy_from_table',
                 'copy_to': 'copy_to_table'}
    """

    def __init__(self):

        self._copy_from_env = 'prod'
        self._copy_to_env = 'uat'
        self._copy_from_bucket = 'igloo-data-warehouse-prod'
        self._copy_to_bucket = 'igloo-data-warehouse-uat'
        self.prefix = 'stage2'
        self.overwrite = True

    def get_env(self, env):

        if env == 'prod':
            env = con.prod

        if env == 'uat':
            env = con.uat

        if env == 'dev':
            env = con.dev

        return env

    def get_S3_Connections_client(self):
        env = con.com
        access_key = env['s3_config']['access_key']
        secret_key = env['s3_config']['secret_key']
        s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        return s3

    def get_files_from_stage2(self, _s3_from, _s3_to):
        """
        This function copies all the files from the directory from PROD to UAT
        """
        stage2_keys = []
        print('Reading Keys')
        more_objects = True
        found_token = False
        while more_objects:
            if found_token:
                response = _s3_from.list_objects_v2(
                    Bucket=self._copy_from_bucket,
                    Prefix=self.prefix,
                    ContinuationToken=found_token)
            else:
                response = _s3_from.list_objects_v2(
                    Bucket=self._copy_from_bucket,
                    Prefix=self.prefix,
                    )

            # use copy_object or copy_from
            for obj in response["Contents"]:
                stage2_keys.append(obj['Key'])

                # Now check there is more objects to list
            if "NextContinuationToken" in response:
                found_token = response["NextContinuationToken"]
                more_objects = True
            else:
                more_objects = False
            # print(len(d18_keys))
        print(stage2_keys)
        print(len(stage2_keys))
        # return stage2_keys

        print('copying data')
        for key in stage2_keys:
            temp = key.split('_')[-1]
            if key not in 'stage2/':
                print(key)
                copy_source = {
                    'Bucket': self._copy_from_bucket,
                    'Key': key
                }
                key1 = self.prefix + '/' + key.split('/')[-2] + '/' + key.split('/')[-1]
                _s3_to.copy(copy_source, self._copy_to_bucket, key1)

    def process_copy(self):
        _env_from = self.get_env(self._copy_from_env)
        _env_to = self.get_env(self._copy_to_env)
        s3_from = self.get_S3_Connections_client()
        # s3_to = self.get_S3_Connections_client(_env_to)

        try:
            self.get_files_from_stage2(s3_from, s3_from)

        except Exception as e:
            print('Error:' + str(e))


if __name__ == "__main__":

    p = CopyStage2Files()
    p.process_copy()