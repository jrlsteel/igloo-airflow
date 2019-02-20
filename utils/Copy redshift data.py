import sys
import pandas_redshift as pr

sys.path.append('..')
from conf import config as con


class CopyRedshiftdata:
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

        self._copy_from_env = 'dev'
        self._copy_to_env = 'prod'
        self.overwrite = True
        self._copy_tables = [
                            # {'copy_from': 'ref_registrations_status_elec_audit',
                            #  'copy_to': 'temp_ref_registrations_status_elec_audit_dev'},
                            #  {'copy_from': 'ref_registrations_status_gas_audit',
                            #   'copy_to': 'temp_ref_registrations_status_gas_audit_dev'}
                             ]

    def get_connection(self, env):
        try:
            pr.connect_to_redshift(host=env['redshift_config']['host'], port=env['redshift_config']['port'],
                                   user=env['redshift_config']['user'], password=env['redshift_config']['pwd'],
                                   dbname=env['redshift_config']['db'])
            print("Connected to Redshift : " + str(env['redshift_config']['db']))

            pr.connect_to_s3(aws_access_key_id=env['s3_config']['access_key'],
                             aws_secret_access_key=env['s3_config']['secret_key'],
                             bucket=env['s3_config']['bucket_name'],
                             subdirectory='aws-glue-tempdir/')
        except Exception as e:
            raise e

    def close_connection(self):
        pr.close_up_shop()

    def get_env(self, env):

        if env == 'prod':
            env = con.prod

        if env == 'uat':
            env = con.uat

        if env == 'dev':
            env = con.dev

        return env

    def process_copy(self):
        try:
            if len(self._copy_tables) == 0:
                print("Copy tables data is empty")

            else:
                for table in self._copy_tables:
                    _env_from = self.get_env(self._copy_from_env)
                    self.get_connection(_env_from)
                    copy_from_sql = "select * from {0}".format(table['copy_from'])
                    print('Copying data from : ' + table['copy_from'])
                    df_copy_from = pr.redshift_to_pandas(copy_from_sql)
                    # print(df_copy_from.to_csv(None, index=False))
                    self.close_connection()

                    if df_copy_from.empty:
                        return "No data to copy"
                    else:
                        _env_to = self.get_env(self._copy_to_env)
                        self.get_connection(_env_to)
                        if self.overwrite:
                            print('Deleting existing data from : '+ table['copy_to'])
                            delete_sql = "delete from {0}".format(table['copy_to'])
                            pr.exec_commit(delete_sql)

                        print('Inserting data into : ' + table['copy_to'])
                        pr.pandas_to_redshift(df_copy_from, table['copy_to'], append=True, index=False)
                        self.close_connection()
                        print('Total records copied :' + str(df_copy_from.count()))

        except Exception as e:
            print('Error:' + str(e))


if __name__ == "__main__":
    p = CopyRedshiftdata()
    p.process_copy()