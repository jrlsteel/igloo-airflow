import sys
import pandas_redshift as pr
import pandas as pd

from utils.conf1 import config as con


class SynergyEmailCheck:
    def __init__(self):
        self.email_hash_sql = """select distinct rm.account_id,
                                                    users.email,
                                                    func_sha1(lower(users.email)) as hash,
                                                    case
                                                        when max(nvl(least(rm.supplyenddate, rm.associationenddate), getdate() + 1)) > getdate()
                                                            then 'current'
                                                        else 'past' end           as customer_type
                                    from ref_meterpoints rm
                                             inner join ref_cdb_supply_contracts sc on rm.account_id = sc.external_id
                                             inner join ref_cdb_user_permissions up on up.permissionable_type = 'App\\\\SupplyContract' and
                                                                                       up.permissionable_id = sc.id
                                             inner join ref_cdb_users users on users.id = up.user_id
                                    --where account_id in (1831, 54977)
                                    group by rm.account_id, users.email
                                    order by account_id"""

    def get_connection(self, env):
        try:
            pr.connect_to_redshift(
                host=env["redshift_config"]["host"],
                port=env["redshift_config"]["port"],
                user=env["redshift_config"]["user"],
                password=env["redshift_config"]["pwd"],
                dbname=env["redshift_config"]["db"],
            )
            print("Connected to Redshift : " + str(env["redshift_config"]["db"]))

            pr.connect_to_s3(
                aws_access_key_id=env["s3_config"]["access_key"],
                aws_secret_access_key=env["s3_config"]["secret_key"],
                bucket=env["s3_config"]["bucket_name"],
                subdirectory="aws-glue-tempdir/",
            )
        except Exception as e:
            raise e

    def close_connection(self):
        pr.close_up_shop()

    def get_env(self, env):

        if env == "prod":
            env = con.prod

        if env == "uat":
            env = con.uat

        if env == "dev":
            env = con.dev

        return env

    def get_email_hash_matches(self, filename):
        try:
            print("reading synergy hashes")
            # get synergy email hashes from csv
            df_seh = pd.read_csv(filename)
            print(df_seh.size)

            print("reading igloo hashes")
            # get igloo email hashes (ieh)
            self.get_connection(con.prod)
            df_ieh = pr.redshift_to_pandas(self.email_hash_sql)
            self.close_connection()
            print(df_ieh["hash"].size)

            print("comparing")
            # find matches
            matches = pd.merge(left=df_seh, right=df_ieh, right_on="hash", left_on=df_seh.columns[0], how="inner")

            print("writing matches")
            outfilename = filename.split(".")[0] + "_igloo_matches.csv"
            matched_hashes = matches["hash"]
            matched_hashes.to_csv(outfilename, header=True, index=False)

            outfilename = filename.split(".")[0] + "_igloo_matches_full.csv"
            matches.to_csv(outfilename, header=True, index=False)

            return matches
        except Exception as e:
            print("Error:" + str(e))


if __name__ == "__main__":
    fn = "Sha 1 Hash.csv"
    p = SynergyEmailCheck()
    matches = p.get_email_hash_matches(fn)
    print(matches)
