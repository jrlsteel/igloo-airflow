import json as json
import pandas as pd
import pandas_redshift as pr
import pymysql


class TableDiffChecker:
    def __init__(self):
        self.env_configs = {}

    def set_environment_config(self, env_name, env_config):
        self.env_configs[env_name] = env_config

    def compare_objects(self, comparison_type, env_a_name, object_a_def, env_b_name=None, object_b_def=None):
        # set object def and/or env def for b to a if b is not provided (indicating same value)
        if object_b_def is None:
            object_b_def = object_a_def
        if env_b_name is None:
            env_b_name = env_a_name

        # Set up results dictionary
        comparison_log = {
            "comparison_type": comparison_type,
            "env_a": env_a_name,
            "env_b": env_b_name
        }

        # expand object definitions into queries based on defined comparison type
        if comparison_type == 'table':
            query_a = "select * from {0}".format(object_a_def)
            query_b = "select * from {0}".format(object_b_def)
            comparison_log["table_a"] = object_a_def
            comparison_log["table_b"] = object_b_def
        elif comparison_type == 'query':
            query_a = object_a_def
            query_b = object_b_def
            comparison_log["query_a"] = object_a_def
            comparison_log["query_b"] = object_b_def
        else:
            raise TypeError(
                "Comparison type '{0}' is unsupported. Supported types: 'table', 'query'".format(comparison_type))

        # load relevant environments
        env_a = self.env_configs[env_a_name]
        env_b = self.env_configs[env_b_name]

        # load data
        df_a = self.load_table(env_a, query_a)
        df_b = self.load_table(env_b, query_b)

        overall_match = True

        # compare schemas
        comparison_log["schemas"] = self.compare_schemas(df_a, df_b)
        overall_match = overall_match and comparison_log["schemas"]["full_match"]

        # compare row count
        comparison_log["row_count"] = self.compare_row_count(df_a, df_b)
        overall_match = overall_match and comparison_log["row_count"]["full_match"]

        comparison_log["overall_match"] = overall_match

        return comparison_log

    @staticmethod
    def compare_schemas(df_a, df_b):
        res = {
            "full_match": True,
            "field_match": True,
            "type_match": True,
            "schema_a": {field: df_a[field].dtype.name for field in list(df_a.columns)},
            "schema_b": {field: df_b[field].dtype.name for field in list(df_b.columns)},
            "common_fields": [],
            "type_mismatches": [],
            "unique_fields": []
        }

        # compare field names and data types
        for field, dtype in res["schema_a"].items():
            if field in res["schema_b"].keys():
                # field present in both schemas, check for type mismatch
                if dtype != res["schema_b"][field]:
                    res["type_mismatches"].append({
                        "field": field,
                        "dtype_a": dtype,
                        "dtype_b": res["schema_b"][field]
                    })
                    res["type_match"] = False
                    res["full_match"] = False
                else:
                    # exact match
                    res["common_fields"].append({
                        "field": field,
                        "dtype": dtype
                    })
            else:
                # field unique to query a
                res["unique_fields"].append({
                    "field_name": field,
                    "dtype": dtype,
                    "unique_to": "A"
                })
                res["field_match"] = False
                res["full_match"] = False
        for field, dtype in res["schema_b"].items():
            if field not in res["schema_a"].keys():
                # field unique to query b
                res["unique_fields"].append({
                    "field_name": field,
                    "dtype": dtype,
                    "unique_to": "B"
                })
                res["field_match"] = False
                res["full_match"] = False

        return res

    @staticmethod
    def compare_row_count(df_a, df_b):
        return {
            "full_match": len(df_a.index) == len(df_b.index),
            "row_count_a": len(df_a.index),
            "row_count_b": len(df_b.index)
        }

    @staticmethod
    def load_table(env, query):  # TODO - finish this bit
        if env['database_type'] == "redshift":
            # connect to redshift
            print("connecting to redshift")
            pr.connect_to_redshift(host=env['host'], port=env['port'],
                                   user=env['user'], password=env['pwd'],
                                   dbname=env['db'])
            print("connected to redshift")

            # read from redshift
            print("reading using sql '{0}'".format(query))
            data_df = pr.redshift_to_pandas(query)
            print("data read")

            # close redshift connection
            pr.close_up_shop()

        elif env['database_type'] == 'rds':
            # connect rds
            # read rds
            data_df = None
            pass
        else:
            raise TypeError("Database type '{0}' is not supported. Valid types are 'redshift' and 'rds'".format(
                env['database_type']))

        return data_df


if __name__ == '__main__':
    from conf.config import redshift_config as uat_config

    uat_config["database_type"] = "redshift"

    tdc = TableDiffChecker()
    tdc.set_environment_config(env_name="uat", env_config=uat_config)
    res = tdc.compare_objects(comparison_type="query",
                              env_a_name="uat",
                              object_a_def="select * from ref_meterpoints limit 100",
                              object_b_def="select * from ref_meters limit 100")
    print(res)
    print(json.dumps(res, indent=4))
