import json as json
import pandas as pd
import pandas_redshift as pr
import pymysql


class TableDiffChecker:
    def __init__(self):
        self.env_configs = {}

    def set_environment_config(self, env_name, env_config):
        self.env_configs[env_name] = env_config

    def compare_objects(self, comparison_type, env_a_name, object_a_def, key_cols, env_b_name=None, object_b_def=None):
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
    def compare_key_sets(df_a, df_b, key_cols):
        # key_cols should be a python list of strings, one string per field, such as ['account_id', 'meterpoint_id']
        keyset_res = {
            "table_a": {
                "total_rows": "",
                "num_key_sets": "",
                "num_unique_key_sets": "return a list of these internally only",
                "num_duplicated_key_sets": ""
            },
            "table_b": {
                "total_rows": "",
                "num_key_sets": "",
                "num_unique_key_sets": "return a list of these internally only",
                "num_duplicated_key_sets": ""
            },
            "comparison": {
                "num_unique_keys_a_only": "",
                "num_unique_keys_common": "return a list of these internally only",
                "num_unique_keys_b_only": "",
            }
        }

        key_fields_a = df_a[key_cols]
        key_fields_b = df_b[key_cols]

        return keyset_res

    # Gets the number of unique rows, split into those used only once and those used many times, from the dataframe
    @staticmethod
    def get_counts_on_slice(df_slice):
        unique_row_counts = df_slice.value_counts()
        single_use_rows = unique_row_counts[unique_row_counts == 1]

        slice_counts_res = {
            "total_rows": len(df_slice.index),
            "num_key_sets": len(unique_row_counts.index),
            "num_unique_key_sets": len(single_use_rows.index)
        }
        slice_counts_res["num_duplicated_key_sets"] = slice_counts_res["num_key_sets"] - slice_counts_res[
            "num_unique_key_sets"]

        return slice_counts_res

    @staticmethod
    def compare_schemas(df_a, df_b):
        schemas_res = {
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
        for field, dtype in schemas_res["schema_a"].items():
            if field in schemas_res["schema_b"].keys():
                # field present in both schemas, check for type mismatch
                if dtype != schemas_res["schema_b"][field]:
                    schemas_res["type_mismatches"].append({
                        "field": field,
                        "dtype_a": dtype,
                        "dtype_b": schemas_res["schema_b"][field]
                    })
                    schemas_res["type_match"] = False
                    schemas_res["full_match"] = False
                else:
                    # exact match
                    schemas_res["common_fields"].append({
                        "field": field,
                        "dtype": dtype
                    })
            else:
                # field unique to query a
                schemas_res["unique_fields"].append({
                    "field_name": field,
                    "dtype": dtype,
                    "unique_to": "A"
                })
                schemas_res["field_match"] = False
                schemas_res["full_match"] = False
        for field, dtype in schemas_res["schema_b"].items():
            if field not in schemas_res["schema_a"].keys():
                # field unique to query b
                schemas_res["unique_fields"].append({
                    "field_name": field,
                    "dtype": dtype,
                    "unique_to": "B"
                })
                schemas_res["field_match"] = False
                schemas_res["full_match"] = False

        return schemas_res

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
    from conf.config import redshift_config as olduat_config

    olduat_config["database_type"] = "redshift"
    from conf.config import redshift_config_prod as oldprod_config

    oldprod_config["database_type"] = "redshift"
    from conf.config import redshift_newprod as newprod_config

    newprod_config["database_type"] = "redshift"

    tdc = TableDiffChecker()
    tdc.set_environment_config(env_name="old_prod", env_config=oldprod_config)
    tdc.set_environment_config(env_name="new_prod", env_config=newprod_config)
    res = tdc.compare_objects(comparison_type="table",
                              env_a_name="old_prod",
                              env_b_name="new_prod",
                              object_a_def="ref_tariffs")
    print(res)
    print(json.dumps(res, indent=4))
