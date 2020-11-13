import json as json
import pandas as pd
import pandas_redshift as pr
import traceback
from datetime import datetime as dt
import pymysql
import sys
import argparse
import boto3

sys.path.append('..')
sys.path.append('/home/ec2-user/python/enzek-meterpoint-readings')
from common.utils import batch_logging_insert, batch_logging_update, get_jobID


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
            "env_b": env_b_name,
            "exec_success": True,
            "overall_match": True
        }

        try:
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
            print("Loading query A")
            df_a = self.load_table(env_a, query_a)
            print("Loading query B")
            df_b = self.load_table(env_b, query_b)

            # compare schemas
            print("Comparing schemas")
            comparison_log["schemas"] = self.compare_schemas(df_a, df_b)
            comparison_log["overall_match"] &= comparison_log["schemas"]["full_match"]

            # compare row count
            print("Comparing row counts")
            comparison_log["row_count"] = self.compare_row_count(df_a, df_b)
            comparison_log["overall_match"] &= comparison_log["row_count"]["full_match"]

            if comparison_log["row_count"]["row_count_a"] > 0 and comparison_log["row_count"]["row_count_b"] > 0:
                # compare key sets
                print("Comparing keys")
                comparison_log["key_sets"], common_keys = self.compare_key_sets(df_a, df_b, key_cols)
                comparison_log["overall_match"] &= comparison_log["key_sets"]["full_match"]

                # compare every other column (joined with the keyset)
                # filter each dataframe down to common keysets
                print("Comparing common fields & using shared keys")
                df_a_filtered = self.filter_data_frame_to_key_set(df_a, common_keys)
                df_b_filtered = self.filter_data_frame_to_key_set(df_b, common_keys)
                # get list of common-named columns (including those with differing data types) & remove etlchange
                shared_cols = list(comparison_log["schemas"]["common_fields"].keys()) + \
                              list(comparison_log["schemas"]["type_mismatches"].keys())
                if 'etlchange' in shared_cols:
                    shared_cols.remove('etlchange')
                comparison_log["common_keys_and_fields"] = self.compare_common_column_contents(
                    df_a=df_a_filtered,
                    df_b=df_b_filtered,
                    key_cols=key_cols,
                    common_cols=shared_cols
                )
                comparison_log["overall_match"] &= comparison_log["common_keys_and_fields"]["full_match"]

        except Exception:
            comparison_log["exec_success"] = False
            comparison_log["exception"] = traceback.format_exc()
            traceback.print_exc()

        return comparison_log

    @staticmethod
    def compare_common_column_contents(df_a, df_b, key_cols, common_cols):
        result_counts = {
            "full_match": True,
            "differing_cols": 0
        }
        for comparison_col in common_cols:
            # find equivalence between dataframes
            cols = key_cols.copy()
            if comparison_col not in key_cols:
                cols.append(comparison_col)
            # the following two slices are only necessary due to the size of some tables (such as the daily metering
            # report). The merge method below can work directly on full tables but runs out of memory on the large ones
            df_a_slice = df_a[cols]
            df_b_slice = df_b[cols]
            tagged_rows = df_a_slice.merge(df_b_slice, on=cols, how='outer', indicator=True)

            # there will be the same number of right_only and left_only tags, no need to log both
            differing_values = sum(tagged_rows["_merge"] == "left_only")
            result_counts[comparison_col] = {
                "part_of_key": comparison_col in key_cols,
                "nulls_a": len(df_a.index) - int(df_a[comparison_col].count()),
                "nulls_b": len(df_b.index) - int(df_b[comparison_col].count()),
                "common_values": sum(tagged_rows["_merge"] == "both"),
                "differing_values": differing_values
            }
            if differing_values > 0:
                result_counts["full_match"] = False
                result_counts["differing_cols"] += 1

        return result_counts

    @staticmethod
    def filter_data_frame_to_key_set(df, key_set):
        key_cols = list(key_set.columns)
        df_multiindex = df.set_index(key_cols).index
        key_set_multiindex = key_set.set_index(key_cols).index
        df = df[df_multiindex.isin(key_set_multiindex)]
        return df

    @staticmethod
    def compare_key_sets(df_a, df_b, key_cols):
        # key_cols should be a python list of strings, one string per field, such as ['account_id', 'meterpoint_id']

        if not set(key_cols).issubset(df_a.columns):
            raise RuntimeError("Dataframe A does not contain the specified key columns")
        if not set(key_cols).issubset(df_b.columns):
            raise RuntimeError("Dataframe B does not contain the specified key columns")

        key_res_a, key_set_a = TableDiffChecker.get_unique_key_stats(df_a, key_cols)
        key_res_b, key_set_b = TableDiffChecker.get_unique_key_stats(df_b, key_cols)

        # compare single-use keys from both tables
        field_names = list(key_set_a.columns)
        tagged_rows = key_set_a.merge(key_set_b, on=field_names, how='outer', indicator=True)
        common_keys = tagged_rows[tagged_rows["_merge"] == "both"][field_names]
        num_unique_a = sum(tagged_rows["_merge"] == "left_only")

        num_unique_b = sum(tagged_rows["_merge"] == "right_only")

        key_counts_match = True
        for metric in key_res_a.keys():
            key_counts_match &= (key_res_a[metric] == key_res_b[metric])
        single_use_match = num_unique_a == 0 and num_unique_b == 0
        key_set_res = {
            "full_match": single_use_match and key_counts_match,
            "key_counts_match": key_counts_match,
            "single_use_keys_match": single_use_match,
            "one_key_per_row": key_res_a["num_duplicated_key_sets"] == 0 and key_res_b["num_duplicated_key_sets"] == 0,
            "table_a": key_res_a,
            "table_b": key_res_b,
            "unique_to_a": num_unique_a,
            "common_keys": sum(tagged_rows["_merge"] == "both"),
            "unique_to_b": num_unique_b
        }

        return key_set_res, common_keys

    # Gets the number of unique rows, split into those used only once and those used many times, from the dataframe
    @staticmethod
    def get_unique_key_stats(df, subset_cols):
        unique_row_counts = df.value_counts(subset=subset_cols)
        # Pick out rows which only have a single occurrence,
        # convert the resulting multiindex series to a dataframe,
        # use the series index names as column names,
        # remove the count column (named 0)
        single_use_rows = unique_row_counts[unique_row_counts == 1] \
            .to_frame() \
            .reset_index() \
            .drop(columns=[0])

        slice_counts_res = {
            "total_rows": len(df.index),
            "num_key_sets": len(unique_row_counts.index),
            "num_single_use_key_sets": len(single_use_rows.index)
        }
        slice_counts_res["num_duplicated_key_sets"] = slice_counts_res["num_key_sets"] - \
                                                      slice_counts_res["num_single_use_key_sets"]

        return slice_counts_res, single_use_rows

    @staticmethod
    def compare_schemas(df_a, df_b):
        schemas_res = {
            "full_match": True,
            "field_match": True,
            "type_match": True,
            "schema_a": {field: df_a[field].dtype.name for field in list(df_a.columns)},
            "schema_b": {field: df_b[field].dtype.name for field in list(df_b.columns)},
            "common_fields": {},
            "type_mismatches": {},
            "unique_to_a": {},
            "unique_to_b": {}
        }

        # compare field names and data types
        for field, dtype in schemas_res["schema_a"].items():
            if field in schemas_res["schema_b"].keys():
                # field present in both schemas, check for type mismatch
                if dtype != schemas_res["schema_b"][field]:
                    schemas_res["type_mismatches"][field] = {
                        "dtype_a": dtype,
                        "dtype_b": schemas_res["schema_b"][field]
                    }
                    schemas_res["type_match"] = False
                    schemas_res["full_match"] = False
                else:
                    # exact match
                    schemas_res["common_fields"][field] = dtype
            else:
                # field unique to query a
                schemas_res["unique_to_a"][field] = dtype
                schemas_res["field_match"] = False
                schemas_res["full_match"] = False

        for field, dtype in schemas_res["schema_b"].items():
            if field not in schemas_res["schema_a"].keys():
                # field unique to query b
                schemas_res["unique_to_b"][field] = dtype
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
    def load_table(env, query):
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
            print("connecting to RDS")
            conn = pymysql.connect(env['host'],
                                   user=env['user'],
                                   passwd=env['pwd'],
                                   db=env['db'],
                                   port=env['port'],
                                   charset='utf8')
            print("connected to RDS")

            # read rds
            print("reading using sql '{0}'".format(query))
            data_df = pd.read_sql(query, conn)
            print("data read")

            # close connection
            conn.close()
        else:
            raise TypeError("Database type '{0}' is not supported. Valid types are 'redshift' and 'rds'".format(
                env['database_type']))

        return data_df


# compares the calculated tables between old & new environments
def compare_calculated_tables(stage=None):
    job_id = get_jobID()
    batch_logging_insert(job_id, 70, 'calculated_table_comparisons', 'compare_tables.py')

    try:
        from process_table_comparisons.table_definitions import calculated_table_keys
        from conf.config import redshift_comparison_configs
        if stage is None:
            from conf.config import environment_config as current_environment
            stage = current_environment["environment"]
        old_env_name = "old_" + stage
        new_env_name = "new_" + stage

        tdc = TableDiffChecker()
        tdc.set_environment_config(env_name=old_env_name, env_config=redshift_comparison_configs["old_env"])
        tdc.set_environment_config(env_name=new_env_name, env_config=redshift_comparison_configs["new_env"])

        results = {
            "full_match": {

            },
            "schema_mismatch": {

            },
            "data_mismatch": {

            },
            "exec_failure": {

            }
        }
        for table_name, key_cols in calculated_table_keys.items():
            res = tdc.compare_objects(comparison_type="table",
                                      env_a_name=old_env_name,
                                      env_b_name=new_env_name,
                                      object_a_def=table_name,
                                      key_cols=key_cols)
            if not res["exec_success"]:
                results["exec_failure"][table_name] = res
            elif not res["schemas"]["full_match"]:
                results["schema_mismatch"][table_name] = res
            elif res["overall_match"]:
                results["full_match"][table_name] = res
            else:
                results["data_mismatch"][table_name] = res

        time = dt.now().strftime("%Y%m%d-%H%M%S")
        fname = "calculated_tables_comparison_{stage}_{datetime}.json".format(stage=stage, datetime=time)
        with open(fname, 'w') as outfile:
            json.dump(results, outfile, indent=4)

        if len(results["exec_failure"]) > 0:
            raise RuntimeError("{num_exec_fail} failure(s) encountered in calculated table comparisons".format(
                num_exec_fail=len(results["exec_failure"])))
    except Exception as e:
        print("Error in calculated table comparison script: " + str(e))
        batch_logging_update(job_id, 'f', str(e))
        raise e

    batch_logging_update(job_id, 'e')


def compare_tables(table_comparison_config_name, output_to_s3=False):
    job_id = get_jobID()
    batch_logging_insert(job_id, 70, table_comparison_config_name, 'compare_tables.py')

    exit_status = 0

    try:
        from conf.config import table_comparison_configs

        table_comparison_config = table_comparison_configs[table_comparison_config_name]

        old_env_name = table_comparison_config["old_env"]["name"]
        new_env_name = table_comparison_config["new_env"]["name"]

        tdc = TableDiffChecker()
        tdc.set_environment_config(env_name=old_env_name, env_config=table_comparison_config["old_env"]["redshift_config"])
        tdc.set_environment_config(env_name=new_env_name, env_config=table_comparison_config["old_env"]["redshift_config"])

        results = {
            "full_match": {

            },
            "schema_mismatch": {

            },
            "data_mismatch": {

            },
            "exec_failure": {

            }
        }
        for table_name, key_cols in table_comparison_config["table_keys"].items():
            res = tdc.compare_objects(comparison_type="table",
                                      env_a_name=old_env_name,
                                      env_b_name=new_env_name,
                                      object_a_def=table_name,
                                      key_cols=key_cols)
            if not res["exec_success"]:
                results["exec_failure"][table_name] = res
                exit_status = 1
            elif not res["schemas"]["full_match"]:
                results["schema_mismatch"][table_name] = res
                exit_status = 1
            elif res["overall_match"]:
                results["full_match"][table_name] = res
            else:
                results["data_mismatch"][table_name] = res
                exit_status = 1

        time = dt.now().strftime("%Y%m%d-%H%M%S")
        fname = "tables_comparison_{name}_{datetime}.json".format(name=table_comparison_config_name, datetime=time)
        with open(fname, 'w') as outfile:
            json.dump(results, outfile, indent=4)

        if output_to_s3:
            from conf.config import s3_config
            s3 = boto3.resource(
                's3',
                aws_access_key_id=s3_config["access_key"],
                aws_secret_access_key=s3_config["secret_key"]
            )
            object = s3.Object(s3_config["bucket_name"], 'table_comparisons/{}'.format(fname))
            object.put(Body=json.dumps(results))

        if len(results["exec_failure"]) > 0:
            raise RuntimeError("{num_exec_fail} failure(s) encountered in calculated table comparisons".format(
                num_exec_fail=len(results["exec_failure"])))
    except Exception as e:
        print("Error in table comparison script: " + str(e))
        batch_logging_update(job_id, 'f', str(e))
        raise e

    batch_logging_update(job_id, 'e')

    return exit_status
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Database table comparison tool')
    parser.add_argument('--table-comparison-config', type=str)
    parser.add_argument('--output-to-s3', action="store_true")

    args = parser.parse_args()

    # If no arguments specified, just compare all calculated tables
    # and store results locally.
    if args.table_comparison_config is None:
        compare_calculated_tables()
    else:
        print("Comparing tables using config: {}".format(args.table_comparison_config))
        exit_status = compare_tables(args.table_comparison_config, output_to_s3=args.output_to_s3)
        print("Exiting with status {}".format(exit_status))
        sys.exit(exit_status)
