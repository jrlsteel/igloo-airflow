import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from process_ensek_api.schema_validation import validateSchema as vs
from process_ensek_api import processAllEnsekScripts as ae
from process_ensek_api import submit_staging_job as ss
from process_ensek_api import submit_customerdb_job as scdb
from process_ensek_api import processEnsekApiCounts as ec


def submit_schema_validations():
    try:
        schema_validation_response = vs.processAccounts()
        if schema_validation_response:
            print("Schema Validation job completed successfully")
            # return schema_validation_response
        else:
            print("Error occurred in Schema Validation job")
            # return schema_validation_response
            raise Exception
    except Exception as e:
        print("Error in Schema Validation :- " + str(e))
        sys.exit(1)


def submit_all_ensek_scripts():
    try:
        all_ensek_scripts_response = ae.process_all_ensek_scripts()
        if all_ensek_scripts_response:
            print("All Ensek Scripts job completed successfully")
            # return all_ensek_scripts_response
        else:
            print("Error occurred in All Ensek Scripts job")
            # return all_ensek_scripts_response
            raise Exception
    except Exception as e:
        print("Error in Ensek Scripts :- " + str(e))
        sys.exit(1)


def submit_staging_job():
    try:
        staging_job_response = ss.process_staging_job()
        if staging_job_response:
            print("Staging Job Completed successfully")
            # return staging_job_response
        else:
            print("Error occurred in Staging Job")
            # return staging_job_response
            raise Exception
    except Exception as e:
        print("Error in Staging Job :- " + str(e))
        sys.exit(1)


def submit_ensek_counts():
    try:
        ensek_counts_response = ec.process_count()
        if ensek_counts_response:
            print("Ensek Counts Job Completed successfully")
            # return ensek_counts_response
        else:
            print("Error occurred in Ensek Count Job")
            # return ensek_counts_response
            raise Exception
    except Exception as e:
        print("Error in Ensek Counts job :- " + str(e))
        sys.exit(1)


def submit_customerdb_job():
    try:
        staging_job_response = scdb.process_customerdb_job()
        if staging_job_response:
            print("CustomerDB Job Completed successfully")
            # return staging_job_response
        else:
            print("Error occurred in CustomerDB Job")
            # return staging_job_response
            raise Exception
    except Exception as e:
        print("Error in Customer DB Job :- " + str(e))
        sys.exit(1)


def process_ensek_api_jobs():

    while True:
        # run schema validation job
        print("schema validation running...")
        submit_schema_validations()
        # run all ensek scripts
        print("Ensek Scripts running...")
        submit_all_ensek_scripts()
        # run staging glue job
        print("Staging Job running...")
        submit_staging_job()
        print("Ensek Counts running...")
        submit_ensek_counts()
        print("CustomerDB Jobs Running...")
        submit_customerdb_job()
        print("All jobs completed successfully")


if __name__ == '__main__':
    process_ensek_api_jobs()

