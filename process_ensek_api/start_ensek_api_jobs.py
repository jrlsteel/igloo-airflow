import sys
import os
from time import sleep
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from process_ensek_api.schema_validation import validateSchema as vs
from process_ensek_api import processAllEnsekScripts as ae
from process_ensek_api import processEnsekApiCounts as ec
from common import process_glue_job as glue


def submit_schema_validations():
    try:
        schema_validation_response = vs.processAccounts()
        if schema_validation_response:
            print("{0}: Schema Validation job completed successfully".format(datetime.now().strftime('%H:%M:%S')))
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
            print("{0}: All Ensek Scripts job completed successfully".format(datetime.now().strftime('%H:%M:%S')))
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
        obj_stage = glue.ProcessGlueJob(job_name='process_staging_files', input_files='ensek_files')
        staging_job_response = obj_stage.run_glue_job()
        if staging_job_response:
            print("{0}: Staging Job Completed successfully".format(datetime.now().strftime('%H:%M:%S')))
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
            print("{0}: Ensek Counts Job Completed successfully".format(datetime.now().strftime('%H:%M:%S')))
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
        obj_trigger = glue.ProcessGlueJob(trigger_name='ensek-customerdb-ondemand')
        trigger_job_response = obj_trigger.run_glue_trigger()
        if trigger_job_response:
            print("{0}: CustomerDB Glue trigger started successfully".format(datetime.now().strftime('%H:%M:%S')))
            # return staging_job_response
        else:
            print("Error occurred in CustomerDB Job")
            # return staging_job_response
            raise Exception
    except Exception as e:
        print("Error in Customer DB Job :- " + str(e))
        sys.exit(1)


def process_ensek_api_jobs():

    # run schema validation job
    print("{0}: Schema validation running...".format(datetime.now().strftime('%H:%M:%S')))
    submit_schema_validations()

    # run all ensek scripts
    print("{0}: Ensek Scripts running...".format(datetime.now().strftime('%H:%M:%S')))
    submit_all_ensek_scripts()

    # run staging glue job
    print("{0}: Staging Job running...".format(datetime.now().strftime('%H:%M:%S')))
    submit_staging_job()

    """disabled for now as we cannot ssh access to igloo-dwh from EC2
    # print("Ensek Counts running...".format(datetime.now().strftime('%H:%M:%S')))
    submit_ensek_counts() """

    print("{0}: CustomerDB Jobs running...".format(datetime.now().strftime('%H:%M:%S')))
    submit_customerdb_job()
    # # wait for 10 minutes before starting the next run
    # sleep(600)

    print("{0}: All jobs completed successfully".format(datetime.now().strftime('%H:%M:%S')))


if __name__ == '__main__':
    process_ensek_api_jobs()

