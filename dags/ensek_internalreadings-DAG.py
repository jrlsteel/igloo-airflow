import sys


import datetime
from airflow.utils.dates import days_ago
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import sentry_sdk

import cdw.common.utils as util
from cdw.common.process_glue_job import ProcessGlueJob as glue_job
from cdw.common.slack_utils import alert_slack
from cdw.process_verification.verification_template import (
    verify_new_api_response_files_in_s3_directory,
    verify_seventeen_new_files_in_s3,
    ref_verification_step,
)
from cdw.common.directories import common

args = {
    "owner": "Airflow",
    "start_date": days_ago(2),
    "on_failure_callback": alert_slack,
}


dag = DAG(
    dag_id="ensek_internalreadings",
    default_args=args,
    schedule_interval=None,
    tags=["cdw", "ensek", "internalreadings"],
    catchup=False,
    description="Extracts Meter Reading information for each account ID from the following endpoint: https://igloo.ignition.ensek.co.uk/api/account/{<account_id>}/meter-readings?sortField=meterReadingDateTime&sortDirection=Descending",
)


directory = util.get_dir()
s3_key = directory["s3_key"]
s3_bucket = directory["s3_bucket"]
environment = util.get_env()
staging_job_name = directory["glue_staging_internalreadings_job_name"]
ref_job_name = directory["glue_ref_internalreadings_job_name"]
staging_process_name = "ensek-readings-internal"
ref_process_name = "ensek_ref_readings"

date_today_string = str(datetime.date.today())


def process_glue_job(job_name, process_name):
    try:
        print("job_name-- ", job_name)
        print("s3_bucket-- ", s3_bucket)
        print("environment-- ", environment)
        print("process_name-- ", process_name)
        obj_stage = glue_job(
            job_name=job_name,
            s3_bucket=s3_bucket,
            environment=environment,
            processJob=process_name,
        )
        job_response = obj_stage.run_glue_job()
        if job_response:
            print("{0}: Staging Job Completed successfully".format(datetime.datetime.now().strftime("%H:%M:%S")))
        else:
            print("Error occurred in Staging Job")
            raise Exception
    except Exception as e:
        print("Error in Staging Job :- " + str(e))
        print("Unexpected error:", sys.exc_info()[0])
        raise


def sentry_wrapper(python_task_variable):
    """
    :param: execution_date a string in the form 'YYYY-MM-DD'
    """
    try:
        python_task_variable
    except Exception as e:
        sentry_sdk.capture_exception(e)
        sentry_sdk.flush(5)
        raise e


# DAG Tasks

api_extract_internalreadings = BashOperator(
    task_id="api_extract_internalreadings_bash",
    bash_command="cd /opt/airflow/cdw/process_Ensek/processEnsekReadings && python process_ensek_internal_readings.py",
    dag=dag,
)
api_extract_internalreadings.doc_md = """# Purpose
    
    Extracts meterreadings information from Ensek for each Account ID
    
    #Suggested action on failure
    
    Alert metering-squad, don't rerun
    
    # Justification
    
    This step has a long run time, and rerunning it would likely impact other batch processes and Ensek response times.
    """


api_extract_verify_internalreadings = PythonOperator(
    task_id="api_extract_verify_internalreadings",
    python_callable=verify_new_api_response_files_in_s3_directory,
    op_kwargs={
        "search_filter": date_today_string,
        "expected_value": "{{ var.value.LIVE_ACCOUNTS_FOR_VERIFY }}",
        "percent": "{{ var.value.PERCENT_FOR_API_VERIFY }}",
        "s3_prefix": s3_key["ReadingsInternal"],
    },
    dag=dag,
)
api_extract_verify_internalreadings.doc_md = """# Purpose
    Verification: There are sufficient new files in the s3 directory: {}, demonstrating the success of this API extracts

    # Suggest action on failure

    No action 

    # Justification

    As the step simply counts files in a directory there will be no change given it runs after extractions. 
    """.format(
    s3_key["ReadingsInternal"]
)

# Staging

staging_internalreadings = PythonOperator(
    task_id="staging_internalreadings",
    python_callable=process_glue_job,
    op_args=[staging_job_name, staging_process_name],
    dag=dag,
)
staging_internalreadings.doc_md = """# Purpose
    Runs glue job on the stage 1 internalreadings files which reduces them to 17 parquet files and writes them to stage 2

    # Suggest action on failure

    No action 

    # Justification

    Given this task is long running, rerunning it would impact other batch processes significantly.
    """


staging_verify_internalreadings = PythonOperator(
    task_id="staging_verify_internalreadings",
    python_callable=verify_seventeen_new_files_in_s3,
    op_kwargs={"s3_prefix": common["s3_keys"]["internal_readings_stage2"]},
    dag=dag,
)
staging_verify_internalreadings.doc_md = """# Purpose
    Verification: There are sufficient new files in the staging directory.

    # Suggest action on failure

    No action 

    # Justification

    As the step simply counts files in a directory there will be no change given it runs after extractions. 
    """


ref_tables_internalreadings = PythonOperator(
    task_id="ref_tables_internalreadings",
    python_callable=process_glue_job,
    op_args=[ref_job_name, ref_process_name],
    dag=dag,
)
ref_tables_internalreadings.doc_md = """# Purpose
    This step takes stage 2 data and inserts it into relavent redshift reference tables.

    # Suggest action on failure

    No action 

    # Justification

    Given this task is long running, rerunning it would impact other batch processes significantly.
    """


ref_tables_verify_internalreadings = PythonOperator(
    task_id="ref_tables_verify_internalreadings",
    python_callable=ref_verification_step,
    op_kwargs={
        "ref_readings_internal": "ref_readings_internal",
    },
    dag=dag,
)
ref_tables_verify_internalreadings.doc_md = """# Purpose
    Verification: There are sufficient new files in the staging directory.

    # Suggest action on failure

    No action 

    # Justification

    As the step simply counts files in a directory there will be no change given it runs after extractions. 
    """

# Dependencies

api_extract_internalreadings >> staging_internalreadings >> ref_tables_internalreadings
api_extract_internalreadings >> api_extract_verify_internalreadings
staging_internalreadings >> staging_verify_internalreadings
ref_tables_internalreadings >> ref_tables_verify_internalreadings
