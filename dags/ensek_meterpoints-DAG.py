import sys

sys.path.append("/opt/airflow/enzek-meterpoint-readings")

import datetime
from airflow.utils.dates import days_ago
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import sentry_sdk

import common.utils as util
from common.process_glue_job import ProcessGlueJob as glue_job
from common.slack_utils import alert_slack
from process_verification.verification_template import (
    verify_new_api_response_files_in_s3_directory,
    verify_seventeen_new_files_in_s3,
    ref_verification_step,
)


args = {
    "owner": "Airflow",
    "start_date": days_ago(2),
    "on_failure_callback": alert_slack,
}


dag = DAG(
    dag_id="ensek_meterpoints",
    default_args=args,
    schedule_interval=None,
    tags=["cdw", "example", "ensek", "meterpoints"],
    catchup=False,
    description="Extracts Meterpoint information for each account ID from the following endpoint: https://api.igloo.ignition.ensek.co.uk/Accounts/<account_id>/MeterPoints?includeHistory=true",
)


directory = util.get_dir()
s3_key = directory["s3_key"]
s3_bucket = directory["s3_bucket"]
environment = util.get_env()
staging_job_name = directory["glue_staging_meterpoints_job_name"]
ref_job_name = directory["glue_ref_meterpoints_job_name"]
staging_process_name = "meterpoints"
ref_process_name = "ensek_meterpoints"

date_today_string = str(datetime.date.today())


def process_glue_job(job_name, process_name):
    # job_id = util.get_jobID()
    try:
        print("job_name-- ", job_name)
        print("s3_bucket-- ", s3_bucket)
        print("environment-- ", environment)
        print("process_name-- ", process_name)
        # util.batch_logging_insert(job_id, 9, 'ensek_pa_staging_gluejob', 'start_ensek_api_pa_jobs.py')
        obj_stage = glue_job(
            job_name=job_name,
            s3_bucket=s3_bucket,
            environment=environment,
            processJob=process_name,
        )
        job_response = obj_stage.run_glue_job()
        if job_response:
            print("{0}: Staging Job Completed successfully".format(datetime.datetime.now().strftime("%H:%M:%S")))
            # util.batch_logging_update(job_id, 'e')
            # return staging_job_response
        else:
            print("Error occurred in Staging Job")
            # return staging_job_response
            raise Exception
    except Exception as e:
        print("Error in Staging Job :- " + str(e))
        # util.batch_logging_update(job_id, 'f', str(e))
        sys.exit(1)


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


def dummy_python_task():
    print("I am a dummy python operator for airflow.")


# DAG Tasks

api_extract_meterpoints = BashOperator(
    task_id="api_extract_meterpoints_bash",
    bash_command="cd /opt/airflow/enzek-meterpoint-readings/process_Ensek/processEnsekMeterpoints && python process_ensek_meterpoints_no_history.py",
    dag=dag,
)
api_extract_meterpoints.doc_md = """# Purpose
    
    Extracts meterpoint information from account Ensek for each Account ID
    
    #Suggested action on failure
    
    No action
    
    # Justification
    
    This step has a long run time, and rerunning it would likely impact other batch processes and Ensek response times.
    """


api_extract_verify_meterpoints = PythonOperator(
    task_id="api_extract_verify_meterpoints",
    python_callable=verify_new_api_response_files_in_s3_directory,
    op_kwargs={
        "search_filter": date_today_string,
        "expected_value": "{{ var.value.LIVE_ACCOUNTS_FOR_VERIFY }}",
        "percent": "{{ var.value.PERCENT_FOR_API_VERIFY }}",
        "s3_prefix": s3_key["MeterPoints"],
    },
    dag=dag,
)
api_extract_verify_meterpoints.doc_md = """# Purpose
    Verification: There are sufficient new files in the s3 directory: {}, demonstrating the success of this API extracts

    # Suggest action on failure

    No action 

    # Justification

    As the step simply counts files in a directory there will be no change given it runs after extractions. 
    """.format(
    s3_key["MeterPoints"]
)


api_extract_verify_meterpoints_attributes = PythonOperator(
    task_id="api_extract_verify_meterpoints_attributes",
    python_callable=verify_new_api_response_files_in_s3_directory,
    op_kwargs={
        "search_filter": date_today_string,
        "expected_value": "{{ var.value.LIVE_ACCOUNTS_FOR_VERIFY }}",
        "percent": "{{ var.value.PERCENT_FOR_API_VERIFY }}",
        "s3_prefix": s3_key["MeterPointsAttributes"],
    },
    dag=dag,
)
api_extract_verify_meterpoints.doc_md = """# Purpose
    Verification: There are sufficient new files in the s3 directory: {}, demonstrating the success of this API extracts

    # Suggest action on failure

    No action 

    # Justification

    As the step simply counts files in a directory there will be no change given it runs after extractions. 
    """.format(
    s3_key["MeterPointsAttributes"]
)


api_extract_verify_meters = PythonOperator(
    task_id="api_extract_verify_meters",
    python_callable=verify_new_api_response_files_in_s3_directory,
    op_kwargs={
        "search_filter": date_today_string,
        "expected_value": "{{ var.value.LIVE_ACCOUNTS_FOR_VERIFY }}",
        "percent": "{{ var.value.PERCENT_FOR_API_VERIFY }}",
        "s3_prefix": s3_key["Meters"],
    },
    dag=dag,
)
api_extract_verify_meters.doc_md = """# Purpose
    Verification: There are sufficient new files in the s3 directory: {}, demonstrating the success of this API extracts

    # Suggest action on failure

    No action 

    # Justification

    As the step simply counts files in a directory there will be no change given it runs after extractions. 
    """.format(
    s3_key["Meters"]
)


api_extract_verify_meters_attributes = PythonOperator(
    task_id="api_extract_verify_meters_attributes",
    python_callable=verify_new_api_response_files_in_s3_directory,
    op_kwargs={
        "search_filter": date_today_string,
        "expected_value": "{{ var.value.LIVE_ACCOUNTS_FOR_VERIFY }}",
        "percent": "{{ var.value.PERCENT_FOR_API_VERIFY }}",
        "s3_prefix": s3_key["MetersAttributes"],
    },
    dag=dag,
)
api_extract_verify_meters_attributes.doc_md = """# Purpose
    Verification: There are sufficient new files in the s3 directory: {}, demonstrating the success of this API extracts

    # Suggest action on failure

    No action 

    # Justification

    As the step simply counts files in a directory there will be no change given it runs after extractions. 
    """.format(
    s3_key["MetersAttributes"]
)


api_extract_verify_registers = PythonOperator(
    task_id="api_extract_verify_registers",
    python_callable=verify_new_api_response_files_in_s3_directory,
    op_kwargs={
        "search_filter": date_today_string,
        "expected_value": "{{ var.value.LIVE_ACCOUNTS_FOR_VERIFY }}",
        "percent": "{{ var.value.PERCENT_FOR_API_VERIFY }}",
        "s3_prefix": s3_key["Registers"],
    },
    dag=dag,
)
api_extract_verify_registers.doc_md = """# Purpose
    Verification: There are sufficient new files in the s3 directory: {}, demonstrating the success of this API extracts

    # Suggest action on failure

    No action 

    # Justification

    As the step simply counts files in a directory there will be no change given it runs after extractions. 
    """.format(
    s3_key["Registers"]
)


api_extract_verify_register_attributes = PythonOperator(
    task_id="api_extract_verify_register_attributes",
    python_callable=verify_new_api_response_files_in_s3_directory,
    op_kwargs={
        "search_filter": date_today_string,
        "expected_value": "{{ var.value.LIVE_ACCOUNTS_FOR_VERIFY }}",
        "percent": "{{ var.value.PERCENT_FOR_API_VERIFY }}",
        "s3_prefix": s3_key["RegistersAttributes"],
    },
    dag=dag,
)
api_extract_verify_register_attributes.doc_md = """# Purpose
    Verification: There are sufficient new files in the s3 directory: {}, demonstrating the success of this API extracts

    # Suggest action on failure

    No action 

    # Justification

    As the step simply counts files in a directory there will be no change given it runs after extractions. 
    """.format(
    s3_key["RegistersAttributes"]
)


staging_meterpoints = PythonOperator(
    task_id="staging_meterpoints",
    python_callable=process_glue_job,
    op_args=[staging_job_name, staging_process_name],
    dag=dag,
)
staging_meterpoints.doc_md = """# Purpose
    Runs glue job on the stage 1 meterpoints files which reduces them to 17 parquet files and writes them to stage 2

    # Suggest action on failure

    No action 

    # Justification

    Given this task is long running, rerunning it would impact other batch processes significantly.
    """


staging_verify_meterpoints = PythonOperator(
    task_id="staging_verify_meterpoints",
    python_callable=verify_seventeen_new_files_in_s3,
    op_kwargs={"s3_prefix": "stage2/stage2_meterpoints"},
    dag=dag,
)
staging_verify_meterpoints.doc_md = """# Purpose
    Verification: There are sufficient new files in the staging directory.

    # Suggest action on failure

    No action 

    # Justification

    As the step simply counts files in a directory there will be no change given it runs after extractions. 
    """


ref_tables_meterpoints = PythonOperator(
    task_id="ref_tables_meterpoints",
    python_callable=process_glue_job,
    op_args=[ref_job_name, ref_process_name],
    dag=dag,
)
ref_tables_meterpoints.doc_md = """# Purpose
    This step takes stage 2 data and inserts it into relavent redshift reference tables.

    # Suggest action on failure

    No action 

    # Justification

    Given this task is long running, rerunning it would impact other batch processes significantly.
    """


ref_tables_verify_meterpoints = PythonOperator(
    task_id="ref_tables_verify_meterpoints",
    python_callable=ref_verification_step,
    op_kwargs={
        "ref_meterpoints": "ref_meterpoints",
        "ref_meterpoints_attributes": "ref_meterpoints_attributes",
        "ref_meters": "ref_meters",
        "ref_meters_attributes": "ref_meters_attributes",
        "ref_registers": "ref_registers",
        "ref_registers_attributes": "ref_registers_attributes",
    },
    dag=dag,
)
ref_tables_verify_meterpoints.doc_md = """# Purpose
    Verification: There are sufficient new files in the staging directory.

    # Suggest action on failure

    No action 

    # Justification

    As the step simply counts files in a directory there will be no change given it runs after extractions. 
    """

# Dependencies

api_extract_meterpoints >> staging_meterpoints >> ref_tables_meterpoints
api_extract_meterpoints >> api_extract_verify_meterpoints
api_extract_meterpoints >> api_extract_verify_meterpoints_attributes
api_extract_meterpoints >> api_extract_verify_meters
api_extract_meterpoints >> api_extract_verify_meters_attributes
api_extract_meterpoints >> api_extract_verify_registers
api_extract_meterpoints >> api_extract_verify_register_attributes
staging_meterpoints >> staging_verify_meterpoints
ref_tables_meterpoints >> ref_tables_verify_meterpoints
