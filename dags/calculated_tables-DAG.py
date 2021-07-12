from __future__ import print_function

import sys

from airflow.utils.dates import days_ago

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

sys.path.append("/opt/airflow/enzek-meterpoint-readings")

from common.slack_utils import alert_slack, post_slack_message
from common import process_glue_job

from process_eac_aq import start_eac_aq_pa_jobs
from process_eac_aq import start_igloo_ind_eac_aq_jobs
from process_eac_aq import start_consumption_accuracy_jobs
from process_tado import start_tado_efficiency_jobs
from process_EstimatedAdvance import start_est_advance_job
from process_reports import start_reporting_jobs


args = {
    "owner": "Airflow",
    "start_date": days_ago(2),  # don't know what this is doing
    "on_failure_callback": alert_slack,
}

dag = DAG(
    dag_id="igloo_calculated",
    default_args=args,
    schedule_interval=None,
    tags=["cdw"],
    catchup=False,
    max_active_runs=1,
    description="Creates all ref_calculated_<> tables in Redshift.",
)
# Non-report calculated jobs

eacaqpa_job_1 = PythonOperator(
    task_id="eacaqpa_job_1",
    dag=dag,
    python_callable=start_eac_aq_pa_jobs.EacAqPa().submit_eac_aq_gluejob,
)

igloo_indeacaq_job = PythonOperator(
    task_id="igloo_indeacaq_job",
    dag=dag,
    python_callable=start_igloo_ind_eac_aq_jobs.IglIndEacAq().submit_eac_aq_gluejob,
)

consumption_accuracy_job = PythonOperator(
    task_id="consumption_accuracy_job",
    dag=dag,
    python_callable=start_consumption_accuracy_jobs.ConsumptionAccuracy().submit_consumption_accuracy_gluejob,
)

tado_efficiency_job = PythonOperator(
    task_id="tado_efficiency_job",
    dag=dag,
    python_callable=start_tado_efficiency_jobs.TADOEfficiencyJobs().submit_tado_efficiency_batch_gluejob,
)

estimated_advance_job = PythonOperator(
    task_id="estimated_advance_job",
    dag=dag,
    python_callable=start_est_advance_job.EstimatedAdvance().submit_estimated_advance_gluejob,
)

eligibility_job = PythonOperator(
    task_id="eligibility_job",
    dag=dag,
    python_callable=start_reporting_jobs.ReportingJobs().submit_eligibility_reporting_batch_gluejob,
)

# Calculated Report jobs to be triggered after tado efficiency


reports = {
    "daily_reporting_customer_file": {
        "slack_message": "Daily Customer File Updated (ref_calculated_daily_customer_file)"
    },
    "daily_reporting_ds_batch_legacy": {},
    "daily_reporting_igl_tariffs": {},
    "daily_reporting_metering": {"slack_message": "Daily Metering File Updated (ref_calculated_metering_report)"},
    "daily_reporting_sales": {"slack_message": "Daily Sales Report Updated (ref_calculated_sales_report)"},
    "daily_reporting_tariff_comparison": {},
    "daily_reporting_cumulative_ppc": {},
    "daily_reporting_eac_calc_params": {},
    "daily_reporting_cumulative_alp_cv": {},
    "daily_reporting_ref_aq_calc_params": {},
    "daily_reporting_epc_address_linking": {},
    "daily_reporting_epc_certificates": {},
    "daily_reporting_epc_recommendations": {},
    "daily_reporting_meter_port_elec": {
        "slack_message": "Smart Portfolio Elec Report Updated (ref_calculated_metering_portfolio_elec_report)"
    },
    "daily_reporting_meter_port_gas": {
        "slack_message": "Smart Portfolio Gas Report Updated (ref_calculated_metering_portfolio_gas_report)"
    },
    "daily_reporting_smart_missing_invalid_reads": {},
    "daily_reporting_account_debt_status": {},
    "daily_reporting_magnum_patches": {},
}

i = 0
last_operator = ""
for report_name, value in reports.items():
    current_operator = PythonOperator(
        task_id=report_name,
        python_callable=process_glue_job.process_glue_job_await_completion,
        op_args=["_process_ref_tables", report_name],
        dag=dag,
    )
    if i == 0:
        current_operator.set_upstream(tado_efficiency_job)
        last_operator = current_operator
    else:
        new_operator = current_operator
        new_operator.set_upstream(last_operator)
        last_operator = new_operator
    if "slack_message" in value.keys():
        slack_operator = PythonOperator(
            dag=dag,
            task_id="{}{}".format(report_name.replace("daily_reporting_", ""), "_slack_alert"),
            python_callable=post_slack_message,
            op_args=[f"""Task Complete: {value["slack_message"]} :white_check_mark:""", "cdw-daily-reporting"],
        ).set_upstream(current_operator)
    i += 1


eacaqpa_job_1 >> igloo_indeacaq_job >> consumption_accuracy_job >> tado_efficiency_job

last_operator >> estimated_advance_job >> eligibility_job
