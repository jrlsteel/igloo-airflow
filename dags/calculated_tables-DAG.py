from __future__ import print_function

import sys

from airflow.utils.dates import days_ago

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor


import datetime
from cdw.conf import config
from cdw.common import utils as util
from cdw.common.slack_utils import alert_slack, post_slack_message, post_slack_sla_alert
from cdw.common import process_glue_job, schedules

dag_id = "igloo_calculated"
env = config.environment_config["environment"]


def get_schedule():
    return schedules.get_schedule(env, dag_id)


args = {
    "owner": "Airflow",
    "start_date": days_ago(2),  # don't know what this is doing
    "on_failure_callback": alert_slack,
    "sla": util.get_sla_timedelta(dag_id),
}

dag = DAG(
    dag_id=dag_id,
    default_args=args,
    schedule_interval=get_schedule(),
    sla_miss_callback=post_slack_sla_alert,
    tags=["cdw", "reporting"],
    catchup=False,
    max_active_runs=1,
    description="Creates all ref_calculated_<> tables in Redshift.",
)

ref_ensek_transactions_finish = ExternalTaskSensor(
    task_id="ref_ensek_transactions_finish",
    external_dag_id="ensek_am",
    external_task_id="ref_ensek_transactions",
    start_date=days_ago(2),
)

dependencies = {
    "daily_reporting_customer_file": {
        "calculated_dependencies": ["igl_ind_eac_aq", "cons_accu"],
        "slack_message": "Daily Customer File Updated (ref_calculated_daily_customer_file)",
    },
    "daily_reporting_ds_batch_legacy": {"calculated_dependencies": ["daily_reporting_customer_file"]},
    "daily_reporting_igl_tariffs": {"calculated_dependencies": ["daily_reporting_customer_file"]},
    "daily_reporting_metering": {
        "calculated_dependencies": ["ref_ensek_transactions_finish"],
        "slack_message": "Daily Metering File Updated (ref_calculated_metering_report)",
    },
    "daily_reporting_sales": {
        "calculated_dependencies": ["cons_accu"],
        "slack_message": "Daily Sales Report Updated (ref_calculated_sales_report)",
    },
    "daily_reporting_tariff_comparison": {
        "calculated_dependencies": [
            "daily_reporting_customer_file",
            "daily_reporting_igl_tariffs",
        ]
    },
    "daily_reporting_cumulative_ppc": {"calculated_dependencies": ["ref_ensek_transactions_finish"]},
    "daily_reporting_eac_calc_params": {"calculated_dependencies": ["igl_ind_eac_aq"]},
    "daily_reporting_cumulative_alp_cv": {"calculated_dependencies": ["ref_ensek_transactions_finish"]},
    "daily_reporting_ref_aq_calc_params": {"calculated_dependencies": ["igl_ind_eac_aq"]},
    "daily_reporting_epc_address_linking": {"calculated_dependencies": ["ref_ensek_transactions_finish"]},
    "daily_reporting_epc_certificates": {"calculated_dependencies": ["daily_reporting_epc_address_linking"]},
    "daily_reporting_epc_recommendations": {"calculated_dependencies": ["daily_reporting_epc_certificates"]},
    "daily_reporting_meter_port_elec": {
        "calculated_dependencies": ["daily_reporting_customer_file"],
        "slack_message": "Smart Portfolio Elec Report Updated (ref_calculated_metering_portfolio_elec_report)",
    },
    "daily_reporting_meter_port_gas": {
        "calculated_dependencies": ["daily_reporting_customer_file"],
        "slack_message": "Smart Portfolio Gas Report Updated (ref_calculated_metering_portfolio_gas_report)",
    },
    "daily_reporting_smart_missing_invalid_reads": {"calculated_dependencies": ["ref_ensek_transactions_finish"]},
    "daily_reporting_account_debt_status": {"calculated_dependencies": ["ref_ensek_transactions_finish"]},
    "daily_reporting_magnum_patches": {"calculated_dependencies": ["ref_ensek_transactions_finish"]},
    "eac_aq": {"calculated_dependencies": ["ref_ensek_transactions_finish"]},
    "igl_ind_eac_aq": {"calculated_dependencies": ["eac_aq"]},
    "cons_accu": {"calculated_dependencies": ["eac_aq", "igl_ind_eac_aq"]},
    "tado_efficiency": {"calculated_dependencies": ["cons_accu"]},
    "est_adv": {
        "calculated_dependencies": [
            "daily_reporting_cumulative_ppc",
            "eac_aq",
            "igl_ind_eac_aq",
        ]
    },
    "eligibility_reporting": {"calculated_dependencies": ["daily_reporting_customer_file"]},
    "mv_reports_ds_consumption_accuracy_register": {
        "calculated_dependencies": ["eac_aq", "igl_ind_eac_aq"],
        "sql_expression": """REFRESH MATERIALIZED VIEW mv_reports_ds_consumption_accuracy_register""",
    },
}

# Create all processing tasks
tasks = {"ref_ensek_transactions_finish": ref_ensek_transactions_finish}
for report_name, report_info in dependencies.items():
    if "sql_expression" in report_info.keys():
        tasks[report_name] = PythonOperator(
            dag=dag,
            task_id=f"{report_name}_sql_expression",
            python_callable=util.execute_redshift_sql_query,
            op_args=[report_info["sql_expression"]],
        )
    else:
        tasks[report_name] = PythonOperator(
            task_id=report_name,
            python_callable=process_glue_job.run_glue_job_await_completion,
            op_args=["_process_ref_tables", report_name],
            dag=dag,
        )

# Create slack message tasks and dependencies
for report_name, report_info in dependencies.items():
    if "slack_message" in report_info.keys():
        slack_operator = PythonOperator(
            dag=dag,
            task_id="{}{}".format(report_name.replace("daily_reporting_", ""), "_slack_alert"),
            python_callable=post_slack_message,
            op_args=[f"""Task Complete: {report_info["slack_message"]} :white_check_mark:""", "cdw-daily-reporting"],
        ).set_upstream(tasks[report_name])
    for dependency in report_info["calculated_dependencies"]:
        tasks[report_name].set_upstream(tasks[dependency])
