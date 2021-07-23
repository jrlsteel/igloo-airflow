from __future__ import print_function

import sys

from airflow.utils.dates import days_ago

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

sys.path.append("/opt/airflow/enzek-meterpoint-readings")

from common.slack_utils import alert_slack, post_slack_message
from common import process_glue_job


args = {
    "owner": "Airflow",
    "start_date": days_ago(2),  # don't know what this is doing
    "on_failure_callback": alert_slack,
}

dag = DAG(
    dag_id="igloo_calculated",
    default_args=args,
    schedule_interval=None,
    tags=["cdw", "reporting"],
    catchup=False,
    max_active_runs=1,
    description="Creates all ref_calculated_<> tables in Redshift.",
)

dependencies = {
    "daily_reporting_customer_file": {
        "calculated_dependencies": ["igl_ind_eac_aq", "cons_accu"],
        "slack_message": "Daily Customer File Updated (ref_calculated_daily_customer_file)",
    },
    "daily_reporting_ds_batch_legacy": {"calculated_dependencies": ["daily_reporting_customer_file"]},
    "daily_reporting_igl_tariffs": {"calculated_dependencies": ["daily_reporting_customer_file"]},
    "daily_reporting_metering": {
        "calculated_dependencies": [],
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
    "daily_reporting_cumulative_ppc": {"calculated_dependencies": []},
    "daily_reporting_eac_calc_params": {"calculated_dependencies": ["igl_ind_eac_aq"]},
    "daily_reporting_cumulative_alp_cv": {"calculated_dependencies": []},
    "daily_reporting_ref_aq_calc_params": {"calculated_dependencies": ["igl_ind_eac_aq"]},
    "daily_reporting_epc_address_linking": {"calculated_dependencies": []},
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
    "daily_reporting_smart_missing_invalid_reads": {"calculated_dependencies": []},
    "daily_reporting_account_debt_status": {"calculated_dependencies": []},
    "daily_reporting_magnum_patches": {"calculated_dependencies": []},
    "eac_aq": {"calculated_dependencies": []},
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
}

# Create all processing tasks
tasks = {}
for report_name, report_info in dependencies.items():
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
