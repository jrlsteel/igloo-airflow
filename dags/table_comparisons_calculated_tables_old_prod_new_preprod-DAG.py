from __future__ import print_function

import time
from builtins import range
from pprint import pprint

from airflow.utils.dates import days_ago

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import PythonVirtualenvOperator
from airflow.operators.bash_operator import BashOperator


args = {
    'owner': 'Airflow',
    'start_date': days_ago(2),
}

dag = DAG(
    dag_id='table_comparisons_calculated_tables_old_prod_new_preprod',
    default_args=args,
    schedule_interval='00 09 * * *',
    tags=['cdw'],
    # We only want to run one at a time to conserve resource, but
    # we want all to run, even if any fail.
    concurrency=1
)

ref_calculated_aq = BashOperator(
    task_id='ref_calculated_aq',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_table_comparisons && python compare_tables.py --table-comparison-config old_prod_new_preprod_ref_calculated_aq --output-to-s3',
    dag=dag,
)

ref_calculated_daily_customer_file = BashOperator(
    task_id='ref_calculated_daily_customer_file',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_table_comparisons && python compare_tables.py --table-comparison-config old_prod_new_preprod_ref_calculated_daily_customer_file --output-to-s3',
    dag=dag,
)

ref_calculated_daily_sales_batch = BashOperator(
    task_id='ref_calculated_daily_sales_batch',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_table_comparisons && python compare_tables.py --table-comparison-config old_prod_new_preprod_ref_calculated_daily_sales_batch --output-to-s3',
    dag=dag,
)

ref_calculated_eac = BashOperator(
    task_id='ref_calculated_eac',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_table_comparisons && python compare_tables.py --table-comparison-config old_prod_new_preprod_ref_calculated_eac --output-to-s3',
    dag=dag,
)

ref_calculated_igl_ens_tariff_comparison = BashOperator(
    task_id='ref_calculated_igl_ens_tariff_comparison',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_table_comparisons && python compare_tables.py --table-comparison-config old_prod_new_preprod_ref_calculated_igl_ens_tariff_comparison --output-to-s3',
    dag=dag,
)

ref_calculated_igl_ind_aq = BashOperator(
    task_id='ref_calculated_igl_ind_aq',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_table_comparisons && python compare_tables.py --table-comparison-config old_prod_new_preprod_ref_calculated_igl_ind_aq --output-to-s3',
    dag=dag,
)

ref_calculated_igl_ind_eac = BashOperator(
    task_id='ref_calculated_igl_ind_eac',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_table_comparisons && python compare_tables.py --table-comparison-config old_prod_new_preprod_ref_calculated_igl_ind_eac --output-to-s3',
    dag=dag,
)

ref_calculated_metering_portfolio_elec_report = BashOperator(
    task_id='ref_calculated_metering_portfolio_elec_report',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_table_comparisons && python compare_tables.py --table-comparison-config old_prod_new_preprod_ref_calculated_metering_portfolio_elec_report --output-to-s3',
    dag=dag,
)

ref_calculated_metering_portfolio_gas_report = BashOperator(
    task_id='ref_calculated_metering_portfolio_gas_report',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_table_comparisons && python compare_tables.py --table-comparison-config old_prod_new_preprod_ref_calculated_metering_portfolio_gas_report --output-to-s3',
    dag=dag,
)

ref_calculated_metering_report = BashOperator(
    task_id='ref_calculated_metering_report',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_table_comparisons && python compare_tables.py --table-comparison-config old_prod_new_preprod_ref_calculated_metering_report --output-to-s3',
    dag=dag,
)

ref_calculated_sales_report = BashOperator(
    task_id='ref_calculated_sales_report',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_table_comparisons && python compare_tables.py --table-comparison-config old_prod_new_preprod_ref_calculated_sales_report --output-to-s3',
    dag=dag,
)

ref_calculated_tado_efficiency_average = BashOperator(
    task_id='ref_calculated_tado_efficiency_average',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_table_comparisons && python compare_tables.py --table-comparison-config old_prod_new_preprod_ref_calculated_tado_efficiency_average --output-to-s3',
    dag=dag,
)

ref_calculated_tado_efficiency_batch = BashOperator(
    task_id='ref_calculated_tado_efficiency_batch',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_table_comparisons && python compare_tables.py --table-comparison-config old_prod_new_preprod_ref_calculated_tado_efficiency_batch --output-to-s3',
    dag=dag,
)

ref_calculated_tado_fuel = BashOperator(
    task_id='ref_calculated_tado_fuel',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_table_comparisons && python compare_tables.py --table-comparison-config old_prod_new_preprod_ref_calculated_tado_fuel --output-to-s3',
    dag=dag,
)

ref_calculated_tariff_accounts = BashOperator(
    task_id='ref_calculated_tariff_accounts',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_table_comparisons && python compare_tables.py --table-comparison-config old_prod_new_preprod_ref_calculated_tariff_accounts --output-to-s3',
    dag=dag,
)

ref_smart_readings = BashOperator(
    task_id='ref_smart_readings',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_table_comparisons && python compare_tables.py --table-comparison-config ref_smart_readings --output-to-s3',
    dag=dag,
)

ref_smart_inventory = BashOperator(
    task_id='ref_smart_inventory',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_table_comparisons && python compare_tables.py --table-comparison-config ref_smart_inventory --output-to-s3',
    dag=dag,
)

ref_calculated_aq
ref_calculated_daily_customer_file
ref_calculated_daily_sales_batch
ref_calculated_eac
ref_calculated_igl_ens_tariff_comparison
ref_calculated_igl_ind_aq
ref_calculated_igl_ind_eac
ref_calculated_metering_portfolio_elec_report
ref_calculated_metering_portfolio_gas_report
ref_calculated_metering_report
ref_calculated_sales_report
ref_calculated_tado_efficiency_average
ref_calculated_tado_efficiency_batch
ref_calculated_tado_fuel
ref_calculated_tariff_accounts
ref_smart_readings
ref_smart_inventory
