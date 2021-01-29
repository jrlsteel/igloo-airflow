import os
import pandas as pd
import requests
import json
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator


def format_bytes(size):
    # 2**10 = 1024
    power = 2 ** 10
    n = 0
    power_labels = ["k", "m", "g", "t"]
    while size > power:
        size /= power
        n += 1
    return str(round(size, 2)) + power_labels[n] + "b"


def send_slack_notification(
    slack_alert_url, monitored_system, usage_percent, used_bytes, remaining_bytes
):
    header = (
        ":floppy_disk: {monitored_system} disk usage at {dsu}% :floppy_disk:".format(
            monitored_system=monitored_system,
            dsu=usage_percent,
        )
    )
    slack_data = {
        "text": header,
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": header,
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "Disk space used: {dsu}. Disk space remaining: {dsr}.".format(
                        dsu=format_bytes(used_bytes), dsr=format_bytes(remaining_bytes)
                    ),
                },
            },
        ],
    }
    response = requests.post(
        slack_alert_url,
        data=json.dumps(slack_data),
        headers={"Content-Type": "application/json"},
    )
    if response.status_code != 200:
        raise ValueError(
            "Request to slack returned an error {status_code}, the response is:\n{response_text}".format(
                status_code=response.status_code,
                response_text=response.text,
            )
        )


def check_disk_usage(monitored_system, disk_usage_alert_percent, slack_alert_url):
    disk_usage = [s.split()[:4] for s in os.popen("df").read().splitlines()]
    df_disk_usage = pd.DataFrame(disk_usage[1:], columns=disk_usage[0])
    # row of interest
    roi = df_disk_usage[df_disk_usage["Filesystem"] == "overlay"]

    capacity_bytes = float(roi["1K-blocks"].values[0])
    used_bytes = float(roi["Used"].values[0])
    remaining_bytes = float(roi["Available"].values[0])

    usage_percent = round(100.0 * used_bytes / capacity_bytes, 2)

    print(
        "{monitored_system} disk usage at {usage_percent}%, alerting threshold set to {disk_usage_alert_percent}".format(
            monitored_system=monitored_system,
            usage_percent=usage_percent,
            disk_usage_alert_percent=float(disk_usage_alert_percent),
        )
    )
    print(
        "Disk space used: {used_bytes}. Disk space remaining: {remaining_bytes}".format(
            used_bytes=used_bytes,
            remaining_bytes=remaining_bytes,
        )
    )

    if usage_percent > float(disk_usage_alert_percent):
        send_slack_notification(
            slack_alert_url,
            "{monitored_system}".format(monitored_system=monitored_system),
            usage_percent,
            used_bytes,
            remaining_bytes,
        )


args = {"owner": "Airflow", "start_date": datetime(2021, 1, 25)}

dag = DAG(
    dag_id="monitoring_disk_usage",
    default_args=args,
    schedule_interval="@hourly",
    tags=["monitoring"],
    catchup=False,
    max_active_runs=1,
)

monitoring_disk_usage_task = PythonOperator(
    task_id="check_disk_usage",
    python_callable=check_disk_usage,
    op_kwargs={
        "monitored_system": "{{ var.value.ENVIRONMENT }} Airflow",
        "disk_usage_alert_percent": "{{ var.value.DISK_USAGE_ALERT_PERCENT }}",
        "slack_alert_url": "{{ var.value.DISK_USAGE_ALERT_SLACK_URL  }}",
    },
    dag=dag,
)
