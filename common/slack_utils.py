import sys
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.operators.python_operator import PythonOperator

import requests
import datetime
import json

import boto3

sys.path.append("..")

from common import utils
from common.secrets_manager import get_secret
from conf import config

client = boto3.client("secretsmanager")

SLACK_CONN_ID = "slack"

"""
This relies upon an HTTP connection being created in the Airflow dashboard named slack
Host: https://hooks.slack.com/services
Conn Type: HTTP
Password: /T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX (this was provided by Ed)
"""


def post_slack_message(message, slack_channel):

    print(f"Posting message to slack: \n    {message}")
    env = utils.get_env()

    if env in ["dev", "preprod"]:
        slack_channel = "test-alerts"
        message = env + " : " + message

    return requests.post(
        "https://slack.com/api/chat.postMessage",
        {
            "token": get_secret(client, config.slack_config["secret_id"])["slack_reports_token"],
            "channel": slack_channel,
            "text": message,
        },
    )


def post_slack_sla_alert(dag, task_list, blocking_task_list, slas, blocking_tis):
    sla_td = dag.default_args["sla"]
    dt = datetime.datetime.strptime(str(sla_td), "%H:%M:%S")
    msg = f"""
:warning: The following tasks were/are outstanding inside `{dag.dag_id}` after `{dt.strftime("%Hh %Mm %Ss")}` of runtime:"""
    for sla in slas:
        msg += f"""
------------------------------
*Task*: `{sla.task_id}`
*Execution Date*: `{sla.execution_date}`"""
    post_slack_message(
        msg,
        "prod_ops",
    )


def get_slack_webhook_token():
    return BaseHook.get_connection(SLACK_CONN_ID).password


def alert_slack(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password

    task_instance = context.get("task_instance")

    if hasattr(task_instance.task, "doc_md"):
        task_doc = task_instance.task.doc_md
    elif hasattr(task_instance.task, "doc"):
        task_doc = task_instance.task.doc
    else:
        task_doc = None

    if task_doc is not None:
        slack_msg = """
:red_circle: Task Failed. 
*Task*: {task}  
*Dag*: {dag} 
*Task Instance*: {ti}
*Execution Time*: {exec_date}  
*Log Url*: {log_url}
{task_doc}
""".format(
            task=context.get("task_instance").task_id,
            dag=context.get("task_instance").dag_id,
            ti=context.get("task_instance"),
            exec_date=context.get("execution_date"),
            log_url=context.get("task_instance").log_url,
            task_doc=task_doc,
        )
    else:
        slack_msg = """
:red_circle: Task Failed. 
*Task*: {task}  
*Dag*: {dag} 
*Task Instance*: {ti}
*Execution Time*: {exec_date}  
*Log Url*: {log_url}
""".format(
            task=context.get("task_instance").task_id,
            dag=context.get("task_instance").dag_id,
            ti=context.get("task_instance"),
            exec_date=context.get("execution_date"),
            log_url=context.get("task_instance").log_url,
        )

    failed_alert = SlackWebhookOperator(
        task_id="slack_test",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username="airflow",
    )

    return failed_alert.execute(context=context)
