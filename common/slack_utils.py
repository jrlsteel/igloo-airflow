from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

SLACK_CONN_ID = "slack"

"""
This relies upon an HTTP connection being created in the Airflow dashboard named slack
Host: https://hooks.slack.com/services
Conn Type: HTTP
Password: /T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX (this was provided by Ed)
"""


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
