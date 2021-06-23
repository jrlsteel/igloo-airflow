import gocardless_pro
import pandas as pd
import json
import csv
from multiprocessing import freeze_support
from datetime import datetime
import math

import sys

sys.path.append("..")

from common import utils as util
from common.utils import IglooLogger
from conf import config as con
from connections.connect_db import get_finance_s3_connections as s3_con


class GoCardlessEventProcessor:
    def __init__(self):
        self.last_run_sql = "select max(created_at) as lastRun from aws_fin_stage1_extracts.fin_go_cardless_api_events "

        dir_root = util.get_dir()
        self.bucket_name = dir_root["s3_finance_bucket"]
        self.s3_conn = s3_con(self.bucket_name)
        dir_gc = dir_root["s3_finance_goCardless_key"]
        self.events_directory = dir_gc["Events"]
        self.mandates_directory = dir_gc["Mandates-Files"]
        self.subscriptions_directory = dir_gc["Subscriptions-Files"]
        self.payments_directory = dir_gc["Payments-Files"]
        self.refunds_directory = dir_gc["Refunds-Files"]

        gc_api_client = gocardless_pro.Client(
            access_token=con.go_cardless["access_token"], environment=con.go_cardless["environment"]
        )
        self.events_api = gc_api_client.events
        self.mandates_api = gc_api_client.mandates
        self.subscriptions_api = gc_api_client.subscriptions
        self.payments_api = gc_api_client.payments
        self.refunds_api = gc_api_client.refunds

    def process_events(self):
        iglog = IglooLogger(source="process_events method")
        start_date = str(util.execute_query(self.last_run_sql).iat[0, 0])
        end_date = str(datetime.now().replace(microsecond=0).isoformat()) + str(".000Z")

        iglog.in_prod_env("Listing Events between {start} and {end}".format(start=start_date, end=end_date))
        event_datalist = []
        try:
            for event in self.events_api.all(params={"created_at[gt]": start_date, "created_at[lte]": end_date}):
                iglog.in_test_env(event.id)
                list_row = [
                    event.id,
                    event.created_at,
                    event.resource_type,
                    event.action,
                    event.customer_notifications,
                    event.details.cause,
                    event.details.description,
                    event.details.origin,
                    event.details.reason_code,
                    event.details.scheme,
                    event.details.will_attempt_retry,
                    event.links.mandate,
                    event.links.new_customer_bank_account,
                    event.links.new_mandate,
                    event.links.organisation,
                    event.links.parent_event,
                    event.links.payment,
                    event.links.payout,
                    event.links.previous_customer_bank_account,
                    event.links.refund,
                    event.links.subscription,
                ]
                event_datalist.append(list_row)

        except (
            json.decoder.JSONDecodeError,
            gocardless_pro.errors.GoCardlessInternalError,
            gocardless_pro.errors.MalformedResponseError,
            gocardless_pro.errors.InvalidApiUsageError,
            AttributeError,
        ) as e:
            print(sys.exc_info())

            iglog.in_prod_env(
                "Failed to update events between {start} and {end}".format(start=start_date, end=end_date)
            )

        iglog.in_test_env("converting to dataframe")
        df_event = pd.DataFrame(
            event_datalist,
            columns=[
                "id",
                "created_at",
                "resource_type",
                "action",
                "customer_notifications",
                "cause",
                "description",
                "origin",
                "reason_code",
                "scheme",
                "will_attempt_retry",
                "mandate",
                "new_customer_bank_account",
                "new_mandate",
                "organisation",
                "parent_event",
                "payment",
                "payout",
                "previous_customer_bank_account",
                "refund",
                "subscription",
            ],
        )

        # write events retrieved during this execution out to S3 in a single file
        iglog.in_test_env("Writing to S3")
        column_list = util.get_common_info("go_cardless_column_order", "events")
        df_string = df_event.to_csv(index=False, columns=column_list, quoting=csv.QUOTE_NONNUMERIC)
        s3_conn = self.s3_conn
        file_name = "go_cardless_events_{start}_{end}.csv".format(start=start_date, end=end_date)
        folder = "timestamp={year}-Q{quarter}/".format(
            year=math.ceil(datetime.today().year), quarter=math.ceil(datetime.today().month / 3.0)
        )
        s3_conn.key = self.events_directory + folder + file_name
        s3_conn.set_contents_from_string(df_string)
        iglog.in_test_env("Events table updated")

    def process_mandates(self, mandate_ids, thread_name=None):
        iglog = IglooLogger(source=thread_name)
        iglog.in_prod_env("Updating {num_ids} mandates".format(num_ids=len(mandate_ids)))
        s3_conn = s3_con(self.bucket_name)
        key_directory = self.mandates_directory
        api = self.mandates_api
        for m_id in mandate_ids:
            try:
                iglog.in_test_env(m_id)
                mandate = api.get(m_id)
                mandate_row = [
                    mandate.id,
                    mandate.links.customer,
                    mandate.links.new_mandate,
                    mandate.created_at,
                    mandate.next_possible_charge_date,
                    mandate.payments_require_approval,
                    mandate.reference,
                    mandate.scheme,
                    mandate.status,
                    mandate.links.creditor,
                    mandate.links.customer_bank_account,
                    mandate.metadata.get("AccountId", None),
                    mandate.metadata.get("StatementId", None),
                    datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                ]
                header = [
                    "mandate_id",
                    "CustomerId",
                    "new_mandate_id",
                    "created_at",
                    "next_possible_charge_date",
                    "payments_require_approval",
                    "reference",
                    "scheme",
                    "status",
                    "creditor",
                    "customer_bank_account",
                    "EnsekID",
                    "EnsekStatementId",
                    "extract_timestamp",
                ]
                csv_string = util.list_to_csv_string([header, mandate_row])
                s3_conn.key = "{directory}go_cardless_mandates_{entity_id}.csv".format(
                    directory=key_directory, entity_id=mandate.id
                )
                s3_conn.set_contents_from_string(csv_string)
                iglog.in_test_env("Written to {0}".format(s3_conn.key))

            except (
                json.decoder.JSONDecodeError,
                gocardless_pro.errors.GoCardlessInternalError,
                gocardless_pro.errors.MalformedResponseError,
                gocardless_pro.errors.InvalidApiUsageError,
                AttributeError,
            ) as e:
                iglog.in_prod_env("error processing mandate with id {entity_id}: {err}".format(entity_id=m_id, err=e))

    def process_subscriptions(self, sub_id_list, thread_name=None):
        iglog = IglooLogger(source=thread_name)
        iglog.in_prod_env("Updating {num_ids} subscriptions".format(num_ids=len(sub_id_list)))
        s3_conn = s3_con(self.bucket_name)
        key_directory = self.subscriptions_directory
        api = self.subscriptions_api

        for sub_id in sub_id_list:
            try:
                iglog.in_test_env(sub_id)
                subscription = api.get(sub_id)
                upcoming_payments = subscription.upcoming_payments
                if len(upcoming_payments) > 0:
                    charge_date = upcoming_payments[0]["charge_date"]
                    amount_subscription = upcoming_payments[0]["amount"]
                else:
                    charge_date = None
                    amount_subscription = None

                subscription_row = [
                    subscription.id,
                    subscription.created_at,
                    subscription.amount,
                    subscription.currency,
                    subscription.status,
                    subscription.name,
                    subscription.start_date,
                    subscription.end_date,
                    subscription.interval,
                    subscription.interval_unit,
                    subscription.day_of_month,
                    subscription.month,
                    subscription.count,
                    subscription.payment_reference,
                    subscription.app_fee,
                    subscription.retry_if_possible,
                    subscription.links.mandate,
                    charge_date,
                    amount_subscription,
                    datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                ]
                header = [
                    "id",
                    "created_at",
                    "amount",
                    "currency",
                    "status",
                    "name",
                    "start_date",
                    "end_date",
                    "interval",
                    "interval_unit",
                    "day_of_month",
                    "month",
                    "count_no",
                    "payment_reference",
                    "app_fee",
                    "retry_if_possible",
                    "mandate",
                    "charge_date",
                    "amount_subscription",
                    "extract_timestamp",
                ]
                csv_string = util.list_to_csv_string([header, subscription_row])
                s3_conn.key = "{directory}go_cardless_subscriptions_{entity_id}.csv".format(
                    directory=key_directory, entity_id=subscription.id
                )
                s3_conn.set_contents_from_string(csv_string)
                iglog.in_test_env("Written to {0}".format(s3_conn.key))

            except (
                json.decoder.JSONDecodeError,
                gocardless_pro.errors.GoCardlessInternalError,
                gocardless_pro.errors.MalformedResponseError,
                gocardless_pro.errors.InvalidApiUsageError,
                AttributeError,
            ):
                iglog.in_prod_env("error processing subscription with id {entity_id}".format(entity_id=sub_id))

    def process_payments(self, payment_id_list, thread_name=None):
        iglog = IglooLogger(source=thread_name)
        iglog.in_prod_env("Updating {num_ids} payments".format(num_ids=len(payment_id_list)))
        s3_conn = s3_con(self.bucket_name)
        key_directory = self.payments_directory
        api = self.payments_api
        for pay_id in payment_id_list:
            try:
                iglog.in_test_env(pay_id)
                payment = api.get(pay_id)

                if payment.metadata:
                    ensek_account_id = payment.metadata.get("AccountId", None)
                    statement_id = payment.metadata.get("StatementId", None)
                else:
                    ensek_account_id = None
                    statement_id = None

                payment_row = [
                    payment.id,
                    payment.amount,
                    payment.amount_refunded,
                    payment.charge_date,
                    payment.created_at,
                    payment.currency,
                    payment.description,
                    payment.reference,
                    payment.status,
                    payment.links.payout,
                    payment.links.mandate,
                    payment.links.subscription,
                    ensek_account_id,
                    statement_id,
                    datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                ]
                header = [
                    "id",
                    "amount",
                    "amount_refunded",
                    "charge_date",
                    "created_at",
                    "currency",
                    "description",
                    "reference",
                    "status",
                    "payout",
                    "mandate",
                    "subscription",
                    "EnsekID",
                    "StatementId",
                    "extract_timestamp",
                ]
                csv_string = util.list_to_csv_string([header, payment_row])
                s3_conn.key = "{directory}go_cardless_Payments_{entity_id}.csv".format(
                    directory=key_directory, entity_id=payment.id
                )
                s3_conn.set_contents_from_string(csv_string)
                iglog.in_test_env("Written to {0}".format(s3_conn.key))

            except (
                json.decoder.JSONDecodeError,
                gocardless_pro.errors.GoCardlessInternalError,
                gocardless_pro.errors.MalformedResponseError,
                gocardless_pro.errors.InvalidApiUsageError,
                AttributeError,
            ):
                iglog.in_prod_env("error processing payment with id {entity_id}".format(entity_id=pay_id))

    def process_refunds(self, refund_id_list, thread_name=None):
        iglog = IglooLogger(source=thread_name)
        iglog.in_prod_env("Updating {num_ids} refunds".format(num_ids=len(refund_id_list)))
        s3_conn = s3_con(self.bucket_name)
        key_directory = self.refunds_directory
        api = self.refunds_api

        for ref_id in refund_id_list:
            try:
                iglog.in_test_env(ref_id)
                refund = api.get(ref_id)

                refund_row = [
                    refund.metadata.get("AccountId", None),
                    refund.amount,
                    refund.created_at,
                    refund.currency,
                    refund.id,
                    refund.links.mandate,
                    refund.metadata,
                    refund.links.payment,
                    refund.reference,
                    refund.status,
                    datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                ]
                header = [
                    "EnsekID",
                    "amount",
                    "created_at",
                    "currency",
                    "id",
                    "mandate",
                    "metadata",
                    "payment",
                    "reference",
                    "status",
                    "extract_timestamp",
                ]
                csv_string = util.list_to_csv_string([header, refund_row])
                s3_conn.key = "{directory}go_cardless_Refunds_{entity_id}.csv".format(
                    directory=key_directory, entity_id=refund.id
                )
                s3_conn.set_contents_from_string(csv_string)
                iglog.in_test_env("Written to {0}".format(s3_conn.key))

            except (
                json.decoder.JSONDecodeError,
                gocardless_pro.errors.GoCardlessInternalError,
                gocardless_pro.errors.MalformedResponseError,
                gocardless_pro.errors.InvalidApiUsageError,
                AttributeError,
            ):
                iglog.in_prod_env("error processing refund with id {entity_id}".format(entity_id=ref_id))


if __name__ == "__main__":
    freeze_support()

    iglog = IglooLogger(source="GC Events Main Method")

    gc_processor = GoCardlessEventProcessor()

    # EVENTS
    master_source = util.get_master_source("go_cardless")
    current_env = util.get_env()
    iglog.in_prod_env("Current environment: {0}, Master_Source: {1}".format(current_env, master_source))
    if master_source == current_env:  # current environment is master source, run the data extract script
        gc_processor.process_events()

    n_proc = 2

    # MANDATES
    man_ids = util.get_ids_from_redshift(entity_type="mandate", job_name="go_cardless")
    iglog.in_test_env(message=str(man_ids))
    util.run_api_extract_multithreaded(id_list=man_ids, method=gc_processor.process_mandates, num_processes=n_proc)

    # SUBSCRIPTIONS
    sub_ids = util.get_ids_from_redshift(entity_type="subscription", job_name="go_cardless")
    util.run_api_extract_multithreaded(id_list=sub_ids, method=gc_processor.process_subscriptions, num_processes=n_proc)

    # PAYMENTS
    pay_ids = util.get_ids_from_redshift(entity_type="payment", job_name="go_cardless")
    util.run_api_extract_multithreaded(id_list=pay_ids, method=gc_processor.process_payments, num_processes=n_proc)

    # REFUNDS
    ref_ids = util.get_ids_from_redshift(entity_type="refund", job_name="go_cardless")
    util.run_api_extract_multithreaded(id_list=ref_ids, method=gc_processor.process_refunds, num_processes=n_proc)
