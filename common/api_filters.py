account_ids = {
    "daily": """-- account_ids in our database that are live, meterpoint data absent or closed in the last 56 days
                select * from vw_etl_account_ids_daily order by account_id""",
    "weekly": """-- account_ids in our database that are live, meterpoint data absent or closed in the last 365 days
                 select * from vw_etl_account_ids_weekly order by account_id""",

}

acc_mp_ids = {
    "daily": """select * from vw_etl_acc_mp_ids order by account_id""",
}

pending_acc_ids = {
    "daily": """select * from vw_etl_pending_acc_ids_weekly order by account_id""",
    "weekly": """select * from vw_etl_pending_acc_ids_weekly order by account_id""",
}


tariff_diff_acc_ids = {
    #"daily": """select * from vw_etl_tariff_diff_acc_ids_daily order by account_id """,
    #"weekly": """select * from vw_etl_tariff_diff_acc_ids_daily order by account_id""",
    "daily": """select * from vw_etl_account_ids_daily order by account_id """,
    "weekly": """select * from vw_etl_account_ids_daily order by account_id""",
}


epc_postcodes = {
    "daily": """select * from vw_etl_epc_postcodes_daily order by postcode""",
    "weekly": """select * from vw_etl_epc_postcodes_daily order by postcode""",

}

land_registry_postcodes = {
    "daily": """select * from vw_etl_land_registry_postcodes_daily order by postcode""",
    "weekly": """select * from vw_etl_land_registry_postcodes_daily order by postcode""",

}

weather_postcodes = {
    "daily": """select * from vw_etl_weather_postcodes_daily order by postcode""",
    "weekly": """select * from vw_etl_weather_postcodes_daily order by postcode""",

}

smart_reads_billing = {
    "elec": """select * from vw_etl_smart_billing_reads_elec""",
    "gas": """select * from vw_etl_smart_billing_reads_gas""",

}
