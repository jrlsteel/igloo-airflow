account_ids = {
    "daily": """-- account_ids in our database that are live, meterpoint data absent or closed in the last 56 days
                select full_account_list.account_id
                from (select account_id
                      from ref_meterpoints_raw
                      union
                      distinct
                      select account_id
                      from ref_occupier_accounts
                      union
                      distinct
                      select external_id as account_id
                      from ref_cdb_supply_contracts) full_account_list
                         left join (select account_id, max(nvl(end_date, getdate() + 1000)) as sed
                                    from ref_meterpoints_raw
                                    group by account_id) acc_sed on acc_sed.account_id = full_account_list.account_id
                where datediff(days, nvl(sed, getdate() + 1000), getdate()) < 56
                  and full_account_list.account_id is not null
                union
                distinct
                -- accounts whose most recent transaction was within the past 4 weeks or left a non-zero balance
                select account_id
                from (select account_id,
                             creationdetail_createddate,
                             currentbalance,
                             row_number() over (partition by account_id order by creationdetail_createddate desc) as rn
                      from ref_account_transactions) ordered_transactions
                where rn = 1
                  and (currentbalance != 0 or datediff(days, creationdetail_createddate, getdate()) < 28)
                  and account_id is not null
                union
                distinct
                -- accounts present in the manual override table
                select account_id
                from dwh_manual_batch_accounts
                where getdate() between nvl(use_from, getdate() - 1) and nvl(use_until, getdate() + 1)
                  and daily_batch
                  and account_id is not null
                order by account_id""",
    "weekly": """-- account_ids in our database that are live, meterpoint data absent or closed in the last 365 days
                select full_account_list.account_id
                from (select account_id
                      from ref_meterpoints_raw
                      union
                      distinct
                      select account_id
                      from ref_occupier_accounts
                      union
                      distinct
                      select external_id as account_id
                      from ref_cdb_supply_contracts) full_account_list
                         left join (select account_id, max(nvl(end_date, getdate() + 1000)) as sed
                                    from ref_meterpoints_raw
                                    group by account_id) acc_sed on acc_sed.account_id = full_account_list.account_id
                where datediff(days, nvl(sed, getdate() + 1000), getdate()) < 365
                  and full_account_list.account_id is not null
                union
                distinct
                -- accounts whose most recent transaction was within the past year or left a non-zero balance
                select account_id
                from (select account_id,
                             creationdetail_createddate,
                             currentbalance,
                             row_number() over (partition by account_id order by creationdetail_createddate desc) as rn
                      from ref_account_transactions) ordered_transactions
                where rn = 1
                  and (currentbalance != 0 or datediff(days, creationdetail_createddate, getdate()) < 365)
                  and account_id is not null
                union
                distinct
                -- accounts present in the manual override table
                select account_id
                from dwh_manual_batch_accounts
                where getdate() between nvl(use_from, getdate() - 1) and nvl(use_until, getdate() + 1)
                  and account_id is not null
                order by account_id""",

}

acc_mp_ids = {
    "daily": """select distinct account_id, meter_point_id, meterpointnumber, meterpointtype
                from ref_meterpoints_raw
                where datediff(days, greatest(supplystartdate, associationstartdate), getdate()) < 7
                order by account_id""",
}

pending_acc_ids = {
    "daily": """select account_id, min(greatest(associationstartdate, supplystartdate)) as ssd
from ref_meterpoints
group by account_id
having datediff(days, ssd, getdate()) <= 7""",
    "weekly": """select account_id, min(greatest(associationstartdate, supplystartdate)) as ssd
from ref_meterpoints
group by account_id
having datediff(days, ssd, getdate()) <= 7"""
}


tariff_diff_acc_ids = {
    "daily": """select distinct account_id
                from vw_tariff_checks
                where error_code in ('LIVE_ENSEK_MISSING', 'LIVE_MISMATCH',
                                     'PENDING_ENSEK_MISSING', 'PENDING_MISMATCH')""",
    "weekly": """select distinct account_id
                from vw_tariff_checks
                where error_code in ('LIVE_ENSEK_MISSING', 'LIVE_MISMATCH',
                                     'PENDING_ENSEK_MISSING', 'PENDING_MISMATCH',
                                     'FINAL_ENSEK_MISSING', 'FINAL_MISMATCH')"""
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
    "daily": """select * from vw_etl_weather_postcode_sectors""",
    "weekly": """select * from vw_etl_weather_postcode_sectors""",

}

smart_reads_billing = {
    "elec": """select * from vw_etl_smart_billing_reads_elec""",
    "gas": """select * from vw_etl_smart_billing_reads_gas""",
    "all": """select * from vw_etl_smart_billing_reads_all""",
}

weather_forecast = {
    "hourly": {
      "stage1": """select * from vw_etl_weather_postcode_sectors""",
      "stage2": """select * from vw_etl_weather_forecast_hourly_load"""
    },
    "daily": {
      "stage1": """select * from vw_etl_weather_postcode_sectors""",
      "stage2": """select * from vw_etl_weather_forecast_daily_load"""
    }
}
