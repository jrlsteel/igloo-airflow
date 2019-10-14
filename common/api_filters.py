account_ids = {
    "daily": """select coalesce(sc.external_id, oa.account_id, mr.account_id)            as acc_id,
                       case when count(mp_sed) < count(*) then null else max(mp_sed) end as sed
                from ref_cdb_supply_contracts sc
                         full join ref_occupier_accounts oa on sc.external_id = oa.account_id
                         full join (select account_id, least(supplyenddate, associationenddate) as mp_sed
                                    from ref_meterpoints_raw) mr on mr.account_id = coalesce(oa.account_id, sc.external_id)
                group by acc_id
                having acc_id is not null
                   and (sed is null or datediff(weeks, sed, getdate()) < 8)
                order by acc_id""",
    "weekly": """select coalesce(sc.external_id, oa.account_id, mr.account_id)            as acc_id,
                       case when count(mp_sed) < count(*) then null else max(mp_sed) end as sed
                from ref_cdb_supply_contracts sc
                         full join ref_occupier_accounts oa on sc.external_id = oa.account_id
                         full join (select account_id, least(supplyenddate, associationenddate) as mp_sed
                                    from ref_meterpoints_raw) mr on mr.account_id = coalesce(oa.account_id, sc.external_id)
                group by acc_id
                having acc_id is not null
                   and (sed is null or datediff(years, sed, getdate()) < 1)
                order by acc_id""",

}

acc_mp_ids = {
    "daily": """select distinct account_id, meter_point_id, meterpointnumber, meterpointtype
                from ref_meterpoints_raw
                where datediff(weeks, greatest(supplystartdate, associationstartdate), getdate()) < 1
                order by account_id""",
}
