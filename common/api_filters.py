account_ids = {
    "daily": """select account_id, case when count(mp_sed) < count(*) then null else max(mp_sed) end as sed
              from (select account_id, least(supplyenddate, associationenddate) as mp_sed
                    from ref_meterpoints_raw mpr) mps
              group by account_id
              having sed is null or datediff(weeks, sed, getdate()) < 8
              order by account_id""",
    "weekly": """select account_id, case when count(mp_sed) < count(*) then null else max(mp_sed) end as sed
              from (select account_id, least(supplyenddate, associationenddate) as mp_sed
                    from ref_meterpoints_raw mpr) mps
              group by account_id
              having sed is null or datediff(years, sed, getdate()) < 1
              order by account_id""",

}

acc_mp_ids = {
    "daily": """select distinct account_id, meter_point_id, meterpointnumber, meterpointtype
                from ref_meterpoints_raw
                where datediff(weeks, greatest(supplystartdate, associationstartdate), getdate()) < 1
                order by account_id""",
}
