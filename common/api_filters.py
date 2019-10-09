account_ids = {
    "within_sed_plus_8_weeks": """select account_id, case when count(mp_sed) < count(*) then null else max(mp_sed) end as sed
              from (select account_id, least(supplyenddate, associationenddate) as mp_sed
                    from ref_meterpoints_raw mpr) mps
              group by account_id
              having sed is null or datediff(weeks, sed, getdate()) < 8
              order by account_id""",
    "within_sed_plus_1_year": """select account_id, case when count(mp_sed) < count(*) then null else max(mp_sed) end as sed
              from (select account_id, least(supplyenddate, associationenddate) as mp_sed
                    from ref_meterpoints_raw mpr) mps
              group by account_id
              having sed is null or datediff(years, sed, getdate()) < 1
              order by account_id""",

}