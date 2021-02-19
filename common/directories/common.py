common = {
    "smart_mv_hh_elec_refresh": {
        "sql_query_smart_mv_hh_elec_refresh": "REFRESH MATERIALIZED VIEW mv_smart_stage2_smarthalfhourlyreads_elec",
    },
    "d0379": {
        "elective_hh_trial_account_ids": [
            4601,
            13435,
            53238,
            45431,
            41653,
            166804,
            190409,
            55455,
            76056,
            114983,
            123989,
            1835,
            167692,
            199180,
            121074,
            210251,
            4646,
            127729,
            133302,
            132237,
            159031,
            147091,
            200955,
            192751,
            211886,
            199361,
        ],
        "s3_key_prefix": "flows/outbound/D0379-elective-hh-trial",
        "sql_query": """
select
    account_id,
    mpxn,
    measurement_class,
    primaryvalue,
    cast(hhdate as timestamp) as hhdate
from vw_etl_d0379_hh_elec_settlement
where
    account_id in ({elective_hh_trial_account_ids}) and
    cast(hhdate as timestamp) >= '{from_datetime}' and
    cast(hhdate as timestamp) <= '{to_datetime}'
order by account_id, mpxn, hhdate;
""",
    },
    "ensek_column_order": {
        # Ensek PA
        "ref_readings_internal": [
            "accountId",
            "billable",
            "hasLiveCharge",
            "hasRegisterAdvance",
            "meterId",
            "meterPointId",
            "meterPointNumber",
            "meterPointType",
            "meterReadingCreatedDate",
            "meterReadingDateTime",
            "meterReadingId",
            "meterReadingSourceUID",
            "meterReadingStatusUID",
            "meterReadingTypeUID",
            "meterSerialNumber",
            "readingValue",
            "registerId",
            "registerReadingId",
            "registerReference",
            "required",
        ],
        "ref_meterpoints": [
            "associationStartDate",
            "associationEndDate",
            "supplyStartDate",
            "supplyEndDate",
            "isSmart",
            "isSmartCommunicating",
            "meter_point_id",
            "meterPointNumber",
            "meterPointType",
            "account_id",
        ],
        "ref_meterpoints_attributes": [
            "attributes_attributeDescription",
            "attributes_attributeName",
            "attributes_attributeValue",
            "attributes_effectiveFromDate",
            "attributes_effectiveToDate",
            "meter_point_id",
            "account_id",
        ],
        "ref_meters": [
            "meterSerialNumber",
            "installedDate",
            "removedDate",
            "meterId",
            "meter_point_id",
            "account_id",
        ],
        "ref_meters_attributes": [
            "metersAttributes_attributeDescription",
            "metersAttributes_attributeName",
            "metersAttributes_attributeValue",
            "meter_id",
            "meter_point_id",
            "account_id",
        ],
        "ref_registers": [
            "registers_eacAq",
            "registers_registerReference",
            "registers_sourceIdType",
            "registers_tariffComponent",
            "registers_tpr",
            "registers_tprPeriodDescription",
            "meter_id",
            "register_id",
            "meter_point_id",
            "account_id",
        ],
        "ref_registers_attributes": [
            "registersAttributes_attributeDescription",
            "registersAttributes_attributeName",
            "registersAttributes_attributeValue",
            "meter_id",
            "register_id",
            "meter_point_id",
            "account_id",
        ],
        # Ensek Non PA
        "annual_statements": [
            "AccountId",
            "Amount",
            "CreatedDate",
            "EnergyType",
            "From",
            "StatementID",
            "To",
            "account_id",
        ],
        "internal_estimates_elec": [
            "EffectiveFrom",
            "EffectiveTo",
            "EstimationValue",
            "IsLive",
            "MPAN",
            "RegisterId",
            "SerialNumber",
            "account_id",
        ],
        "internal_estimates_gas": [
            "EffectiveFrom",
            "EffectiveTo",
            "EstimationValue",
            "IsLive",
            "MPRN",
            "RegisterId",
            "SerialNumber",
            "account_id",
        ],
        "direct_debit": [
            "Amount",
            "BankAccount",
            "BankName",
            "PaymentDate",
            "Reference",
            "SortCode",
            "account_id",
        ],
        "direct_debit_health_check": [
            "directDebitIsActive",
            "isTopup",
            "paymentAmount",
            "paymentDay",
            "DirectDebitType",
            "BankAccountIsActive",
            "DirectDebitStatus",
            "NextAvailablePaymentDate",
            "AccountName",
            "Amount",
            "BankName",
            "BankAccount",
            "PaymentDate",
            "Reference",
            "SortCode",
            "account_id",
        ],
        "account_transactions": [
            "actions",
            "amount",
            "creationDetail_createdBy",
            "creationDetail_createdDate",
            "currentBalance",
            "id",
            "isCancelled",
            "method",
            "sourceDate",
            "statementId",
            "transactionType",
            "transactionTypeFriendlyName",
            "account_id",
        ],
        "occupier_accounts": [
            "AccountID",
            "COT Date",
            "Current Balance",
            "Days Since COT Date",
            "etl_change",
        ],
        "tariff_history": [
            "tariffName",
            "startDate",
            "endDate",
            "discounts",
            "tariffType",
            "exitFees",
            "account_id",
        ],
        "tariff_history_elec_unit_rates": [
            "chargeableComponentUID",
            "name",
            "rate",
            "registers",
            "tariffName",
            "startDate",
            "endDate",
            "account_id",
        ],
        "tariff_history_elec_standing_charge": [
            "chargeableComponentUID",
            "name",
            "rate",
            "registers",
            "tariffName",
            "startDate",
            "endDate",
            "account_id",
        ],
        "tariff_history_gas_unit_rates": [
            "chargeableComponentUID",
            "name",
            "rate",
            "registers",
            "tariffName",
            "startDate",
            "endDate",
            "account_id",
        ],
        "tariff_history_gas_standing_charge": [
            "chargeableComponentUID",
            "name",
            "rate",
            "registers",
            "tariffName",
            "startDate",
            "endDate",
            "account_id",
        ],
        "registrations_elec_meterpoint": [
            "account_id",
            "meter_point_id",
            "meterpointnumber",
            "status",
        ],
        "registrations_gas_meterpoint": [
            "account_id",
            "meter_point_id",
            "meterpointnumber",
            "status",
        ],
        "account_settings": [
            "AccountID",
            "BillDayOfMonth",
            "BillFrequencyMonths",
            "NextBillDate",
            "NextBillDay",
            "NextBillMonth",
            "NextBillYear",
            "SendEmail",
            "SendPost",
            "TopupWithEstimates",
            "account_id",
        ],
    },
    "alp_column_order": {
        "cv": ["name", "applicable_at", "applicable_for", "value"],
        "wcf": ["name", "applicable_at", "applicable_for", "value"],
    },
    "weather_column_order": {
        "historical_weather": [
            "azimuth",
            "clouds",
            "datetime",
            "dewpt",
            "dhi",
            "dni",
            "elev_angle",
            "ghi",
            "h_angle",
            "pod",
            "precip",
            "pres",
            "rh",
            "slp",
            "snow",
            "solar_rad",
            "temp",
            "timestamp_local",
            "timestamp_utc",
            "ts",
            "uv",
            "vis",
            "weather",
            "wind_dir",
            "wind_spd",
            "timezone",
            "state_code",
            "country_code",
            "lat",
            "lon",
            "city_name",
            "station_id",
            "city_id",
            "sources",
            "postcode",
        ],
    },
    "land_registry_column_order": {
        "land_registry": [
            "transactionDate",
            "newBuild",
            "pricePaid",
            "transactionId",
            "propertyType",
            "recordStatus",
            "transactionCategory",
            "uprn",
            "id",
        ]
    },
    "epc_column_order": {
        "epc_certificates": [
            "address",
            "address1",
            "address2",
            "address3",
            "building_reference_number",
            "built_form",
            "certificate_hash",
            "co2_emiss_curr_per_floor_area",
            "co2_emissions_current",
            "co2_emissions_potential",
            "constituency",
            "constituency_label",
            "county",
            "current_energy_efficiency",
            "current_energy_rating",
            "energy_consumption_current",
            "energy_consumption_potential",
            "energy_tariff",
            "environment_impact_current",
            "environment_impact_potential",
            "extension_count",
            "flat_storey_count",
            "flat_top_storey",
            "floor_description",
            "floor_energy_eff",
            "floor_env_eff",
            "floor_height",
            "floor_level",
            "glazed_area",
            "glazed_type",
            "heat_loss_corridoor",
            "heating_cost_current",
            "heating_cost_potential",
            "hot_water_cost_current",
            "hot_water_cost_potential",
            "hot_water_energy_eff",
            "hot_water_env_eff",
            "hotwater_description",
            "inspection_date",
            "lighting_cost_current",
            "lighting_cost_potential",
            "lighting_description",
            "lighting_energy_eff",
            "lighting_env_eff",
            "lmk_key",
            "local_authority",
            "local_authority_label",
            "lodgement_date",
            "low_energy_lighting",
            "main_fuel",
            "main_heating_controls",
            "mainheat_description",
            "mainheat_energy_eff",
            "mainheat_env_eff",
            "mainheatc_energy_eff",
            "mainheatc_env_eff",
            "mainheatcont_description",
            "mains_gas_flag",
            "mechanical_ventilation",
            "multi_glaze_proportion",
            "number_habitable_rooms",
            "number_heated_rooms",
            "number_open_fireplaces",
            "photo_supply",
            "postcode",
            "potential_energy_efficiency",
            "potential_energy_rating",
            "property_type",
            "roof_description",
            "roof_energy_eff",
            "roof_env_eff",
            "secondheat_description",
            "sheating_energy_eff",
            "sheating_env_eff",
            "solar_water_heating_flag",
            "total_floor_area",
            "transaction_type",
            "unheated_corridor_length",
            "walls_description",
            "walls_energy_eff",
            "walls_env_eff",
            "wind_turbine_count",
            "windows_description",
            "windows_energy_eff",
            "windows_env_eff",
        ]
    },
    "go_cardless_column_order": {
        "customers": [
            "client_id",
            "created_at",
            "email",
            "given_name",
            "family_name",
            "company_name",
            "country_code",
            "EnsekID",
        ],
        "events": [
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
        "payouts": [
            "payout_id",
            "amount",
            "arrival_date",
            "created_at",
            "deducted_fees",
            "payout_type",
            "reference",
            "status",
            "creditor",
            "creditor_bank_account",
        ],
    },
    "postcodes": {
        #  The API from which we download the postcode data.
        "api_url": "https://opendata.camden.gov.uk/api/views/tr8t-gqz7/rows.csv?accessType=DOWNLOAD",
        # Where to write the data to in S3
        "s3_key": "stage2/stage2_Postcodes/postcodes.csv",
        # The dtypes to use when we read the CSV file in to a pandas DataFrame
        "extract_dtypes": {
            "Postcode 1": "object",
            "Postcode 2": "object",
            "Postcode 3": "object",
            "Date Introduced": "object",
            "User Type": "int64",
            "Easting": "int64",
            "Northing": "int64",
            "Positional Quality": "int64",
            "County Code": "object",
            "County Name": "object",
            "Local Authority Code": "object",
            "Local Authority Name": "object",
            "Ward Code": "object",
            "Ward Name": "object",
            "Country Code": "object",
            "Country Name": "object",
            "Region Code": "object",
            "Region Name": "object",
            "Parliamentary Constituency Code": "object",
            "Parliamentary Constituency Name": "object",
            "European Electoral Region Code": "object",
            "European Electoral Region Name": "object",
            "Primary Care Trust Code": "object",
            "Primary Care Trust Name": "object",
            "Lower Super Output Area Code": "object",
            "Lower Super Output Area Name": "object",
            "Middle Super Output Area Code": "object",
            "Middle Super Output Area Name": "object",
            "Output Area Classification Code": "object",
            "Output Area Classification Name": "object",
            "Longitude": "float64",
            "Latitude": "float64",
            "Spatial Accuracy": "object",
            "Last Uploaded": "object",
            "Location": "object",
            "Socrata ID": "int64",
        },
        # The dtypes to use when we write the CSV file to S3. This is similar to the above
        # dict, but includes a type for the 'etl_change' column that the transform step adds.
        "transform_dtypes": {
            "Postcode 1": "object",
            "Postcode 2": "object",
            "Postcode 3": "object",
            "Date Introduced": "object",
            "User Type": "int64",
            "Easting": "int64",
            "Northing": "int64",
            "Positional Quality": "int64",
            "County Code": "object",
            "County Name": "object",
            "Local Authority Code": "object",
            "Local Authority Name": "object",
            "Ward Code": "object",
            "Ward Name": "object",
            "Country Code": "object",
            "Country Name": "object",
            "Region Code": "object",
            "Region Name": "object",
            "Parliamentary Constituency Code": "object",
            "Parliamentary Constituency Name": "object",
            "European Electoral Region Code": "object",
            "European Electoral Region Name": "object",
            "Primary Care Trust Code": "object",
            "Primary Care Trust Name": "object",
            "Lower Super Output Area Code": "object",
            "Lower Super Output Area Name": "object",
            "Middle Super Output Area Code": "object",
            "Middle Super Output Area Name": "object",
            "Output Area Classification Code": "object",
            "Output Area Classification Name": "object",
            "Longitude": "float64",
            "Latitude": "float64",
            "Spatial Accuracy": "object",
            "Last Uploaded": "object",
            "Location": "object",
            "Socrata ID": "int64",
            "etl_change": "datetime64[ns]",
        },
    },
}
