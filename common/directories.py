############ Url and token for Ensek Api's ##############
dev = {
    "apis": {
        "accounts": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}"
        },
        "meterpoints": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/MeterPoints"
        },

        "meterpoints_history": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/MeterPoints?includeHistory=true"
        },

        "meterpoints_readings": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/MeterPoints/{1}/Readings"
        },

        "meterpoints_readings_billeable": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/MeterPoints/{1}/Readings?isBilleable=true"
        },

        "direct_debits": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/DirectDebits"
        },

        "direct_debits_heath_check": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/DirectDebits/HealthCheck"
        },

        "internal_estimates": {
            "api_url": "https://igloo.ignition.ensek.co.uk/api/accounts/{0}/estimatedusage"
        },

        "internal_readings": {
            "api_url": "https://igloo.ignition.ensek.co.uk/api/account/{0}/meter-readings?sortField=meterReadingDateTime&sortDirection=Descending"
        },

        "occupier_accounts": {
            "api_url": "https://igloo.ignition.ensek.co.uk/api/DataSources/GetData?name=OccupierAccounts"
        },

        "account_status": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/SupplyStatus"
        },

        "elec_status": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/Processes/Registrations/Elec"
        },

        "gas_status": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/Processes/Registrations/Gas"
        },

        "elec_mp_status": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/Processes/Registrations/Elec?meterpointnumber={1}"
        },

        "gas_mp_status": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/Processes/Registrations/Gas?meterpointnumber={1}"
        },

        "tariff_history": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/TariffsWithHistory"
        },

        "live_balances": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/LiveBalances"
        },

        "live_balances_with_detail": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/LiveBalancesWithDetail"
        },
        "account_transactions": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/Transactions"
        },
        "annual_statements": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/annualStatements"
        },
        "statements": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/Statements"
        },
        "account_settings": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/AccountSettings"
        },

        "token": "Wk01QnVWVU01aWlLTiVeUWtwMUIyRU5EbCN0VTJUek01KmJJVFcyVGFaeiNtJkFpYUJwRUNNM2MzKjVHcjVvIQ==",

        "igloo_epc_certificates": {
            "api_url": "https://epc.opendatacommunities.org/api/v1/domestic/search?postcode={0}",
            "token": "am9uYXRoYW4uc3RlZWxAaWdsb28uZW5lcmd5OjZhZDU0ZGY4NzM0MmI4YmEyYzI1YTYyZTFlOGYxNTM4NTA2ZTQyMzQ="
        },
        "igloo_epc_recommendations": {
            "api_url": "https://epc.opendatacommunities.org/api/v1/domestic/recommendations/{0}",
            "token": "am9uYXRoYW4uc3RlZWxAaWdsb28uZW5lcmd5OjZhZDU0ZGY4NzM0MmI4YmEyYzI1YTYyZTFlOGYxNTM4NTA2ZTQyMzQ="
        },

        "igloo_epc_full": {
            "api_url": "https://epc.opendatacommunities.org/login-with-token?",
            "file_url": "https://epc.opendatacommunities.org/files/all-domestic-certificates.zip"
        },

        "historical_weather": {
            "api_url": "https://api.weatherbit.io/v2.0/history/hourly?postal_code={0}&start_date={1}&end_date={2}&key={3}",
            "token": "ee3b1ce4ed8c475c9919d0a024f9d265"
        },
        "historical_energy_weather": {
            "api_url": "https://api.weatherbit.io/v2.0/history/energy?postal_code={0}&start_date={1}&end_date={2}&key={3}",
            "token": "ee3b1ce4ed8c475c9919d0a024f9d265"
        },
        "land_registry": {
            "api_url": "http://landregistry.data.gov.uk/data/ppi/transaction-record",
        },
        "gas_historical": {
            "api_url": "http://marketinformation.natgrid.co.uk/MIPIws-public/public/publicwebservice.asmx",
        },
        "igloo_zendesk_user_tickets": {
            "api_url": "https://iglooenergy.zendesk.com/api/v2/users/{0}/tickets/requested.json",
        },
        "igloo_eac_batch": {
            "api_url": "https://9m73o40v3h.execute-api.eu-west-1.amazonaws.com/prod/Accounts/{0}/eac",
            "token": "quqSpLjJly3jlh79S7uUN9uE1YoPQums86o0768f"
        },

    },

    "s3_bucket": "igloo-data-warehouse-dev-555393537168",

    "s3_key": {
        "Accounts": "stage1/Accounts/",
        "MeterPoints": "stage1/MeterPoints/",
        "MeterPointsAttributes": "stage1/MeterPointsAttributes/",
        "Meters": "stage1/Meters/",
        "MetersAttributes": "stage1/MetersAttributes/",
        "Registers": "stage1/Registers/",
        "RegistersAttributes": "stage1/RegistersAttributes/",
        "Readings": "stage1/Readings/",
        "ReadingsInternal": "stage1/ReadingsInternal/",
        "AccountStatus": "stage1/AccountStatus/",
        "RegistrationsElec": "stage1/RegistrationsElec/",
        "RegistrationsGas": "stage1/RegistrationsGas/",
        "RegistrationsElecMeterpoint": "stage1/RegistrationsElecMeterpoint/",
        "RegistrationsGasMeterpoint": "stage1/RegistrationsGasMeterpoint/",
        "EstimatesElecInternal": "stage1/EstimatesElecInternal/",
        "EstimatesGasInternal": "stage1/EstimatesGasInternal/",
        "TariffHistory": "stage1/TariffHistory/",
        "TariffHistoryElecStandCharge": "stage1/TariffHistoryElecStandCharge/",
        "TariffHistoryElecUnitRates": "stage1/TariffHistoryElecUnitRates/",
        "TariffHistoryGasStandCharge": "stage1/TariffHistoryGasStandCharge/",
        "TariffHistoryGasUnitRates": "stage1/TariffHistoryGasUnitRates/",
        "DirectDebit": "stage1/DirectDebit/",
        "DirectDebitHealthCheck": "stage1/DirectDebitHealthCheck/",
        "ReadingsBilleable": "stage1/ReadingsBilleable/",
        "LiveBalances": "stage1/LiveBalances/",
        "LiveBalancesWithDetail": "stage1/LiveBalancesWithDetail/",
        "AccountTransactions": "stage1/AccountTransactions/",
        "MeterPointsHistory": "stage1/MeterPointsHistory/",
        "MeterPointsAttributesHistory": "stage1/MeterPointsAttributesHistory/",
        "MetersHistory": "stage1/MetersHistory/",
        "MetersAttributesHistory": "stage1/MetersAttributesHistory/",
        "RegistersHistory": "stage1/RegistersHistory/",
        "RegistersAttributesHistory": "stage1/RegistersAttributesHistory/",
        "ReadingsHistory": "stage1/ReadingsHistory/",
        "ReadingsBilleableHistory": "stage1/ReadingsBilleableHistory/",
        "AnnualStatements": "stage1/AnnualStatements/",
        "Statements": "stage1/Statements/",
        "AccountSettings": "stage1/AccountSettings/",
        "OccupierAccounts": "stage2/stage2_OccupierAccounts/"
    },


    "s3_d18_key": {
        "D18Raw": "stage1/D18/D18Raw/",
        "D18BPP": "stage1/D18/D18BPP/",
        "D18PPC": "stage1/D18/D18PPC/",
        "D18_SFTP": "D18",
        "D18Archive": "stage1/D18/D18Archive/",
        "D18Suffix": ".flw"
    },

    "s3_epc_key": {
        "EPCCertificates": "stage1/EPC/EPCCertificates/",
        "EPCRecommendations": "stage1/EPC/EPCRecommendations/"
    },

    "s3_epc_full_key": {
        "EPCFullDownload_path": "stage1/EPC_Full/all-domestic-certificates.zip",
        "EPCFullExtract_path": "stage1/EPC_Full/EPC_Full_Extract",
        "EPCFullCertificates": "stage1/EPC_Full/EPCCertificates/",
        "EPCFullRecommendations": "stage1/EPC_Full/EPCRecommendations/"
    },


    "s3_zendesk_igloo_key": {
        "ZendeskUserTickets": "stage1/Zendesk/UserTickets/"
    },

    "s3_weather_key": {
        "HistoricalWeather": "stage1/HistoricalWeather/"
    },
    "s3_alp_wcf": {
        "AlpWCF": "stage1/ALP/AlpWCF/"
    },
    "s3_alp_cv": {
        "AlpCV": "stage1/ALP/AlpCV/"
    },

    "s3_land_reg_key": {
        "LandRegistry": "stage1/LandRegistry/"
    },
    "s3_nrl_key": {
        "Raw": "stage1/ReadingsNRL/NRLRaw/",
        "NRL": "stage1/ReadingsNRL/NRL/",
        "SFTP": "CNGexport/NRL",
        "Suffix": ".flw"
    },
    "s3_nosi_key": {
        "Raw": "stage1/ReadingsNOSIGas/",
        "SFTP": "NOSI"
    },

    "glue_ensek_job_name": "_process_ref_tables_dev",
    "glue_d18_job_name": "_process_ref_tables_dev",
    "glue_direct_debits_job_name": "_process_ref_tables_dev",
    "glue_registrations_meterpoints_status_job_name": "_process_ref_tables_dev",
    "glue_meterpoints_status_job_name": "_process_ref_tables_dev",
    "glue_zendesk_job_name": "_process_ref_tables_dev",
    "glue_historicalweather_jobname": "_process_ref_tables_dev",
    "glue_eac_aq_job_name": "_process_ref_tables_dev",
    "glue_igl_ind_eac_aq_job_name": "_process_ref_tables_dev",
    "glue_epc_job_name": "_process_ref_tables_dev",
    "glue_land_registry_job_name": "_process_ref_tables_dev",
    "glue_alp_job_name": "_process_ref_tables_dev",
    "glue_customerDB_job_name": "_process_ref_customerdb_dev",
    "glue_staging_job_name": "process_staging_files_dev",
    "glue_tado_efficiency_job_name": "_process_ref_tables_dev",
    "glue_daily_sales_job_name": "_process_ref_tables_dev",
    "glue_internal_estimates_job_name":"_process_ref_tables_dev",
    "glue_annual_statements_job_name": "_process_ref_tables_dev",
    "glue_tariff_history_job_name": "_process_ref_tables_dev",
    "glue_cons_accu_job_name": "_process_ref_tables_dev",
    "glue_internal_readings_nosi_job_name": "_process_ref_tables_dev",
    "glue_nrl_job_name": "_process_ref_tables_dev",
    "glue_estimated_advance_job_name": "_process_ref_tables_dev",
    "glue_smart_meter_eligibility_job_name": "_process_ref_tables_dev",
    "glue_occupier_accounts_job_name": "_process_ref_tables_dev",
    "glue_igloo_calculated_tariffs_job_name": "_process_ref_tables_dev",
    "glue_meets_eligibility_job_name": "_process_ref_tables_dev",
    "glue_reporting_job_name": "_process_ref_tables_dev",
}

uat = {
    "apis": {
        "accounts": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}"
        },
        "meterpoints": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/MeterPoints"
        },

        "meterpoints_history": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/MeterPoints?includeHistory=true"
        },

        "meterpoints_readings": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/MeterPoints/{1}/Readings"
        },

        "meterpoints_readings_billeable": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/MeterPoints/{1}/Readings?isBilleable=true"
        },

        "direct_debits": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/DirectDebits"
        },

        "direct_debits_heath_check": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/DirectDebits/HealthCheck"
        },

        "internal_estimates": {
            "api_url": "https://igloo.ignition.ensek.co.uk/api/accounts/{0}/estimatedusage"
        },

        "internal_readings": {
            "api_url": "https://igloo.ignition.ensek.co.uk/api/account/{0}/meter-readings?sortField=meterReadingDateTime&sortDirection=Descending"
        },

        "occupier_accounts": {
            "api_url": "https://igloo.ignition.ensek.co.uk/api/DataSources/GetData?name=OccupierAccounts"
        },

        "account_status": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/SupplyStatus"
        },

        "elec_status": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/Processes/Registrations/Elec"
        },

        "gas_status": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/Processes/Registrations/Gas"
        },

        "elec_mp_status": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/Processes/Registrations/Elec?meterpointnumber={1}"
        },

        "gas_mp_status": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/Processes/Registrations/Gas?meterpointnumber={1}"
        },

        "tariff_history": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/TariffsWithHistory"
        },

        "live_balances": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/LiveBalances"
        },

        "live_balances_with_detail": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/LiveBalancesWithDetail"
        },
        "account_transactions": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/Transactions"
        },
        "annual_statements": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/annualStatements"
        },
        "statements": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/Statements"
        },
        "account_settings": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/AccountSettings"
        },

        "token": "Wk01QnVWVU01aWlLTiVeUWtwMUIyRU5EbCN0VTJUek01KmJJVFcyVGFaeiNtJkFpYUJwRUNNM2MzKjVHcjVvIQ==",

        "igloo_epc_certificates": {
            "api_url": "https://epc.opendatacommunities.org/api/v1/domestic/search?postcode={0}",
            "token": "am9uYXRoYW4uc3RlZWxAaWdsb28uZW5lcmd5OjZhZDU0ZGY4NzM0MmI4YmEyYzI1YTYyZTFlOGYxNTM4NTA2ZTQyMzQ="
        },
        "igloo_epc_recommendations": {
            "api_url": "https://epc.opendatacommunities.org/api/v1/domestic/recommendations/{0}",
            "token": "am9uYXRoYW4uc3RlZWxAaWdsb28uZW5lcmd5OjZhZDU0ZGY4NzM0MmI4YmEyYzI1YTYyZTFlOGYxNTM4NTA2ZTQyMzQ="
        },

        "igloo_epc_full": {
            "api_url": "https://epc.opendatacommunities.org/login-with-token?",
            "file_url": "https://epc.opendatacommunities.org/files/all-domestic-certificates.zip"
        },

        "historical_weather": {
            "api_url": "https://api.weatherbit.io/v2.0/history/hourly?postal_code={0}&start_date={1}&end_date={2}&key={3}",
            "token": "ee3b1ce4ed8c475c9919d0a024f9d265"
        },
        "historical_energy_weather": {
            "api_url": "https://api.weatherbit.io/v2.0/history/energy?postal_code={0}&start_date={1}&end_date={2}&key={3}",
            "token": "ee3b1ce4ed8c475c9919d0a024f9d265"
        },
        "land_registry": {
            "api_url": "http://landregistry.data.gov.uk/data/ppi/transaction-record",
        },
        "gas_historical": {
            "api_url": "http://marketinformation.natgrid.co.uk/MIPIws-public/public/publicwebservice.asmx",
        },
        "igloo_zendesk_user_tickets": {
            "api_url": "https://iglooenergy.zendesk.com/api/v2/users/{0}/tickets/requested.json",
        },
        "igloo_eac_batch": {
            "api_url": "https://9m73o40v3h.execute-api.eu-west-1.amazonaws.com/prod/Accounts/{0}/eac",
            "token": "quqSpLjJly3jlh79S7uUN9uE1YoPQums86o0768f"
        },

    },

    "s3_bucket": "igloo-data-warehouse-dev",

    "s3_key": {
        "Accounts": "stage1/Accounts/",
        "MeterPoints": "stage1/MeterPoints/",
        "MeterPointsAttributes": "stage1/MeterPointsAttributes/",
        "Meters": "stage1/Meters/",
        "MetersAttributes": "stage1/MetersAttributes/",
        "Registers": "stage1/Registers/",
        "RegistersAttributes": "stage1/RegistersAttributes/",
        "Readings": "stage1/Readings/",
        "ReadingsInternal": "stage1/ReadingsInternal/",
        "AccountStatus": "stage1/AccountStatus/",
        "RegistrationsElec": "stage1/RegistrationsElec/",
        "RegistrationsGas": "stage1/RegistrationsGas/",
        "RegistrationsElecMeterpoint": "stage1/RegistrationsElecMeterpoint/",
        "RegistrationsGasMeterpoint": "stage1/RegistrationsGasMeterpoint/",
        "EstimatesElecInternal": "stage1/EstimatesElecInternal/",
        "EstimatesGasInternal": "stage1/EstimatesGasInternal/",
        "TariffHistory": "stage1/TariffHistory/",
        "TariffHistoryElecStandCharge": "stage1/TariffHistoryElecStandCharge/",
        "TariffHistoryElecUnitRates": "stage1/TariffHistoryElecUnitRates/",
        "TariffHistoryGasStandCharge": "stage1/TariffHistoryGasStandCharge/",
        "TariffHistoryGasUnitRates": "stage1/TariffHistoryGasUnitRates/",
        "DirectDebit": "stage1/DirectDebit/",
        "DirectDebitHealthCheck": "stage1/DirectDebitHealthCheck/",
        "ReadingsBilleable": "stage1/ReadingsBilleable/",
        "LiveBalances": "stage1/LiveBalances/",
        "LiveBalancesWithDetail": "stage1/LiveBalancesWithDetail/",
        "AccountTransactions": "stage1/AccountTransactions/",
        "MeterPointsHistory": "stage1/MeterPointsHistory/",
        "MeterPointsAttributesHistory": "stage1/MeterPointsAttributesHistory/",
        "MetersHistory": "stage1/MetersHistory/",
        "MetersAttributesHistory": "stage1/MetersAttributesHistory/",
        "RegistersHistory": "stage1/RegistersHistory/",
        "RegistersAttributesHistory": "stage1/RegistersAttributesHistory/",
        "ReadingsHistory": "stage1/ReadingsHistory/",
        "ReadingsBilleableHistory": "stage1/ReadingsBilleableHistory/",
        "AnnualStatements": "stage1/AnnualStatements/",
        "Statements": "stage1/Statements/",
        "AccountSettings": "stage1/AccountSettings/",
        "OccupierAccounts": "stage2/stage2_OccupierAccounts/"
    },


    "s3_d18_key": {
        "D18Raw": "stage1/D18/D18Raw/",
        "D18BPP": "stage1/D18/D18BPP/",
        "D18PPC": "stage1/D18/D18PPC/",
        "D18_SFTP": "D18",
        "D18Archive": "stage1/D18/D18Archive/",
        "D18Suffix": ".flw"
    },

    "s3_epc_key": {
        "EPCCertificates": "stage1/EPC/EPCCertificates/",
        "EPCRecommendations": "stage1/EPC/EPCRecommendations/"
    },

    "s3_epc_full_key": {
        "EPCFullDownload_path": "stage1/EPC_Full/all-domestic-certificates.zip",
        "EPCFullExtract_path": "stage1/EPC_Full/EPC_Full_Extract",
        "EPCFullCertificates": "stage1/EPC_Full/EPCCertificates/",
        "EPCFullRecommendations": "stage1/EPC_Full/EPCRecommendations/"
    },


    "s3_zendesk_igloo_key": {
        "ZendeskUserTickets": "stage1/Zendesk/UserTickets/"
    },

    "s3_weather_key": {
        "HistoricalWeather": "stage1/HistoricalWeather/"
    },
    "s3_alp_wcf": {
        "AlpWCF": "stage1/ALP/AlpWCF/"
    },
    "s3_alp_cv": {
        "AlpCV": "stage1/ALP/AlpCV/"
    },

    "s3_land_reg_key": {
        "LandRegistry": "stage1/LandRegistry/"
    },
    "s3_nrl_key": {
        "Raw": "stage1/ReadingsNRL/NRLRaw/",
        "NRL": "stage1/ReadingsNRL/NRL/",
        "SFTP": "CNGexport/NRL",
        "Suffix": ".flw"
    },
    "s3_nosi_key": {
        "Raw": "stage1/ReadingsNOSIGas/",
        "SFTP": "NOSI"
    },
    "s3_ensekflow_key": {
        "outbound":{
        "EFfileStore": "stage1Flows/outbound/subfiles/",
        "EFprefix": "/stage1Flows/outbound/master/",
        "EFStartAfter": "stage1Flows/outbound/master",
        "outputTable": "ref_dataflows_outbound",
        "EFSuffix": ".uff"
        },
        "inbound":{
        "EFfileStore": "stage1Flows/inbound/subfiles/",
        "EFprefix": "/stage1Flows/inbound/master/",
        "EFStartAfter": "stage1Flows/inbound/master",
        "outputTable": "ref_dataflows_inbound",
        "EFSuffix": ".usr"
        }
    },

    "glue_ensek_job_name": "_process_ref_tables_uat",
    "glue_d18_job_name": "_process_ref_tables_uat",
    "glue_direct_debits_job_name": "_process_ref_tables_uat",
    "glue_registrations_meterpoints_status_job_name": "_process_ref_tables_uat",
    "glue_meterpoints_status_job_name": "_process_ref_tables_uat",
    "glue_zendesk_job_name": "_process_ref_tables_uat",
    "glue_historicalweather_jobname": "_process_ref_tables_uat",
    "glue_eac_aq_job_name": "_process_ref_tables_uat",
    "glue_igl_ind_eac_aq_job_name": "_process_ref_tables_uat",
    "glue_epc_job_name": "_process_ref_tables_uat",
    "glue_land_registry_job_name": "_process_ref_tables_uat",
    "glue_alp_job_name": "_process_ref_tables_uat",
    "glue_customerDB_job_name": "_process_ref_customerdb_uat",
    "glue_staging_job_name": "process_staging_files_uat",
    "glue_tado_efficiency_job_name": "_process_ref_tables_uat",
    "glue_daily_sales_job_name": "_process_ref_tables_uat",
    "glue_internal_estimates_job_name":"_process_ref_tables_uat",
    "glue_annual_statements_job_name": "_process_ref_tables_uat",
    "glue_tariff_history_job_name": "_process_ref_tables_uat",
    "glue_cons_accu_job_name": "_process_ref_tables_uat",
    "glue_internal_readings_nosi_job_name": "_process_ref_tables_uat",
    "glue_nrl_job_name": "_process_ref_tables_uat",
    "glue_estimated_advance_job_name": "_process_ref_tables_uat",
    "glue_smart_meter_eligibility_job_name": "_process_ref_tables_uat",
    "glue_occupier_accounts_job_name": "_process_ref_tables_uat",
    "glue_igloo_calculated_tariffs_job_name": "_process_ref_tables_uat",
    "glue_meets_eligibility_job_name": "_process_ref_tables_uat",
    "glue_reporting_job_name": "_process_ref_tables_uat",

    "s3_finance_bucket": "igloo-data-warehouse-uat-finance",

    "s3_finance_goCardless_key": {
        "Refunds": "/go-cardless-api-refunds/",
        "Payments": "/go-cardless-api-payments/",
        "Mandates": "/go-cardless-api-mandates/",
        "Payouts": "/go-cardless-api-payouts/",
        "Clients": "/go-cardless-api-clients/",
        "Events": "/go-cardless-api-events/",
        "Subscriptions": "/go-cardless-api-subscriptions/",
        "Subscriptions-Files": "/go-cardless-api-subscriptions-files/",
        "Mandates-Files": "/go-cardless-api-mandates-files/"
    },

    "s3_finance_square_key": {
        "Payments": "/square-api-payments/"
    },
}

preprod = {
    "apis": {
        "accounts": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}"
        },
        "meterpoints": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/MeterPoints"
        },

        "meterpoints_history": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/MeterPoints?includeHistory=true"
        },

        "meterpoints_readings": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/MeterPoints/{1}/Readings"
        },

        "meterpoints_readings_billeable": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/MeterPoints/{1}/Readings?isBilleable=true"
        },

        "direct_debits": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/DirectDebits"
        },

        "direct_debits_heath_check": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/DirectDebits/HealthCheck"
        },

        "internal_estimates": {
            "api_url": "https://igloo.ignition.ensek.co.uk/api/accounts/{0}/estimatedusage"
        },

        "internal_readings": {
            "api_url": "https://igloo.ignition.ensek.co.uk/api/account/{0}/meter-readings?sortField=meterReadingDateTime&sortDirection=Descending"
        },

        "occupier_accounts": {
            "api_url": "https://igloo.ignition.ensek.co.uk/api/DataSources/GetData?name=OccupierAccounts"
        },

        "account_status": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/SupplyStatus"
        },

        "elec_status": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/Processes/Registrations/Elec"
        },

        "gas_status": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/Processes/Registrations/Gas"
        },

        "elec_mp_status": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/Processes/Registrations/Elec?meterpointnumber={1}"
        },

        "gas_mp_status": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/Processes/Registrations/Gas?meterpointnumber={1}"
        },

        "tariff_history": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/TariffsWithHistory"
        },

        "live_balances": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/LiveBalances"
        },

        "live_balances_with_detail": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/LiveBalancesWithDetail"
        },
        "account_transactions": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/Transactions"
        },
        "annual_statements": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/annualStatements"
        },
        "statements": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/Statements"
        },
        "account_settings": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/AccountSettings"
        },

        "token": "Wk01QnVWVU01aWlLTiVeUWtwMUIyRU5EbCN0VTJUek01KmJJVFcyVGFaeiNtJkFpYUJwRUNNM2MzKjVHcjVvIQ==",

        "igloo_epc_certificates": {
            "api_url": "https://epc.opendatacommunities.org/api/v1/domestic/search?postcode={0}",
            "token": "am9uYXRoYW4uc3RlZWxAaWdsb28uZW5lcmd5OjZhZDU0ZGY4NzM0MmI4YmEyYzI1YTYyZTFlOGYxNTM4NTA2ZTQyMzQ="
        },
        "igloo_epc_recommendations": {
            "api_url": "https://epc.opendatacommunities.org/api/v1/domestic/recommendations/{0}",
            "token": "am9uYXRoYW4uc3RlZWxAaWdsb28uZW5lcmd5OjZhZDU0ZGY4NzM0MmI4YmEyYzI1YTYyZTFlOGYxNTM4NTA2ZTQyMzQ="
        },

        "igloo_epc_full": {
            "api_url": "https://epc.opendatacommunities.org/login-with-token?",
            "file_url": "https://epc.opendatacommunities.org/files/all-domestic-certificates.zip"
        },

        "historical_weather": {
            "api_url": "https://api.weatherbit.io/v2.0/history/hourly?postal_code={0}&start_date={1}&end_date={2}&key={3}",
            "token": "ee3b1ce4ed8c475c9919d0a024f9d265"
        },
        "historical_energy_weather": {
            "api_url": "https://api.weatherbit.io/v2.0/history/energy?postal_code={0}&start_date={1}&end_date={2}&key={3}",
            "token": "ee3b1ce4ed8c475c9919d0a024f9d265"
        },
        "land_registry": {
            "api_url": "http://landregistry.data.gov.uk/data/ppi/transaction-record",
        },
        "gas_historical": {
            "api_url": "http://marketinformation.natgrid.co.uk/MIPIws-public/public/publicwebservice.asmx",
        },
        "igloo_zendesk_user_tickets": {
            "api_url": "https://iglooenergy.zendesk.com/api/v2/users/{0}/tickets/requested.json",
        },
        "igloo_eac_batch": {
            "api_url": "https://9m73o40v3h.execute-api.eu-west-1.amazonaws.com/prod/Accounts/{0}/eac",
            "token": "quqSpLjJly3jlh79S7uUN9uE1YoPQums86o0768f"
        },
        "smart_reads_billing": {
            "api_url": "https://vpce-0fc7cde64b2850e80-lxb0i11z.execute-api.eu-west-1.vpce.amazonaws.com/staging/api/v1/meter-reads",
            "api_key": "HorYWPvBNO6ULqsw3aZozyW7vJKJowlacPhTH8I6",
            "host": "xcy0iyaa30.execute-api.eu-west-1.amazonaws.com"
        },
    },
    # All bad practice here needs to be sorted
    "s3_source_bucket": "igloo-data-warehouse-preprod",
    "s3_bucket": "igloo-data-warehouse-preprod-835569423516",
    # All bad practice here needs to be sorted
    "s3_smart_source_bucket": "igloo-data-warehouse-smart-meter-data-prod-630944350233",
    "s3_smart_bucket": "igloo-data-warehouse-smart-meter-data-preprod-835569423516",

    "s3_key": {
        "Accounts": "stage1/Accounts/",
        "MeterPoints": "stage1/MeterPoints/",
        "MeterPointsAttributes": "stage1/MeterPointsAttributes/",
        "Meters": "stage1/Meters/",
        "MetersAttributes": "stage1/MetersAttributes/",
        "Registers": "stage1/Registers/",
        "RegistersAttributes": "stage1/RegistersAttributes/",
        "Readings": "stage1/Readings/",
        "ReadingsInternal": "stage1/ReadingsInternal/",
        "AccountStatus": "stage1/AccountStatus/",
        "RegistrationsElec": "stage1/RegistrationsElec/",
        "RegistrationsGas": "stage1/RegistrationsGas/",
        "RegistrationsElecMeterpoint": "stage1/RegistrationsElecMeterpoint/",
        "RegistrationsGasMeterpoint": "stage1/RegistrationsGasMeterpoint/",
        "EstimatesElecInternal": "stage1/EstimatesElecInternal/",
        "EstimatesGasInternal": "stage1/EstimatesGasInternal/",
        "TariffHistory": "stage1/TariffHistory/",
        "TariffHistoryElecStandCharge": "stage1/TariffHistoryElecStandCharge/",
        "TariffHistoryElecUnitRates": "stage1/TariffHistoryElecUnitRates/",
        "TariffHistoryGasStandCharge": "stage1/TariffHistoryGasStandCharge/",
        "TariffHistoryGasUnitRates": "stage1/TariffHistoryGasUnitRates/",
        "DirectDebit": "stage1/DirectDebit/",
        "DirectDebitHealthCheck": "stage1/DirectDebitHealthCheck/",
        "ReadingsBilleable": "stage1/ReadingsBilleable/",
        "LiveBalances": "stage1/LiveBalances/",
        "LiveBalancesWithDetail": "stage1/LiveBalancesWithDetail/",
        "AccountTransactions": "stage1/AccountTransactions/",
        "MeterPointsHistory": "stage1/MeterPointsHistory/",
        "MeterPointsAttributesHistory": "stage1/MeterPointsAttributesHistory/",
        "MetersHistory": "stage1/MetersHistory/",
        "MetersAttributesHistory": "stage1/MetersAttributesHistory/",
        "RegistersHistory": "stage1/RegistersHistory/",
        "RegistersAttributesHistory": "stage1/RegistersAttributesHistory/",
        "ReadingsHistory": "stage1/ReadingsHistory/",
        "ReadingsBilleableHistory": "stage1/ReadingsBilleableHistory/",
        "AnnualStatements": "stage1/AnnualStatements/",
        "Statements": "stage1/Statements/",
        "AccountSettings": "stage1/AccountSettings/",
        "OccupierAccounts": "stage2/stage2_OccupierAccounts/"
    },


    "s3_d18_key": {
        "D18Raw": "stage1/D18/D18Raw/",
        "D18BPP": "stage1/D18/D18BPP/",
        "D18PPC": "stage1/D18/D18PPC/",
        "D18_SFTP": "D18",
        "D18Archive": "stage1/D18/D18Archive/",
        "D18Suffix": ".flw"
    },

    "s3_epc_key": {
        "EPCCertificates": "stage1/EPC/EPCCertificates/",
        "EPCRecommendations": "stage1/EPC/EPCRecommendations/"
    },

    "s3_epc_full_key": {
        "EPCFullDownload_path": "stage1/EPC_Full/all-domestic-certificates.zip",
        "EPCFullExtract_path": "stage1/EPC_Full/EPC_Full_Extract",
        "EPCFullCertificates": "stage1/EPC_Full/EPCCertificates/",
        "EPCFullRecommendations": "stage1/EPC_Full/EPCRecommendations/"
    },

    "s3_zendesk_igloo_key": {
        "ZendeskUserTickets": "stage1/Zendesk/UserTickets/"
    },

    "s3_weather_key": {
        "HistoricalWeather": "stage1/HistoricalWeather/"
    },
    "s3_alp_wcf": {
        "AlpWCF": "stage1/ALP/AlpWCF/"
    },
    "s3_alp_cv": {
        "AlpCV": "stage1/ALP/AlpCV/"
    },

    "s3_land_reg_key": {
        "LandRegistry": "stage1/LandRegistry/"
    },
    "s3_nrl_key": {
        "Raw": "stage1/ReadingsNRL/NRLRaw/",
        "NRL": "stage1/ReadingsNRL/NRL/",
        "SFTP": "CNGexport/NRL",
        "Suffix": ".flw"
    },
    "s3_nosi_key": {
        "Raw": "stage1/ReadingsNOSIGas/",
        "SFTP": "NOSI"
    },

    "glue_ensek_job_name": "_process_ref_tables_preprod",
    "glue_d18_job_name": "_process_ref_tables_preprod",
    "glue_direct_debits_job_name": "_process_ref_tables_preprod",
    "glue_registrations_meterpoints_status_job_name": "_process_ref_tables_preprod",
    "glue_meterpoints_status_job_name": "_process_ref_tables_preprod",
    "glue_zendesk_job_name": "_process_ref_tables_preprod",
    "glue_historicalweather_jobname": "_process_ref_tables_preprod",
    "glue_eac_aq_job_name": "_process_ref_tables_preprod",
    "glue_igl_ind_eac_aq_job_name": "_process_ref_tables_preprod",
    "glue_epc_job_name": "_process_ref_tables_preprod",
    "glue_land_registry_job_name": "_process_ref_tables_preprod",
    "glue_alp_job_name": "_process_ref_tables_preprod",
    "glue_customerDB_job_name": "_process_ref_customerdb_preprod",
    "glue_staging_job_name": "process_staging_files_preprod",
    "glue_tado_efficiency_job_name": "_process_ref_tables_preprod",
    "glue_daily_sales_job_name": "_process_ref_tables_preprod",
    "glue_internal_estimates_job_name":"_process_ref_tables_preprod",
    "glue_annual_statements_job_name": "_process_ref_tables_preprod",
    "glue_tariff_history_job_name": "_process_ref_tables_preprod",
    "glue_cons_accu_job_name": "_process_ref_tables_preprod",
    "glue_internal_readings_nosi_job_name": "_process_ref_tables_preprod",
    "glue_nrl_job_name": "_process_ref_tables_preprod",
    "glue_estimated_advance_job_name": "_process_ref_tables_preprod",
    "glue_smart_meter_eligibility_job_name": "_process_ref_tables_preprod",
    "glue_occupier_accounts_job_name": "_process_ref_tables_preprod",
    "glue_igloo_calculated_tariffs_job_name": "_process_ref_tables_preprod",
    "glue_meets_eligibility_job_name": "_process_ref_tables_preprod",
    "glue_reporting_job_name": "_process_ref_tables_preprod",


}


prod = {
    "apis": {
        "accounts": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}"
        },
        "meterpoints": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/MeterPoints?includeHistory=true"
        },

        "meterpoints_history": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/MeterPoints?includeHistory=true"
        },

        "meterpoints_readings": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/MeterPoints/{1}/Readings"
        },

        "meterpoints_readings_billeable": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/MeterPoints/{1}/Readings?isBilleable=true"
        },

        "direct_debits": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/DirectDebits"
        },

        "direct_debits_heath_check": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/DirectDebits/HealthCheck"
        },

        "internal_estimates": {
            "api_url": "https://igloo.ignition.ensek.co.uk/api/accounts/{0}/estimatedusage"
        },

        "internal_readings": {
            "api_url": "https://igloo.ignition.ensek.co.uk/api/account/{0}/meter-readings?sortField=meterReadingDateTime&sortDirection=Descending"
        },

        "occupier_accounts": {
            "api_url": "https://igloo.ignition.ensek.co.uk/api/DataSources/GetData?name=OccupierAccounts"
        },

        "internal_psr": {
            "api_url": "https://igloo.ignition.ensek.co.uk/api/GetPSRByAccount/{0}"
        },
        "account_status": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/SupplyStatus"
        },
        "elec_mp_status": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/Processes/Registrations/Elec?meterpointnumber={1}"
        },

        "gas_mp_status": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/Processes/Registrations/Gas?meterpointnumber={1}"
        },

        "elec_status": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/Processes/Registrations/Elec"
        },

        "gas_status": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/Processes/Registrations/Gas"
        },

        "tariff_history": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/TariffsWithHistory"
        },

        "live_balances": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/LiveBalances"
        },
        "live_balances_with_detail": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/LiveBalancesWithDetail"
        },
        "account_transactions": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/Transactions"
        },
        "annual_statements": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/annualStatements"
        },
        "statements": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/Statements"
        },
        "account_settings": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/AccountSettings"
        },

        "token": "Wk01QnVWVU01aWlLTiVeUWtwMUIyRU5EbCN0VTJUek01KmJJVFcyVGFaeiNtJkFpYUJwRUNNM2MzKjVHcjVvIQ==",

        "igloo_epc_certificates": {
            "api_url": "https://epc.opendatacommunities.org/api/v1/domestic/search?postcode={0}",
            "token": "am9uYXRoYW4uc3RlZWxAaWdsb28uZW5lcmd5OjZhZDU0ZGY4NzM0MmI4YmEyYzI1YTYyZTFlOGYxNTM4NTA2ZTQyMzQ="
        },
        "igloo_epc_recommendations": {
            "api_url": "https://epc.opendatacommunities.org/api/v1/domestic/recommendations/{0}",
            "token": "am9uYXRoYW4uc3RlZWxAaWdsb28uZW5lcmd5OjZhZDU0ZGY4NzM0MmI4YmEyYzI1YTYyZTFlOGYxNTM4NTA2ZTQyMzQ="
        },

        "igloo_epc_full": {
            "api_url": "https://epc.opendatacommunities.org/login-with-token?",
            "file_url": "https://epc.opendatacommunities.org/files/all-domestic-certificates.zip"
        },

        "historical_weather": {
            "api_url": "https://api.weatherbit.io/v2.0/history/hourly?postal_code={0}&start_date={1}&end_date={2}&key={3}",
            "token": "ee3b1ce4ed8c475c9919d0a024f9d265"
        },
        "historical_energy_weather": {
            "api_url": "https://api.weatherbit.io/v2.0/history/energy?postal_code={0}&start_date={1}&end_date={2}&key={3}",
            "token": "ee3b1ce4ed8c475c9919d0a024f9d265"
        },
        "land_registry": {
            "api_url": "http://landregistry.data.gov.uk/data/ppi/transaction-record",
        },
        "gas_historical": {
            "api_url": "http://marketinformation.natgrid.co.uk/MIPIws-public/public/publicwebservice.asmx",
        },
        "igloo_zendesk_historical": {
            "api_url": "https://iglooenergy.zendesk.com/api/v2/users/{0}/tickets/requested.json",
        }
    },

    "s3_bucket": "igloo-data-warehouse-prod",

    "s3_key": {
        "Accounts": "stage1/Accounts/",
        "MeterPoints": "stage1/MeterPoints/",
        "MeterPointsAttributes": "stage1/MeterPointsAttributes/",
        "Meters": "stage1/Meters/",
        "MetersAttributes": "stage1/MetersAttributes/",
        "Registers": "stage1/Registers/",
        "RegistersAttributes": "stage1/RegistersAttributes/",
        "Readings": "stage1/Readings/",
        "ReadingsInternal": "stage1/ReadingsInternal/",
        "AccountStatus": "stage1/AccountStatus/",
        "RegistrationsElec": "stage1/RegistrationsElec/",
        "RegistrationsGas": "stage1/RegistrationsGas/",
        "RegistrationsElecMeterpoint": "stage1/RegistrationsElecMeterpoint/",
        "RegistrationsGasMeterpoint": "stage1/RegistrationsGasMeterpoint/",
        "EstimatesElecInternal": "stage1/EstimatesElecInternal/",
        "EstimatesGasInternal": "stage1/EstimatesGasInternal/",
        "TariffHistory": "stage1/TariffHistory/",
        "TariffHistoryElecStandCharge": "stage1/TariffHistoryElecStandCharge/",
        "TariffHistoryElecUnitRates": "stage1/TariffHistoryElecUnitRates/",
        "TariffHistoryGasStandCharge": "stage1/TariffHistoryGasStandCharge/",
        "TariffHistoryGasUnitRates": "stage1/TariffHistoryGasUnitRates/",
        "DirectDebit": "stage1/DirectDebit/",
        "DirectDebitHealthCheck": "stage1/DirectDebitHealthCheck/",
        "ReadingsBilleable": "stage1/ReadingsBilleable/",
        "LiveBalances": "stage1/LiveBalances/",
        "LiveBalancesWithDetail": "stage1/LiveBalancesWithDetail/",
        "AccountTransactions": "stage1/AccountTransactions/",
        "MeterPointsHistory": "stage1/MeterPointsHistory/",
        "MeterPointsAttributesHistory": "stage1/MeterPointsAttributesHistory/",
        "MetersHistory": "stage1/MetersHistory/",
        "MetersAttributesHistory": "stage1/MetersAttributesHistory/",
        "RegistersHistory": "stage1/RegistersHistory/",
        "RegistersAttributesHistory": "stage1/RegistersAttributesHistory/",
        "ReadingsHistory": "stage1/ReadingsHistory/",
        "ReadingsBilleableHistory": "stage1/ReadingsBilleableHistory/",
        "AnnualStatements": "stage1/AnnualStatements/",
        "Statements": "stage1/Statements/",
        "AccountSettings": "stage1/AccountSettings/",
        "OccupierAccounts": "stage2/stage2_OccupierAccounts/"

    },

    "s3_d18_key": {
        "D18Raw": "stage1/D18/D18Raw/",
        "D18BPP": "stage1/D18/D18BPP/",
        "D18PPC": "stage1/D18/D18PPC/",
        "D18_SFTP": "D18",
        "D18Archive": "stage1/D18/D18Archive/",
        "D18Suffix": ".flw"
    },

    "s3_epc_key": {
        "EPCCertificates": "stage1/EPC/EPCCertificates/",
        "EPCRecommendations": "stage1/EPC/EPCRecommendations/"
    },

    "s3_epc_full_key": {
        "EPCFullDownload_path": "stage1/EPC_Full/all-domestic-certificates.zip",
        "EPCFullExtract_path": "stage1/EPC_Full/EPC_Full_Extract",
        "EPCFullCertificates": "stage1/EPC_Full/EPCCertificates/",
        "EPCFullRecommendations": "stage1/EPC_Full/EPCRecommendations/"
    },

    "s3_zendesk_igloo_key": {
        "ZendeskUserTickets": "stage1/Zendesk/UserTickets/"
    },

    "s3_weather_key": {
        "HistoricalWeather": "stage1/HistoricalWeather/"
    },
    "s3_land_reg_key": {
        "LandRegistry": "stage1/LandRegistry/"
    },
    "s3_alp_wcf": {
        "AlpWCF": "stage1/ALP/AlpWCF/"
    },
    "s3_alp_cv": {
        "AlpCV": "stage1/ALP/AlpCV/"
    },
    "s3_nrl_key": {
        "Raw": "stage1/ReadingsNRL/NRLRaw/",
        "NRL": "stage1/ReadingsNRL/NRL/",
        "SFTP": "CNGexport/NRL",
        "Suffix": ".flw"
    },
    "s3_nosi_key": {
        "Raw": "stage1/ReadingsNOSIGas/",
        "SFTP": "NOSI"
    },

    "glue_ensek_job_name": "_process_ref_tables_prod",
    "glue_d18_job_name": "_process_ref_tables_prod",
    "glue_direct_debits_job_name": "_process_ref_tables_prod",
    "glue_registrations_meterpoints_status_job_name": "_process_ref_tables_prod",
    "glue_meterpoints_status_job_name": "_process_ref_tables_prod",
    "glue_historicalweather_jobname": "_process_ref_tables_prod",
    "glue_eac_aq_job_name": "_process_ref_tables_prod",
    "glue_igl_ind_eac_aq_job_name": "_process_ref_tables_prod",
    "glue_epc_job_name": "_process_ref_tables_prod",
    "glue_land_registry_job_name": "_process_ref_tables_prod",
    "glue_alp_job_name": "_process_ref_tables_prod",
    "glue_customerDB_job_name": "_process_ref_customerdb_prod",
    "glue_staging_job_name": "process_staging_files_prod",
    "glue_tado_efficiency_job_name": "_process_ref_tables_prod",
    "glue_internal_estimates_job_name":"_process_ref_tables_prod",
    "glue_annual_statements_job_name": "_process_ref_tables_prod",
    "glue_tariff_history_job_name": "_process_ref_tables_prod",
    "glue_cons_accu_job_name": "_process_ref_tables_prod",
    "glue_internal_readings_nosi_job_name": "_process_ref_tables_prod",
    "glue_nrl_job_name": "_process_ref_tables_prod",
    "glue_daily_sales_job_name": "_process_ref_tables_prod",
    "glue_estimated_advance_job_name": "_process_ref_tables_prod",
    "glue_smart_meter_eligibility_job_name": "_process_ref_tables_prod",
    "glue_occupier_accounts_job_name": "_process_ref_tables_prod",
    "glue_igloo_calculated_tariffs_job_name": "_process_ref_tables_prod",
    "glue_meets_eligibility_job_name": "_process_ref_tables_prod",
    "glue_reporting_job_name": "_process_ref_tables_prod",

}
