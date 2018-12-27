############ Url and token for Ensek Api's ##############
uat = {
    "apis": {
        "meterpoints": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/MeterPoints"
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

        "internal_estimates": {
            "api_url": "https://igloo.ignition.ensek.co.uk/api/accounts/{0}/estimatedusage"
        },

        "internal_readings": {
            "api_url": "https://igloo.ignition.ensek.co.uk/api/account/{0}/meter-readings?sortField=meterReadingDateTime&sortDirection=Descending"
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

        "tariff_history": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/TariffsWithHistory"
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
        }
    },

    "s3_bucket": "igloo-data-warehouse-uat",

    "s3_key": {
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
        "EstimatesElecInternal": "stage1/EstimatesElecInternal/",
        "EstimatesGasInternal": "stage1/EstimatesGasInternal/",
        "TariffHistory": "stage1/TariffHistory/",
        "TariffHistoryElecStandCharge": "stage1/TariffHistoryElecStandCharge/",
        "TariffHistoryElecUnitRates": "stage1/TariffHistoryElecUnitRates/",
        "TariffHistoryGasStandCharge": "stage1/TariffHistoryGasStandCharge/",
        "TariffHistoryGasUnitRates": "stage1/TariffHistoryGasUnitRates/",
        "DirectDebit": "stage1/DirectDebit/",
        "ReadingsBilleable": "stage1/ReadingsBilleable/"
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

    "glue_ensek_job_name": "_process_ref_tables_uat",
    "glue_d18_job_name": "_process_ref_tables_uat",
    "glue_historicalweather_jobname": "_process_ref_tables_uat",
    "glue_epc_job_name": "_process_ref_tables_uat",
    "glue_land_registry_job_name": "_process_ref_tables_uat",
    "glue_customerDB_job_name": "_process_ref_customerdb_uat",
    "glue_staging_job_name": "process_staging_files_uat"

}

prod = {
    "apis": {
        "meterpoints": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/MeterPoints"
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

        "internal_estimates": {
            "api_url": "https://igloo.ignition.ensek.co.uk/api/accounts/{0}/estimatedusage"
        },

        "internal_readings": {
            "api_url": "https://igloo.ignition.ensek.co.uk/api/account/{0}/meter-readings?sortField=meterReadingDateTime&sortDirection=Descending"
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

        "tariff_history": {
            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/TariffsWithHistory"
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
        }
    },

    "s3_bucket": "igloo-data-warehouse-prod",

    "s3_key": {
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
        "EstimatesElecInternal": "stage1/EstimatesElecInternal/",
        "EstimatesGasInternal": "stage1/EstimatesGasInternal/",
        "TariffHistory": "stage1/TariffHistory/",
        "TariffHistoryElecStandCharge": "stage1/TariffHistoryElecStandCharge/",
        "TariffHistoryElecUnitRates": "stage1/TariffHistoryElecUnitRates/",
        "TariffHistoryGasStandCharge": "stage1/TariffHistoryGasStandCharge/",
        "TariffHistoryGasUnitRates": "stage1/TariffHistoryGasUnitRates/",
        "DirectDebit": "stage1/DirectDebit/",
        "ReadingsBilleable": "stage1/ReadingsBilleable/"
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

    "s3_weather_key": {
        "HistoricalWeather": "stage1/HistoricalWeather/"
    },
    "s3_land_reg_key": {
        "LandRegistry": "stage1/LandRegistry"
    },
    "s3_alp_wcf": {
        "AlpWCF": "stage1/ALP/AlpWCF/"
    },
    "s3_alp_cv": {
        "AlpCV": "stage1/ALP/AlpCV/"
    },

    "glue_ensek_job_name": "_process_ref_tables_prod",
    "glue_d18_job_name": "_process_ref_tables_prod",
    "glue_historicalweather_jobname": "_process_ref_tables_uat",
    "glue_epc_job_name": "_process_ref_tables_prod",
    "glue_land_registry_job_name": "_process_ref_tables_prod",
    "glue_customerDB_job_name": "_process_ref_customerdb_prod",
    "glue_staging_job_name": "process_staging_files_prod"

}
