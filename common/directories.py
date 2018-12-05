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

        "token": "Wk01QnVWVU01aWlLTiVeUWtwMUIyRU5EbCN0VTJUek01KmJJVFcyVGFaeiNtJkFpYUJwRUNNM2MzKjVHcjVvIQ=="
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

    "glue_ensek_job_name": "_process_ref_tables_uat",
    "glue_d18_job_name": "_process_ref_tables_uat",
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

        "token": "Wk01QnVWVU01aWlLTiVeUWtwMUIyRU5EbCN0VTJUek01KmJJVFcyVGFaeiNtJkFpYUJwRUNNM2MzKjVHcjVvIQ=="
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

    "glue_ensek_job_name": "_process_ref_tables_prod",
    "glue_d18_job_name": "_process_ref_tables_prod",
    "glue_customerDB_job_name": "_process_ref_customerdb_prod",
    "glue_staging_job_name": "process_staging_files_prod"

}
