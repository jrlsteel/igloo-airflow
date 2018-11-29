
############ Url and token for Ensek Api's ##############
uat = {
    "apis": {
                        "meterpoints": {
                            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/MeterPoints",
                        },

                        "meterpoints_readings": {
                            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/MeterPoints/{1}/Readings"
                        },

                        "meterpoints_readings_billeable": {
                            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/MeterPoints/{1}/Readings?isBilleable=true"
                        },

                        "direct_debits": {
                            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/DirectDebits",
                        },

                        "internal_estimates": {
                            "api_url": "https://igloo.ignition.ensek.co.uk/api/accounts/{0}/estimatedusage",
                        },

                        "internal_readings": {
                            "api_url": "https://igloo.ignition.ensek.co.uk/api/account/{0}/meter-readings?sortField=meterReadingDateTime&sortDirection=Descending"
                        },

                        "account_status": {
                            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/SupplyStatus",
                        },

                        "elec_status": {
                            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/Processes/Registrations/Elec",
                        },

                        "gas_status": {
                            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/Processes/Registrations/Gas",
                        },

                        "tariff_history": {
                            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/TariffsWithHistory",
                        },

                        "account_messages": {
                            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/Messages",
                        },
                        "token": "Wk01QnVWVU01aWlLTiVeUWtwMUIyRU5EbCN0VTJUek01KmJJVFcyVGFaeiNtJkFpYUJwRUNNM2MzKjVHcjVvIQ=="
    },

    # "apis": {
    #                     "meterpoints": {
    #                         "api_url": "https://api.uat.igloo.ignition.ensek.co.uk/Accounts/{0}/MeterPoints",
    #                     },
    #
    #                     "direct_debits": {
    #                         "api_url": "https://api.uat.igloo.ignition.ensek.co.uk/Accounts/{0}/DirectDebits",
    #                     },
    #
    #                     "internal_estimates": {
    #                         "api_url": "https://igloo.uat.ignition.ensek.co.uk/api/accounts/{0}/estimatedusage",
    #                     },
    #
    #                     "internal_readings": {
    #                         "api_url": "https://igloo.uat.ignition.ensek.co.uk/api/account/{0}/meter-readings?sortField=meterReadingDateTime&sortDirection=Descending"
    #                     },
    #
    #                     "account_status": {
    #                         "api_url": "https://api.uat.igloo.ignition.ensek.co.uk/Accounts/{0}/SupplyStatus",
    #                     },
    #
    #                     "elec_status": {
    #                         "api_url": "https://api.uat.igloo.ignition.ensek.co.uk/Accounts/{0}/Processes/Registrations/Elec",
    #                     },
    #
    #                     "gas_status": {
    #                         "api_url": "https://api.uat.igloo.ignition.ensek.co.uk/Accounts/{0}/Processes/Registrations/Gas",
    #                     },
    #
    #                     "tariff_history": {
    #                         "api_url": "https://api.uat.igloo.ignition.ensek.co.uk/Accounts/{0}/TariffsWithHistory",
    #                     },
    #
    #                     "token": "QUtYcjkhJXkmVmVlUEJwNnAxJm1Md1kjU2RaTkRKcnZGVzROdHRiI0deS0EzYVpFS3ZYdCFQSEs0elNrMmxDdQ=="
    # },

    "s3_bucket": "igloo-uat-bucket",

    "s3_key": {
            "MeterPoints": "ensek-meterpoints/MeterPoints/",
            "MeterPointsAttributes": "ensek-meterpoints/Attributes/",
            "Meters": "ensek-meterpoints/Meters/",
            "MetersAttributes": "ensek-meterpoints/MetersAttributes/",
            "Registers": "ensek-meterpoints/Registers/",
            "RegistersAttributes": "ensek-meterpoints/RegistersAttributes/",
            "Readings": "ensek-meterpoints/Readings/",
            "ReadingsInternal": "ensek-meterpoints/ReadingsInternal/",
            "AccountStatus": "ensek-meterpoints/AccountStatus/",
            "RegistrationsElec": "ensek-meterpoints/RegistrationsElec/",
            "RegistrationsGas": "ensek-meterpoints/RegistrationsGas/",
            "EstimatesElecInternal": "ensek-meterpoints/EstimatesElecInternal/",
            "EstimatesGasInternal": "ensek-meterpoints/EstimatesGasInternal/",
            "TariffHistory": "ensek-meterpoints/TariffHistory/",
            "TariffHistoryElecStandCharge": "ensek-meterpoints/TariffHistoryElecStandCharge/",
            "TariffHistoryElecUnitRates": "ensek-meterpoints/TariffHistoryElecUnitRates/",
            "TariffHistoryGasStandCharge": "ensek-meterpoints/TariffHistoryGasStandCharge/",
            "TariffHistoryGasUnitRates": "ensek-meterpoints/TariffHistoryGasUnitRates/",
            "AccountMessages": "ensek-meterpoints/AccountMessages/",
            "DirectDebit": "ensek-meterpoints/DirectDebit/",
            "ReadingsBilleable": "ensek-meterpoints/ReadingsBilleable/"
    }
}

prod = {
        "apis": {
                        "meterpoints": {
                            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/MeterPoints",
                        },

                        "meterpoints_readings": {
                            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/MeterPoints/{1}/Readings"
                        },

                        "meterpoints_readings_billeable": {
                            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/MeterPoints/{1}/Readings?isBilleable=true"
                        },

                        "direct_debits": {
                            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/DirectDebits",
                        },

                        "internal_estimates": {
                            "api_url": "https://igloo.ignition.ensek.co.uk/api/accounts/{0}/estimatedusage",
                        },

                        "internal_readings": {
                            "api_url": "https://igloo.ignition.ensek.co.uk/api/account/{0}/meter-readings?sortField=meterReadingDateTime&sortDirection=Descending"
                        },

                        "account_status": {
                            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/SupplyStatus",
                        },

                        "elec_status": {
                            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/Processes/Registrations/Elec",
                        },

                        "gas_status": {
                            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/Processes/Registrations/Gas",
                        },

                        "tariff_history": {
                            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/TariffsWithHistory",
                        },

                        "account_messages": {
                            "api_url": "https://api.igloo.ignition.ensek.co.uk/Accounts/{0}/Messages",
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
    }

}
