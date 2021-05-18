preprod = {
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
        "forecast_weather": {
            "hourly": {
                "api_url": "https://api.weatherbit.io/v2.0/forecast/hourly?postal_code={0}&hours={1}&key={2}",
                "token": "ee3b1ce4ed8c475c9919d0a024f9d265",
                "data_keys": [
                    "wind_cdir",
                    "rh",
                    "pod",
                    "timestamp_utc",
                    "pres",
                    "solar_rad",
                    "ozone",
                    "wind_gust_spd",
                    "timestamp_local",
                    "snow_depth",
                    "clouds",
                    "ts",
                    "wind_spd",
                    "pop",
                    "wind_cdir_full",
                    "slp",
                    "dni",
                    "dewpt",
                    "snow",
                    "uv",
                    "wind_dir",
                    "clouds_hi",
                    "precip",
                    "vis",
                    "dhi",
                    "app_temp",
                    "datetime",
                    "temp",
                    "ghi",
                    "clouds_mid",
                    "clouds_low"
                ],
                "dtypes": {
                    'wind_cdir': 'object',
                    'rh': 'int64',
                    'pod': 'object',
                    'timestamp_utc': 'object',
                    'pres': 'float64',
                    'solar_rad': 'float64',
                    'ozone': 'float64',
                    'wind_gust_spd': 'float64',
                    'timestamp_local': 'object',
                    'snow_depth': 'float64',
                    'clouds': 'int64',
                    'ts': 'int64',
                    'wind_spd': 'float64',
                    'pop': 'int64',
                    'wind_cdir_full': 'object',
                    'slp': 'float64',
                    'dni': 'float64',
                    'dewpt': 'float64',
                    'snow': 'float64',
                    'uv': 'float64',
                    'wind_dir': 'int64',
                    'clouds_hi': 'int64',
                    'precip': 'float64',
                    'vis': 'float64',
                    'dhi': 'float64',
                    'app_temp': 'float64',
                    'datetime': 'object',
                    'temp': 'float64',
                    'ghi': 'float64',
                    'clouds_mid': 'int64',
                    'clouds_low': 'int64',
                    'icon': 'object',
                    'code': 'int64',
                    'description': 'object',
                    'city_name': 'object',
                    'lon': 'object',
                    'timezone': 'object',
                    'lat': 'object',
                    'country_code': 'object',
                    'state_code': 'object',
                    'outcode': 'object',
                    'etlchange': 'object'
                },
                "redshift_table": "ref_weather_forecast_hourly"
            },
            "daily": {
                "api_url": "https://api.weatherbit.io/v2.0/forecast/daily?postal_code={0}&days={1}&key={2}",
                "token": "ee3b1ce4ed8c475c9919d0a024f9d265",
                "data_keys": [
                    'moonrise_ts',
                    'wind_cdir',
                    'rh',
                    'pres',
                    'high_temp',
                    'sunset_ts',
                    'ozone',
                    'moon_phase',
                    'wind_gust_spd',
                    'snow_depth',
                    'clouds',
                    'ts',
                    'sunrise_ts',
                    'app_min_temp',
                    'wind_spd',
                    'pop',
                    'wind_cdir_full',
                    'slp',
                    'moon_phase_lunation',
                    'valid_date',
                    'app_max_temp',
                    'vis',
                    'dewpt',
                    'snow',
                    'uv',
                    'wind_dir',
                    'max_dhi',
                    'clouds_hi',
                    'precip',
                    'low_temp',
                    'max_temp',
                    'moonset_ts',
                    'datetime',
                    'temp',
                    'min_temp',
                    'clouds_mid',
                    'clouds_low'
                ],
                "dtypes": {
                    'moonrise_ts': 'int64',
                    'wind_cdir': 'object',
                    'rh': 'int64',
                    'pres': 'float64',
                    'high_temp': 'float64',
                    'sunset_ts': 'int64',
                    'ozone': 'float64',
                    'moon_phase': 'float64',
                    'wind_gust_spd': 'float64',
                    'snow_depth': 'float64',
                    'clouds': 'int64',
                    'ts': 'int64',
                    'sunrise_ts': 'int64',
                    'app_min_temp': 'float64',
                    'wind_spd': 'float64',
                    'pop': 'int64',
                    'wind_cdir_full': 'object',
                    'slp': 'float64',
                    'moon_phase_lunation': 'float64',
                    'valid_date': 'object',
                    'app_max_temp': 'float64',
                    'vis': 'float64',
                    'dewpt': 'float64',
                    'snow': 'float64',
                    'uv': 'float64',
                    'wind_dir': 'int64',
                    'max_dhi': 'object',
                    'clouds_hi': 'int64',
                    'precip': 'float64',
                    'low_temp': 'float64',
                    'max_temp': 'float64',
                    'moonset_ts': 'int64',
                    'datetime': 'object',
                    'temp': 'float64',
                    'min_temp': 'float64',
                    'clouds_mid': 'int64',
                    'clouds_low': 'int64',
                    'icon': 'object',
                    'code': 'int64',
                    'description': 'object',
                    'city_name': 'object',
                    'lon': 'object',
                    'timezone': 'object',
                    'lat': 'object',
                    'country_code': 'object',
                    'state_code': 'object',
                    'outcode': 'object',
                    'etlchange': 'object',
                },
                "redshift_table": "ref_weather_forecast_daily"
            }
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
    "s3_source_bucket": "igloo-data-warehouse-prod-630944350233",
    "s3_bucket": "igloo-data-warehouse-preprod-835569423516",
    # All bad practice here needs to be sorted
    "s3_temp_hh_uat_bucket": "igloo-data-warehouse-uat",
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
        "HistoricalWeather": "stage1/HistoricalWeather/",
        "forecast_weather": {
            "hourly": {
                "stage1": "stage1/HourlyForecast/forecast_issued={0}/",
                "stage2": "stage2/stage2_HourlyForecast/forecast_issued={0}/"
            },
            "daily": {
                "stage1": "stage1/DailyForecast/forecast_issued={0}/",
                "stage2": "stage2/stage2_DailyForecast/forecast_issued={0}/"
            }
        }
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

    # Glue Jobs
    "glue_staging_internalreadings_job_name":"process_staging_ensek_internal_readings_files",
    "glue_ref_internalreadings_job_name":"_process_ensek_internal_readings_ref_tables",
    "glue_staging_meterpoints_job_name": "process_staging_ensek_meterpoints_files",
    "glue_ref_meterpoints_job_name": "_process_meterpoint_ref_tables",
    "glue_ensek_job_name": "_process_ref_tables",
    "glue_d18_job_name": "_process_ref_tables",
    "glue_direct_debits_job_name": "_process_ref_tables",
    "glue_registrations_meterpoints_status_job_name": "_process_ref_tables",
    "glue_meterpoints_status_job_name": "_process_ref_tables",
    "glue_zendesk_job_name": "_process_ref_tables",
    "glue_historicalweather_jobname": "_process_ref_tables",
    "glue_eac_aq_job_name": "_process_ref_tables",
    "glue_igl_ind_eac_aq_job_name": "_process_ref_tables",
    "glue_epc_job_name": "_process_ref_tables",
    "glue_land_registry_job_name": "_process_ref_tables",
    "glue_alp_job_name": "_process_ref_tables",
    "glue_customerDB_job_name": "_process_ref_customerdb",
    "glue_staging_job_name": "process_staging_files",
    "glue_staging_smart_job_name": "process_staging_smart_files",
    "glue_tado_efficiency_job_name": "_process_ref_tables",
    "glue_daily_sales_job_name": "_process_ref_tables",
    "glue_internal_estimates_job_name": "_process_ref_tables",
    "glue_annual_statements_job_name": "_process_ref_tables",
    "glue_tariff_history_job_name": "_process_ref_tables",
    "glue_cons_accu_job_name": "_process_ref_tables",
    "glue_internal_readings_nosi_job_name": "_process_ref_tables",
    "glue_nrl_job_name": "_process_ref_tables",
    "glue_estimated_advance_job_name": "_process_ref_tables",
    "glue_smart_meter_eligibility_job_name": "_process_ref_tables",
    "glue_occupier_accounts_job_name": "_process_ref_tables",
    "glue_igloo_calculated_tariffs_job_name": "_process_ref_tables",
    "glue_meets_eligibility_job_name": "_process_ref_tables",
    "glue_reporting_job_name": "_process_ref_tables",

    "s3_finance_bucket": "igloo-data-warehouse-finance-preprod-835569423516",

    "s3_finance_goCardless_key": {
        "Refunds": "/go-cardless-api-refunds/",
        "Payments": "/go-cardless-api-payments/",
        "Mandates": "/go-cardless-api-mandates/",
        "Payouts": "/go-cardless-api-payouts/",
        "Clients": "/go-cardless-api-clients/",
        "Events": "/go-cardless-api-events/",
        "Subscriptions": "/go-cardless-api-subscriptions/",
        "Subscriptions-Files": "/go-cardless-api-subscriptions-files/",
        "Mandates-Files": "/go-cardless-api-mandates-files/",
        "Payments-Files": "/go-cardless-api-payments-files/",
        "Refunds-Files": "/go-cardless-api-refunds-files/"
    },

    "s3_finance_square_key": {
        "Payments": "/square-api-payments/"
    },


    "master_sources": {
        "go_cardless": "newprod"
    }
}
