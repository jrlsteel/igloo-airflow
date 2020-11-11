############ Url and token for Ensek Api's ##############
common = {
    "ensek_column_order": {
        # Ensek PA
        "ref_readings_internal": ["accountId", "billable", "hasLiveCharge", "hasRegisterAdvance", "meterId",
                                  "meterPointId", "meterPointNumber", "meterPointType", "meterReadingCreatedDate",
                                  "meterReadingDateTime", "meterReadingId", "meterReadingSourceUID",
                                  "meterReadingStatusUID", "meterReadingTypeUID", "meterSerialNumber", "readingValue",
                                  "registerId", "registerReadingId", "registerReference", "required"],
        "ref_meterpoints": ["associationStartDate", "associationEndDate", "supplyStartDate", "supplyEndDate", "isSmart",
                            "isSmartCommunicating", "meter_point_id", "meterPointNumber", "meterPointType",
                            "account_id"],
        "ref_meterpoints_attributes": ["attributes_attributeDescription", "attributes_attributeName",
                                       "attributes_attributeValue", "attributes_effectiveFromDate",
                                       "attributes_effectiveToDate", "meter_point_id", "account_id"],
        "ref_meters": ["meterSerialNumber", "installedDate", "removedDate", "meterId", "meter_point_id", "account_id"],
        "ref_meters_attributes": ["metersAttributes", "attributeDescription", "metersAttributes_attributeName",
                                  "metersAttributes_attributeValue", "meter_id", "meter_point_id", "account_id"],
        "ref_registers": ["registers_eacAq", "registers", "registerReference", "registers_sourceIdType",
                          "registers_tariffComponent", "registers_tpr", "registers_tprPeriodDescription", "meter_id",
                          "register_id", "meter_point_id", "account_id"],
        "ref_registers_attributes": ["registersAttributes_attributeDescription", "registersAttributes_attributeName",
                                     "registersAttributes_attributeValue", "meter_id", "register_id", "meter_point_id",
                                     "account_id"],
        # Ensek Non PA

        "annual_statements": ['AccountId', 'Amount', 'CreatedDate',
                              'EnergyType', 'From', 'StatementID', 'To', 'account_id'],

        "internal_estimates_elec": ['EffectiveFrom', 'EffectiveTo', 'EstimationValue',
                                    'IsLive', 'MPAN', 'RegisterId', 'SerialNumber', 'account_id'],
        "internal_estimates_gas": ['EffectiveFrom', 'EffectiveTo', 'EstimationValue',
                                   'IsLive', 'MPRN', 'RegisterId', 'SerialNumber', 'account_id'],
        "direct_debit": ['Amount', 'BankAccount', 'BankName',
                         'PaymentDate', 'Reference', 'SortCode', 'account_id'],
        "direct_debit_health_check": ['directDebitIsActive', 'isTopup', 'paymentAmount', 'paymentDay',
                                      'DirectDebitType', 'BankAccountIsActive', 'DirectDebitStatus',
                                      'NextAvailablePaymentDate', 'AccountName', 'Amount', 'BankName',
                                      'BankAccount', 'PaymentDate', 'Reference', 'SortCode', 'account_id'],
        "account_transactions": ['actions', 'amount', 'creationDetail_createdBy', 'creationDetail_createdDate',
                                 'currentBalance', 'id', 'isCancelled', 'method', 'sourceDate', 'statementId',
                                 'transactionType', 'transactionTypeFriendlyName', 'account_id'],
        "occupier_accounts": ['AccountID', 'COT Date', 'Current Balance', 'Days Since COT Date', 'etl_change'],
        "tariff_history": ['tariffName', 'startDate', 'endDate', 'discounts', 'tariffType', 'exitFees',
                           'account_id'],
        "tariff_history_elec_unit_rates": ['chargeableComponentUID', 'name', 'rate', 'registers', 'tariffName',
                                           'startDate', 'endDate', 'account_id'],
        "tariff_history_elec_standing_charge": ['chargeableComponentUID', 'name', 'rate', 'registers',
                                                'tariffName', 'startDate', 'endDate', 'account_id'],
        "tariff_history_gas_unit_rates": ['chargeableComponentUID', 'name', 'rate', 'registers', 'tariffName',
                                          'startDate', 'endDate', 'account_id'],
        "tariff_history_gas_standing_charge": ['chargeableComponentUID', 'name', 'rate', 'registers', 'tariffName',
                                               'startDate', 'endDate', 'account_id'],
        "registrations_elec_meterpoint": ['account_id', 'meter_point_id', 'meterpointnumber', 'status'],
        "registrations_gas_meterpoint": ['account_id', 'meter_point_id', 'meterpointnumber', 'status'],
        "account_settings": ['AccountID', 'BillDayOfMonth', 'BillFrequencyMonths', 'NextBillDate', 'NextBillDay',
                             'NextBillMonth', 'NextBillYear', 'SendEmail', 'SendPost', 'TopupWithEstimates',
                             'account_id'],
    },
    "alp_column_order": {
        "cv": ['name', 'applicable_at', 'applicable_for', 'value'],
        "wcf": ['name', 'applicable_at', 'applicable_for', 'value'],
    },
    "weather_column_order": {
        "historical_weather": ['azimuth', 'clouds', 'datetime', 'dewpt', 'dhi', 'dni', 'elev_angle', 'ghi', 'h_angle',
                               'pod', 'precip', 'pres', 'rh', 'slp', 'snow', 'solar_rad', 'temp', 'timestamp_local',
                               'timestamp_utc', 'ts', 'uv', 'vis', 'weather', 'wind_dir', 'wind_spd', 'timezone',
                               'state_code', 'country_code', 'lat', 'lon', 'city_name', 'station_id', 'city_id', 
                               'sources', 'postcode'],
    },
    "land_registry_column_order": {
        "land_registry": ['transactionDate', 'newBuild', 'pricePaid', 'transactionId', 'propertyType', 'recordStatus',
                          'transactionCategory', 'uprn', 'id']
    },
    "epc_column_order": {
        "epc_certificates": ['address', 'address1', 'address2', 'address3', 'building_reference_number', 'built_form',
                             'certificate_hash', 'co2_emiss_curr_per_floor_area', 'co2_emissions_current',
                             'co2_emissions_potential', 'constituency', 'constituency_label', 'county',
                             'current_energy_efficiency', 'current_energy_rating', 'energy_consumption_current',
                             'energy_consumption_potential', 'energy_tariff', 'environment_impact_current',
                             'environment_impact_potential', 'extension_count', 'flat_storey_count', 'flat_top_storey',
                             'floor_description', 'floor_energy_eff', 'floor_env_eff', 'floor_height', 'floor_level',
                             'glazed_area', 'glazed_type', 'heat_loss_corridoor', 'heating_cost_current',
                             'heating_cost_potential', 'hot_water_cost_current', 'hot_water_cost_potential',
                             'hot_water_energy_eff', 'hot_water_env_eff', 'hotwater_description', 'inspection_date',
                             'lighting_cost_current', 'lighting_cost_potential', 'lighting_description',
                             'lighting_energy_eff', 'lighting_env_eff', 'lmk_key', 'local_authority',
                             'local_authority_label', 'lodgement_date', 'low_energy_lighting', 'main_fuel',
                             'main_heating_controls', 'mainheat_description', 'mainheat_energy_eff',
                             'mainheat_env_eff', 'mainheatc_energy_eff', 'mainheatc_env_eff', 
                             'mainheatcont_description', 'mains_gas_flag', 'mechanical_ventilation', 
                             'multi_glaze_proportion', 'number_habitable_rooms', 'number_heated_rooms', 
                             'number_open_fireplaces', 'photo_supply', 'postcode', 'potential_energy_efficiency', 
                             'potential_energy_rating', 'property_type', 'roof_description', 'roof_energy_eff', 
                             'roof_env_eff', 'secondheat_description', 'sheating_energy_eff', 'sheating_env_eff', 
                             'solar_water_heating_flag', 'total_floor_area', 'transaction_type', 
                             'unheated_corridor_length', 'walls_description', 'walls_energy_eff', 'walls_env_eff', 
                             'wind_turbine_count', 'windows_description', 'windows_energy_eff', 'windows_env_eff']
    },
    "go_cardless_column_order": {
        "customers": ['client_id','created_at','email','given_name','family_name','company_name','country_code','EnsekID'],
        "events": ['id','created_at','resource_type','action','customer_notifications','cause','description','origin','reason_code','scheme','will_attempt_retry','mandate','new_customer_bank_account','new_mandate','organisation','parent_event','payment','payout','previous_customer_bank_account','refund','subscription'],
        "payouts": ['payout_id','amount','arrival_date','created_at','deducted_fees','payout_type','reference','status','creditor','creditor_bank_account'],
    }
}

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
    "s3_source_bucket": "igloo-data-warehouse-preprod",
    "s3_bucket": "igloo-data-warehouse-dev-555393537168",
    # All bad practice here needs to be sorted
    "s3_smart_source_bucket": "igloo-data-warehouse-smart-meter-data-prod-630944350233",

    "s3_temp_hh_uat_bucket": "igloo-data-warehouse-uat",
    "s3_smart_bucket": "igloo-data-warehouse-smart-meter-data-dev-555393537168",


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
                "stage2": "stage2/stage2_WeatherForecast48hr/forecast_issued={0}/"
            },
            "daily": {
                "stage1": "stage1/DailyForecast/forecast_issued={0}/",
                "stage2": "stage2/stage2_WeatherForecast16day/forecast_issued={0}/"
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
        "HistoricalWeather": "stage1/HistoricalWeather/",
        "forecast_weather": {
            "hourly": {
                "stage1": "stage1/HourlyForecast/forecast_issued={0}/",
                "stage2": "stage2/stage2_WeatherForecast48hr/forecast_issued={0}/"
            },
            "daily": {
                "stage1": "stage1/DailyForecast/forecast_issued={0}/",
                "stage2": "stage2/stage2_WeatherForecast16day/forecast_issued={0}/"
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
    "s3_ensekflow_key": {
        "outbound": {
            "EFfileStore": "stage1Flows/outbound/subfiles/",
            "EFprefix": "/stage1Flows/outbound/master/",
            "EFStartAfter": "stage1Flows/outbound/master",
            "outputTable": "ref_dataflows_outbound",
            "EFSuffix": ".uff"
        },
        "inbound": {
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
    "glue_internal_estimates_job_name": "_process_ref_tables_uat",
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
        "Mandates-Files": "/go-cardless-api-mandates-files/",
        "Payments-Files": "/go-cardless-api-payments-files/",
        "Refunds-Files": "/go-cardless-api-refunds-files/"
    },

    "s3_finance_square_key": {
        "Payments": "/square-api-payments/"
    },

    "master_sources": {
        "go_cardless": "prod"
    }
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
    "s3_source_bucket": "igloo-data-warehouse-preprod",
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
    
    "master_sources": {
        "go_cardless": "prod"
    }
}

newprod = {
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
    "s3_source_bucket": "igloo-data-warehouse-prod",
    "s3_bucket": "igloo-data-warehouse-prod-630944350233",
    # All bad practice here needs to be sorted
    "s3_temp_hh_uat_bucket": "none",
    "s3_smart_source_bucket": "none",
    "s3_smart_bucket": "none",

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
                "stage2": "stage2/stage2_WeatherForecast48hr/forecast_issued={0}/"
            },
            "daily": {
                "stage1": "stage1/DailyForecast/forecast_issued={0}/",
                "stage2": "stage2/stage2_WeatherForecast16day/forecast_issued={0}/"
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
    "glue_tado_efficiency_job_name": "_process_ref_tables",
    "glue_daily_sales_job_name": "_process_ref_tables",
    "glue_internal_estimates_job_name":"_process_ref_tables",
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

    "s3_finance_bucket": "igloo-data-warehouse-finance-prod-630944350233",

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

    "master_sources": {
        "go_cardless": "prod"
    }
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
                "redshift_table": "ref_weather_forecast_daily"
            }
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
        },
        "smart_reads_billing": {
            "api_url": "https://vpce-0aef8e00f3f1643c5-ew9hc5n2.execute-api.eu-west-1.vpce.amazonaws.com/prod/api/v1/meter-reads",
            "api_key": "17EXj0FzF89jK3nTdZ5JG2ZSsctlxhJr463tFB2g",
            "host": "vduzli2ylh.execute-api.eu-west-1.amazonaws.com",
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
        "HistoricalWeather": "stage1/HistoricalWeather/",
        "forecast_weather": {
            "hourly": {
                "stage1": "stage1/HourlyForecast/forecast_issued={0}/",
                "stage2": "stage2/stage2_WeatherForecast48hr/forecast_issued={0}/"
            },
            "daily": {
                "stage1": "stage1/DailyForecast/forecast_issued={0}/",
                "stage2": "stage2/stage2_WeatherForecast16day/forecast_issued={0}/"
            }
        }
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
    "glue_internal_estimates_job_name": "_process_ref_tables_prod",
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

    "s3_finance_bucket": "igloo-data-warehouse-prod-finance",

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

    "master_sources": {
        "go_cardless": "prod"
    }
}
