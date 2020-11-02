api_config = {
                        'max_api_calls': 36,
                        'allowed_period_in_secs': 0,
                        'total_no_of_calls': 20000,
                        'connection_timeout': 5,
                        'retry_in_secs': 2,
                        'request_rate': 1
}
igloo_api = {
    "base_url": "https://f2ea2fxjp6.execute-api.eu-west-1.amazonaws.com/uat",
    "host": "f2ea2fxjp6.execute-api.eu-west-1.amazonaws.com",
    "api_key": "",
    "aws_access_key": "AKIAJLHPPVNVOH72DKKA",
    "aws_secret_key": ""
}
multi_process = {
                        'total_ensek_processes': 6,
                        'ensek_flow_processes': 47,
                        'max_total_threads': 48
}

s3_config = {
                        'access_key': 'AKIA4FC6RSSOHKCW5DOV',
                        'secret_key': '',
                        'bucket_name': 'igloo-data-warehouse-preprod-835569423516'
}
rds_config = {
                        'host': 'prod-rds-instance-aurora-cluster.cluster-crg2xtjrpp81.eu-west-1.rds.amazonaws.com',
                        #'host': 'localhost' ,
                        'user': 'igloo_readonly',
                        'db': 'igloo',
                        'pwd': '',
                        'port': 3306
}
redshift_ssh_config = {
                        #'host': 'igloowarehouse.cqnmyrrkwt1y.eu-west-1.redshift.amazonaws.com',
                        #'host': 'igloowarehouse-dev.cli0yzw5ugsq.eu-west-1.redshift.amazonaws.com',
                        #'host': 'igloowarehouse-staging.c68wasqrd8cc.eu-west-1.redshift.amazonaws.com',
                        'host': 'localhost',
                        'user': 'igloo',
                        'db': 'igloosense-preprod',
                        'pwd': '',
                        'port': 54394
}
redshift_config = {
                        # 'host': 'igloowarehouse.cqnmyrrkwt1y.eu-west-1.redshift.amazonaws.com',
                        # 'host': 'igloowarehouse-dev.cli0yzw5ugsq.eu-west-1.redshift.amazonaws.com',
                        #'host': ' igloowarehouse-staging.c68wasqrd8cc.eu-west-1.redshift.amazonaws.com',
                        'host': 'localhost',
                        'user': 'igloo',
                        'db': 'igloosense-preprod',
                        'pwd': '',
                        'port': 54394
}
redshift_config_prod = {
                        #'host': 'igloowarehouse.cqnmyrrkwt1y.eu-west-1.redshift.amazonaws.com',
                        #'host': 'igloowarehouse-dev.cli0yzw5ugsq.eu-west-1.redshift.amazonaws.com',
                        #'host': ' igloowarehouse-staging.c68wasqrd8cc.eu-west-1.redshift.amazonaws.com',
                        'host': 'localhost',
                        'user': 'igloo',
                        'db': 'igloosense-preprod',
                        'pwd': '',
                        'port': 54394
}
test_config = {
                        "enable_manual": "N",  # If enabled, uses the account ids from test_config[account_ids]
                        "account_ids": [1831],  # run for one or list of account ids for testing
                        "enable_file":  "N",  # If enabled, uses the account ids from s3 bucket
                        "enable_db": "Y",  # If enabled, fetches the account ids RDS mysql db
                        "enable_db_max": "N",
    "WCFFolders": ['Weather Correction Factor, Allocated (EA)',
                                       'Weather Correction Factor, Allocated (EM)',
                                       'Weather Correction Factor, Allocated (NE)',
                                       'Weather Correction Factor, Allocated (NO)',
                                       'Weather Correction Factor, Allocated (NT)',
                                       'Weather Correction Factor, Allocated (NW)',
                                       'Weather Correction Factor, Allocated (SC)',
                                       'Weather Correction Factor, Allocated (SE)',
                                       'Weather Correction Factor, Allocated (SO)',
                                       'Weather Correction Factor, Allocated (SW)',
                                       'Weather Correction Factor, Allocated (WM)',
                                       'Weather Correction Factor, Allocated (WN)',
                                       'Weather Correction Factor, Allocated (WS)'],
                        "CVFolders":  ['Calorific Value, Campbeltown',
                                       'Calorific Value, LDZ(EA)',
                                       'Calorific Value, LDZ(EM)',
                                       'Calorific Value, LDZ(NE)',
                                       'Calorific Value, LDZ(NO)',
                                       'Calorific Value, LDZ(NT)',
                                       'Calorific Value, LDZ(NW)',
                                       'Calorific Value, LDZ(SC)',
                                       'Calorific Value, LDZ(SE)',
                                       'Calorific Value, LDZ(SO)',
                                       'Calorific Value, LDZ(SW)',
                                       'Calorific Value, LDZ(WM)',
                                       'Calorific Value, LDZ(WN)',
                                       'Calorific Value, LDZ(WS)'],
                        # Careful of null values as Pandas will convert type to a float
                        # "account_ids_sql_prod": "select external_id from (select s.external_id from ref_cdb_supply_contracts s union select 1830 as external_id) order by external_id",
                        "account_ids_sql_prod": "select s.external_id from ref_cdb_supply_contracts s where external_id is not null order by external_id",
                        "account_ids_sql": "select external_id from (select s.external_id from ref_cdb_supply_contracts s union select 1830 as external_id) order by external_id",  # Sql to fetch from rds db.
                        "addresses_sql" : "SELECT id, sub_building_name_number, building_name_number, dependent_thoroughfare, thoroughfare, double_dependent_locality, dependent_locality, post_town, county, postcode, uprn, created_at, updated_at FROM ref_cdb_addresses",
                        "schema_account_ids_sql_redshift": "select account_id from vw_acl_gaselec_dual",
                        "schema_account_ids_sql_rds": "select s.external_id from supply_contracts s where s.external_id is not null order by s.external_id",  # Sql to fetch from rds db.
                        "epc_postcode_sql": "SELECT left(postcode, len(postcode) - 3) + ' ' + right(postcode, 3) as postcode FROM ref_cdb_addresses  group by left(postcode, len(postcode) - 3) + ' ' + right(postcode, 3)",
                        #"epc_certificates_postcode_sql": "SELECT left(postcode, len(postcode) - 3) + ' ' + right(postcode, 3) as postcode FROM ref_cdb_addresses  group by left(postcode, len(postcode) - 3) + ' ' + right(postcode, 3)",
                        "epc_certificates_postcode_sql": "SELECT left(postcode, len(postcode) - 3) + ' ' + right(postcode, 3) as postcode FROM ref_cdb_addresses  group by left(postcode, len(postcode) - 3) + ' ' + right(postcode, 3)",
                        "epc_recommendations_lmkkey_sql": "select distinct(lmk_key) from aws_s3_stage2_extracts.stage2_epccertificates",
                        "land_registry_postcode_sql": "SELECT left(postcode, len(postcode) - 3) + ' ' + right(postcode, 3) as postcode FROM ref_cdb_addresses  group by left(postcode, len(postcode) - 3) + ' ' + right(postcode, 3)",
                        "weather_sql": "SELECT left(postcode, len(postcode) - 3) postcode FROM aws_s3_stage1_extracts.stage1_postcodesuk where group by left(postcode, len(postcode) - 3) order by left(postcode, len(postcode) - 3) asc",
                      "weather_energy_sql": "SELECT left(postcode, len(postcode) - 3) as postcode FROM ref_cdb_addresses group by left(postcode, len(postcode) - 3) order by left(postcode, len(postcode) - 3)",
                        "land_registry_address_sql": "SELECT id, sub_building_name_number, building_name_number, thoroughfare, county, postcode, uprn FROM ref_cdb_addresses ",
}
internalapi_config = {
                        "username": "sakthi.murugan@igloo.energy",
                        "password": "",
                        "grant_type": "password"
}
environment_config = {
                        "environment": "dev"
}
ensek_sftp_config = {
                        "host": "52.214.39.234",
                        "username": "MattClemow",
                        "password": ""
}
igloo_epc_full = {
                        "token": "token=95c4b4cc65cb41eb545d5fa1cd74fbb6c411b5d1&email=jonathan.steel%40igloo.energy"
}
Enseks3_ensek_outbound = {
                        'access_key': 'AKIAX4YVAEELGUTQX55Y',
                        'secret_key': '',
                        'bucket_name': 'arn:aws:s3:::igloo-elec-imiflows-outbound'
}
Enseks3_ensek_inbound = {
                        'access_key': 'AKIAX4YVAEELGUTQX55Y',
                        'secret_key': '',
                        'bucket_name': 'arn:aws:s3:::igloo-flows-dtc-inbound'
}
go_cardless = {
                        "access_token": "" ,
                        "environment": "live"
}
square = {
                        "access_token": "" ,
                        "environment": "production"
}
igloo_finance_config = {
                        'access_key': 'AKIAZFZZWYAM32Z2ZLYR',
                        'secret_key': '',
                        'bucket_name': 'igloo-data-warehouse-uat-finance'
}