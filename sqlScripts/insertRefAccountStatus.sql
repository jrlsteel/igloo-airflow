insert into ref_account_status
(select account_id, status from aws_s3_ensec_api_extracts.cdb_accountstatus);