insert into ref_meters
			(	account_id,
				meter_point_id,
				meter_id,
				meterserialnumber,
				installeddate,
				removeddate)
select 	met.account_id,
				met.meter_point_id,
				met.meterid,
				met.meterserialnumber,
				nullif(met.installeddate,'')::timestamp,
				nullif(met.removeddate,'')::timestamp
from aws_s3_ensec_api_extracts.cdb_meters met;