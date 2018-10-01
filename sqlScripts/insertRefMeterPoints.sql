insert into ref_meterpoints
		  (account_id ,
			 meter_point_id,
			 meterpointnumber,
			 associationstartdate,
			 associationenddate,
			 supplystartdate,
			 supplyenddate,
			 issmart,
			 issmartcommunicating,
			 meterpointtype)
select s.account_id ,
			 s.meter_point_id,
			 s.meterpointnumber,
			 nullif(s.associationstartdate,'')::timestamp,
			 nullif(s.associationenddate,'')::timestamp,
			 nullif(s.supplystartdate,'')::timestamp,
			 nullif(s.supplyenddate,'')::timestamp,
			 s.issmart,
			 s.issmartcommunicating,
			 s.meterpointtype
from aws_s3_ensec_api_extracts.cdb_meterpoints s;