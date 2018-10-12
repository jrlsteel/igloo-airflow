insert into ref_readings
		( account_id,
      meter_point_id,
      reading_registerid,
      reading_id,
      id,
      meterpointid,
      datetime,
      createddate,
      meterreadingsource,
      readingtype,
      reading_value)
select red.account_id,
       red.meter_point_id,
      red.reading_registerid,
      red.reading_id,
      red.id,
      red.meterpointid,
      nullif(red.datetime,'')::timestamp,
      nullif(red.createddate,'')::timestamp,
      red.meterreadingsource,
      red.readingtype,
      red.reading_value
from aws_s3_ensec_api_extracts.cdb_readings red;