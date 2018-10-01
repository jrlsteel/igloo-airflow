insert into ref_meters_attributes
			(	account_id,
				meter_point_id,
				meter_id,
				metersattributes_attributename,
				metersattributes_attributedescription,
				metersattributes_attributevalue)
select 	met.account_id,
				met.meter_point_id,
				met.meter_id,
				met.metersattributes_attributename,
				met.metersattributes_attributedescription,
				met.metersattributes_attributevalue
from aws_s3_ensec_api_extracts.cdb_metersattributes met;