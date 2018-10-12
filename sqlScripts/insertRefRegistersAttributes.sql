insert into ref_register_attributes
			(	account_id,
				meter_point_id,
				meter_id,
        register_id,
				registersattributes_attributename,
				registersattributes_attributedescription,
				registersattributes_attributevalue) as
select 	reg.account_id,
				reg.meter_point_id,
				reg.meter_id,
        reg.register_id,
				reg.registersattributes_attributename,
				reg.registersattributes_attributedescription,
				reg.registersattributes_attributevalue
from aws_s3_ensec_api_extracts.cdb_registersattributes reg;