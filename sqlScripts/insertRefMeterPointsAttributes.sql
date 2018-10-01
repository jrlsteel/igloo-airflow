insert into ref_meterpoints_attributes
				(account_id,
        meter_point_id,
        attributes_attributename,
        attributes_attributedescription,
        attributes_attributevalue,
        attributes_effectivefromdate,
        attributes_effectivetodate)
select  atr.account_id,
        atr.meter_point_id,
        atr.attributes_attributename,
        atr.attributes_attributedescription,
        atr.attributes_attributevalue,
        nullif(atr.attributes_effectivefromdate,''),
        nullif(atr.attributes_effectivetodate,'')
from aws_s3_ensec_api_extracts.cdb_attributes atr;
