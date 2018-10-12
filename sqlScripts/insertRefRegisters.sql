insert into ref_registers
			(	account_id,
				meter_point_id,
				meter_id,
				register_id,
				registers_eacaq,
				registers_registerreference,
				registers_sourceidtype,
				registers_tariffcomponent,
				registers_tpr,
				registers_tprperioddescription)
select 	reg.account_id,
				reg.meter_point_id,
				reg.meter_id,
				reg.register_id,
				reg.registers_eacaq,
				reg.registers_registerreference,
				reg.registers_sourceidtype,
				reg.registers_tariffcomponent,
				reg.registers_tpr,
				reg.registers_tprperioddescription
from aws_s3_ensec_api_extracts.cdb_registers reg;