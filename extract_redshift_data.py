

from sqlalchemy import create_engine
import pandas as pd
import pandas_redshift as pr
import ig_config as con
from pathlib import Path


def extract_meterpoints_data():
    
    pr.connect_to_redshift(host=con.redshift_config['host'], port=con.redshift_config['port'], user=con.redshift_config['user'], password=con.redshift_config['pwd'], dbname=con.redshift_config['db'])
    readings_sql = '''with q as 
                    (select * , row_number() over(partition by reading_id order by reading_id) row_no from ref_readings)
                    select * from q where row_no = 1
                    order by q.reading_id,row_no'''
    ref_readings = pr.redshift_to_pandas(readings_sql)
    # ref_meterpoints = pr.redshift_to_pandas("SELECT * FROM public.ref_meterpoints limit 1000")
    # ref_meters = pr.redshift_to_pandas("SELECT * FROM public.ref_meters limit 1000")
    # ref_metersattributes = pr.redshift_to_pandas("SELECT * FROM public.ref_metersattributes limit 1000")
    # ref_registers = pr.redshift_to_pandas("SELECT * FROM public.ref_registers limit 1000")
    # ref_registersattributes = pr.redshift_to_pandas("SELECT * FROM public.ref_registersattributes limit 1000")
    # ref_attributes = pr.redshift_to_pandas("SELECT * FROM public.ref_attributes limit 1000")
    file = Path('ref_readings_B.csv')
    if not file.exists():
        ref_readings.to_csv('ref_readings_B.csv', index=False)

    # ref_meterpoints.to_csv('ref_meterpoints_B.csv', index=False)
    # ref_meters.to_csv('ref_meters_B.csv', index=False)
    # ref_metersattributes.to_csv('ref_metersattributes_B.csv', index=False)
    # ref_registers.to_csv('ref_registers_B.csv', index=False)
    # ref_registersattributes.to_csv('ref_registersattributes_B.csv', index=False)
    # ref_attributes.to_csv('ref_attributes_B.csv', index=False)
    # print(ref_readings.head(2))
    ref_readings_B = pd.read_csv('ref_readings_B.csv')
    ref_readings_join = ref_readings_B.merge(ref_readings[(ref_readings.reading_value != ref_readings_B.reading_value) | (ref_readings.readingtype != ref_readings_B.readingtype)][['reading_id']], left_on='reading_id', right_on='reading_id', how='right')
    # ref_readings_join.
    ref_readings_join.to_csv('readings_updated.csv', index=False)

    # reading_out = ref_readings_join[ref_readings_join.reading_value != ref_readings_join.reading_value_B]
    # print(reading_out)

def main():
    extract_meterpoints_data()


if __name__ == "__main__":
    main()