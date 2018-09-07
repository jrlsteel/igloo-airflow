

from sqlalchemy import create_engine
import pandas as pd
import pandas_redshift as pr
import ig_config as con


def extract_meterpoints_data():
    
    pr.connect_to_redshift(host=con.redshift_config['host'], port=con.redshift_config['port'], user=con.redshift_config['user'], password=con.redshift_config['pwd'], dbname=con.redshift_config['db'])

    data = pr.redshift_to_pandas("SELECT * FROM public.ref_readings limit 5")

    print(data.head())

def main():
    extract_meterpoints_data()


if __name__ == "__main__":
    main()