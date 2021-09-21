from cdw.common import utils as util
from cdw.common.directories import common

"""
This script forms part of the d3079 data flow creation. 
It refreshes a materialised redshift view containing half-hourly energy consumption of customers with a smart meter.
"""


def refresh_mv_hh_elec_reads():
    # Execute Refresh command on Materialised View for Smart HH Elec

    try:
        smart_mv_hh_elec_refresh_sql_query = common["smart_mv_hh_elec_refresh"]["sql_query_smart_mv_hh_elec_refresh"]
        response = util.execute_redshift_sql_query(smart_mv_hh_elec_refresh_sql_query)
        return response
    except:
        raise


if __name__ == "__main__":
    response = refresh_mv_hh_elec_reads()
