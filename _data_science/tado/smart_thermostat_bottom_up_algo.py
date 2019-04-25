# -*- coding: utf-8 -*-
# !/usr/bin/env python2

"""
Created on 01.03.2019

@author: trevor sweetnam

Estimation of Smart Thermostat Impact Based On Customer Charachteristics

"""

import psycopg2
import numpy
import pandas_redshift as pr
import pandas as pd
import json
import sys

sys.path.append('../..')

from _data_science.tado import mandmh_processor as mp
from connections import connect_db as db
from conf import config as con



def estimate_setpoint_impact(row):
    ''' estimate impact of smart thermostat
        on customer target tempeature given
        current state as reported in mandmh
    '''

    # ADJUST FOR CURRENT SETPOINT
    BASE_TEMP = 20.
    all_defaults = True

    setpoint = row['heating_setpoint'].values[0]

    setpoint = float(setpoint)

    # deal with missing setpoint info
    if not setpoint > 0.:
        setpoint = 20.
    else:
        all_defaults = False

    setpoint_impact = (BASE_TEMP - setpoint) * 0.25  # was 0.25

    # ADJUST FOR CURRENT CONTROL APPROACH
    control_assumption = {'smartthermostat': 0.,
                          'thermostatautomatic': -0.5,
                          'timerprogrammer': -0.25,
                          'manually': 0.25,
                          'thermostatmanual': -0.25,
                          'nocontrol': -0.5}

    control_approach = row['heating_control'].values[0]

    if control_approach in control_assumption.keys():
        control_impact = control_assumption[control_approach]
        all_defaults = False
    else:
        control_impact = 0.

    # ADJUST FOR HEATING EXTENT
    heating_extent_assumption = {'wholehome': 0.,
                                 'specificrooms': +0.5}

    heating_extent = row['heating_extent'].values[0]

    if heating_extent in heating_extent_assumption.keys():
        heating_extent_impact = heating_extent_assumption[heating_extent]
        all_defaults = False
    else:
        heating_extent_impact = 0.

        # print 'SP: ', setpoint, setpoint_impact
    # print 'CA: ', control_approach, control_impact
    # print 'HA: ', heating_extent, heating_extent_impact
    sp_impact = setpoint_impact + control_impact + heating_extent_impact

    return setpoint, setpoint + sp_impact, sp_impact, all_defaults


def estimate_heating_hours(row):
    heating_hours_assumption = {'working_no_kids': [12, 8],
                                'working_kids': [14, 12],
                                'retired': [16, 16],
                                'unknown': [13, 10]}

    heating_type = row['heating_type'].values[0]

    if heating_type in heating_hours_assumption.keys():
        return heating_hours_assumption[heating_type][0], heating_hours_assumption[heating_type][1], False
    else:
        return heating_hours_assumption['unknown'][0], heating_hours_assumption['unknown'][1], True


def estimate_mean_internal_temp(sp, heating_hours):
    ''' estimate MIT for a reference building
        these could be replaced by cusotmer specific data
    '''

    HLC = 250  # W/K
    TIMECONSTANT = 50  # hours
    EXTTEMP = 7.0
    HEATCAPACITY = 0.11 * 100

    unheated_hours = 24. - heating_hours

    # borrow algorithm from DEAP - irish version of SAP
    unheated_heat_loss = HEATCAPACITY * (sp - EXTTEMP) * (1. - numpy.exp(-unheated_hours / TIMECONSTANT))
    mit_unheated = EXTTEMP + unheated_heat_loss * 1000000 / (HLC * unheated_hours * 3600)

    mit = ((sp * heating_hours) + (mit_unheated * unheated_hours)) / 24.0

    return mit


def bottom_up_impact_model(row):
    # estimate the setpoint before and after
    base_sp, est_sp, sp_impact, df_flag1 = estimate_setpoint_impact(row)

    # estimate the heating hours before and after
    base_hours, est_hours, df_flag2 = estimate_heating_hours(row)

    # useful to exclude defaults for testing
    if df_flag1 and df_flag2:
        # print 'Warning All Defaults'
        return numpy.nan, numpy.nan, numpy.nan

    # use these to calculate the mit
    base_mit = estimate_mean_internal_temp(base_sp,
                                           base_hours)  # this is for the thermostat settigns our customers gave us
    est_mit = estimate_mean_internal_temp(est_sp, est_hours)  # this is for our estimated setting and estimated hours

    diff = est_mit - base_mit  # negative values -> est is lower
    diff_percentage = diff / base_mit  # impact as a % of base MIT

    # return the impact
    return diff_percentage, base_mit, est_mit


def db_connection():

    env = con.environment_config['environment']

    # env = None
    # if env_config == 'uat':
    #     env = con.uat
    #
    # if env_config == 'prod':
    #     env = con.prod

    pr.connect_to_redshift(host=env['redshift_config']['host'], port=env['redshift_config']['port'],
                           user=env['redshift_config']['user'], password=env['redshift_config']['pwd'],
                           dbname=env['redshift_config']['db'])
    print("Connected to Redshift")

    pr.connect_to_s3(aws_access_key_id=env['s3_config']['access_key'],
                     aws_secret_access_key=env['s3_config']['secret_key'],
                     bucket=env['s3_config']['bucket_name'],
                     subdirectory='aws-glue-tempdir/')

def get_data():
    pr = db.get_redshift_connection()

    query = 'select * from temp_me_and_my_home_fixed where account_id = 1831'  # drop downs etc
    mamh_raw_fixed = pr.redshift_to_pandas(query)

    # print len(mamh_raw_fixed)

    query = 'select * from temp_me_and_my_home_non_fixed where account_id = 1831'  # appliance data
    mamh_raw_non_fixed = pr.redshift_to_pandas(query)

    clean_df = mp.clean_mamh(mamh_raw_fixed, mamh_raw_non_fixed, uid=None)


    # create family categories based around lifestyle
    h_size, h_type, ht_summary = mp.fun_create_family_categories(clean_df)
    h_summ_df = pd.Series(ht_summary)
    clean_df['heating_type'] = h_summ_df
    clean_df['heating_type'] = 'working_kids'
    clean_df['heating_control'] = 'smartthermostat'
    return clean_df


if __name__ == '__main__':

    clean_df = get_data()

    impacts = {}
    for uid in clean_df['user_id'].unique()[:]:
        impact, base_mit, est_mit = bottom_up_impact_model(clean_df.loc[clean_df['user_id'] == uid])
        impacts[uid] = impact

        print(impact)

    clean_df['st_impact'] = pd.Series(impacts)
    clean_df['st_impact'].plot(kind='hist')

