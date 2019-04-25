# -*- coding: utf-8 -*-
# !/usr/bin/env python2

"""
Created on 30.01.2019

@author: trevor sweetnam

Cleaning function for m&mh data

"""

import pandas as pd
import numpy as np
from scipy.stats import halfnorm

# import sklearn.linear_model as lm
# from sklearn.model_selection import train_test_split
# from sklearn.metrics import confusion_matrix
import matplotlib.pyplot as plt
import itertools

# from eacs_meter_reads2 import Build_and_run_query

pd.options.mode.chained_assignment = None


def clean_mamh(mamh_raw_f, mamh_raw_nf, uid=None):
    debug = False

    # don't know if this scalable
    # res = {}

    user_ids = mamh_raw_f['user_id'].unique()

    print(len(user_ids), ' response records')

    flat = {}

    for uid in user_ids[:]:

        flat[uid] = {}

        flat[uid]['user_id'] = uid

        mamh_user = pd.concat([mamh_raw_f[mamh_raw_f['user_id'] == uid], mamh_raw_nf[mamh_raw_nf['user_id'] == uid]])

        if debug: print('########### ', uid, len(mamh_user), ' ###########')

        pc = 0  # counter for people

        # for question in mamh_user['title']:
        for row in mamh_user.iterrows():

            # if debug: print row

            if 'people' in row[1]['title']:
                pc += 1

                # print '----------'
                # print row

                flat[uid]['person_' + str(pc) + '_age'] = row[1]['user_age']

            # title contains the question
            # if it's an appliance question we need to tidy up the data
            if 'appliance' in row[1]['title']:
                # print row

                if 'kitchen' in row[1]['title']:
                    lab = 'ka_'
                else:
                    lab = 'a_'

                # grab the appliance stuff from the row
                ap_name = row[1]['machine_name']
                ap_use = row[1]['machine_usage']
                ap_age = row[1]['machine_age']

                if ap_name == '':
                    continue
                    # print row
                else:
                    ap_name = lab + ap_name

                # pack it in to the dictionary
                flat[uid][ap_name + '_owned'] = True
                flat[uid][ap_name + '_use'] = ap_use
                flat[uid][ap_name + '_age'] = ap_age

            # otherwise we can just store the answer directly
            else:
                # attribute_value contains the answer
                flat[uid][row[1]['title']] = row[1]['attribute_value']

                # store the number of occupants
        if pc == 0:
            pc = 'N/A'
        flat[uid]['How many people live in your home?'] = pc

    df = pd.DataFrame(flat).transpose()

    # SIMPLIFY COLUMN NAMES
    colmap = {'Do you have a hot water tank?': 'dhw_tank_present',
              'How do you control the time of day your heating is on?': 'heating_control',
              'How do you normally heat your home?': 'heating_extent',
              'How many bedrooms does your home have?': 'number_of_bedrooms',
              'How many people live in your home?': 'number_of_occupants',
              'How old is your boiler, or if you don’t have central heating your most used heater?': 'heating_age',
              'How old is your home?': 'dwelling_age',
              'To what temperature do you normally set your thermostat, or if you don’t have one the temperature you prefer your home to be?': 'heating_setpoint',
              'What is your main source of heating?': 'main_heat_source',
              'What type of home do you live in?': 'dwelling_type'}

    df.rename(columns=colmap, inplace=True)

    return df


def fun_sort_age_columns(df_in):
    df_persons = df_in[['person_10_age', 'person_11_age',
                        'person_12_age', 'person_13_age', 'person_14_age', 'person_15_age',
                        'person_16_age', 'person_17_age', 'person_18_age', 'person_19_age',
                        'person_1_age', 'person_20_age', 'person_21_age', 'person_22_age',
                        'person_23_age', 'person_24_age', 'person_2_age', 'person_3_age',
                        'person_4_age', 'person_5_age', 'person_6_age', 'person_7_age',
                        'person_8_age', 'person_9_age', 'user_id']]
    df_pp = pd.melt(df_persons, id_vars=['user_id'])
    df_ppp = df_pp.groupby(by=['user_id', 'value'], as_index=False).count()
    df_ppp.value[df_ppp['value'] == ''] = '0 to 99'
    df_ppp = df_ppp.pivot(index='user_id', columns='value', values='variable')
    df_ppp = df_ppp.fillna(0)
    try:
        df_ppp['occupants_sum'] = df_ppp['0 to 9'] + df_ppp['10 to 17'] + \
                                  df_ppp['18 to 64'] + df_ppp['65 to 74'] + df_ppp['75 and over'] + \
                                  df_ppp['0 to 99']
    except KeyError:
        df_ppp['occupants_sum'] = df_ppp['0 to 9'] + df_ppp['10 to 17'] + \
                                  df_ppp['18 to 64'] + df_ppp['65 to 74'] + df_ppp['75 and over']

    df_ppp = df_ppp.reset_index()
    df_in = df_in.drop(columns=['person_10_age', 'person_11_age',
                                'person_12_age', 'person_13_age', 'person_14_age', 'person_15_age',
                                'person_16_age', 'person_17_age', 'person_18_age', 'person_19_age',
                                'person_1_age', 'person_20_age', 'person_21_age', 'person_22_age',
                                'person_23_age', 'person_24_age', 'person_2_age', 'person_3_age',
                                'person_4_age', 'person_5_age', 'person_6_age', 'person_7_age',
                                'person_8_age', 'person_9_age'])
    df_in.user_id = df_in.user_id.astype(int)
    df_ppp.user_id = df_ppp.user_id.astype(int)
    df_out = df_in.merge(df_ppp, left_on='user_id', right_on='user_id')
    return (df_out)


def fun_create_family_categories(df_in):
    '''
    categorise households in a useful? manner
    '''

    # 1 occupant -> age ranges.... < 30, 30 - 65, 65+
    # 2 occupants -> age ranges as above
    # 3 occupants -> couple with young kids, couple with teenagers, couple with older kids....
    # 4 occupants as above...

    household_size_out = {}
    household_type_out = {}
    household_type_summary = {}
    #      {'working_no_kids':[10,8],
    # 'working_kids':[12,11],
    # 'retired':[16,16]}

    age_map = {'0 to 9': 1, '10 to 17': 2, '18 to 64': 3, '65 to 74': 4, '75 and over': 5, '': 6}

    # print(df_in)

    # drop blanks
    df = df_in.dropna(subset=['number_of_occupants'])

    # print(df)
    cats = {}
    no_ocs = []
    len_ag = []

    for uid in df['user_id'].unique()[:]:

        no_oc_df = df[df['user_id'] == uid].iloc[0]

        # get the numner of occupants
        # no_oc = no_oc_df['number_of_occupants']
        # print(no_oc_df)
        # select the ages and map - probably faster ot do this on the whole df at once
        ages = no_oc_df[[c for c in no_oc_df.index if 'pers' in c]].dropna().replace(age_map)

        # print(ages.values)

        # sort
        ages = sorted(ages.values, reverse=True)

        no_oc = len(ages)

        ages = [a for a in ages if isinstance(a, int)]

        household_size_out[uid] = no_oc

        # if we don't have any data
        if no_oc == 0:
            ft = 'unknown_no_occupants_reported'
            hs = 'unknown'
        else:
            if no_oc == 1:
                if ages[0] == 3:
                    ft = 'working_age_individual'
                    hs = 'working_no_kids'
                elif ages[0] > 3:
                    ft = 'retired_individual'
                    hs = 'retired'
                else:
                    ft = 'unknown_bad_age_data'
                    hs = 'unknown'

            elif no_oc == 2:
                if all([a == 3 for a in ages]):
                    ft = 'working_age_couple'
                    hs = 'working_no_kids'

                elif sum(ages) > 6:
                    ft = 'retired_couple'
                    hs = 'retired'

                elif sum(ages) < 6:
                    if 1 in ages or 2 in ages and 3 in ages:
                        ft = 'single_parent'
                        hs = 'working_kids'

                    elif 1 in ages or 2 in ages and 3 in ages or 4 in ages:
                        ft = 'retired_parent'
                        hs = 'retired'

                    else:
                        ft = 'undefined_type_2'
                        hs = 'unknown'

            elif no_oc == 3:
                if all([a == 3 for a in ages]):
                    ft = 'working_house_share_3'
                    hs = 'working_no_kids'

                elif sum(ages) > 12:
                    ft = 'retired_3'
                    hs = 'retired'
                else:
                    ft = 'working_age_family_3'
                    hs = 'working_kids'

            elif no_oc == 4:
                if all([a == 3 for a in ages]):
                    ft = 'working_house_share_4'
                    hs = 'working_no_kids'

                elif sum(ages) > 16:
                    ft = 'retired_4'
                    hs = 'retired'

                else:
                    ft = 'working_age_family_4'
                    hs = 'working_kids'

            else:
                if all([a == 3 for a in ages]):
                    ft = 'working_house_share_5plus'
                    hs = 'working_no_kids'

                elif sum(ages) > 20:
                    ft = 'retired_5plus'
                    hs = 'retired'

                else:
                    ft = 'working_age_family_5plus'
                    hs = 'working_kids'
        # except:
        #    print ages
        #    ft = 'unknown'
        #    hs = 'unknown'

        household_type_out[uid] = ft
        household_type_summary[uid] = hs

    return household_size_out, household_type_out, household_type_summary


# h_size, h_type, ht_summary = fun_create_family_categories(clean_df)

def fun_create_appliances_answer_flags(df_in):
    """ Create flags for whether they at least answered on appliance  question
    so we could reasonably assume that they don't own the unclicked appliances
    and potentially distinguish those who didn't want to answer any questions
    regarding appliances
    """
    df_in['a_answered'] = False
    df_in['a_answered'][
        (df_in['a_dishwasher_owned'] == True) |
        (df_in['a_power_shower_owned'] == True) |
        (df_in['a_tumble_dryer_owned'] == True) |
        (df_in['a_washer_dryer_owned'] == True) |
        (df_in['a_washing_machine_owned'] == True)
        ] = True

    df_in['ka_answered'] = False
    df_in['ka_answered'][
        (df_in['ka_electric_hob_owned'] == True) |
        (df_in['ka_electric_oven_owned'] == True) |
        (df_in['ka_gas_hob_owned'] == True) |
        (df_in['ka_gas_oven_owned'] == True) |
        (df_in['ka_standalone_freezer_owned'] == True) |
        (df_in['ka_standalone_fridge_owned'] == True) |
        (df_in['ka_fridge_freezer_owned'] == True)
        ] = True
    return (df_in)


def fun_replace_ownership_nan_with_false(df_in):
    """ Replaces nan with false in appliance onwership columns. Should be run
    after running fun_create_appliances_answer_flags and use that function's
    output as  input.
    """
    a = ['a_dishwasher_owned', 'a_power_shower_owned', 'a_tumble_dryer_owned',
         'a_washer_dryer_owned', 'a_washing_machine_owned']
    ka = ['ka_electric_hob_owned', 'ka_electric_oven_owned', 'ka_gas_hob_owned',
          'ka_gas_oven_owned', 'ka_standalone_freezer_owned',
          'ka_standalone_fridge_owned', 'ka_fridge_freezer_owned']

    for i in a:
        df_in[i].fillna(False, inplace=True)
    for i in ka:
        df_in[i].fillna(False, inplace=True)
    return (df_in)


def fun_translate_usage_to_continuous(ar):
    """ Attempt to translate categorical variables for usage to continous ones.
    """

    # start by making usage column as numnpy array
    # ar = df_in['a_washing_machine_use']
    ar_out = np.array([])
    for a in ar:
        if a == 'daily':
            b = 7.0 + np.random.normal(loc=0, scale=0.5)
        elif a == 'multiple_times_daily':
            b = 7 * halfnorm.rvs(loc=1, scale=2)  # 7*halfnorm.rvs(loc=2,scale=1)
        elif a == 'several_times_weekly':
            b = halfnorm.rvs(loc=2, scale=2)
        elif a == 'several_times_monthly':
            b = np.random.normal(loc=4, scale=2) / 7.0  # once a week
        elif a == 'once_per_month':
            b = 0.25 + np.random.normal(loc=0, scale=0.05)
        else:
            b = np.nan
        ar_out = np.append(ar_out, b)

    return (ar_out)


def fun_add_continuous_usage_column(df_in):
    df_out = df_in
    a = ['a_dishwasher_use', 'a_power_shower_use', 'a_tumble_dryer_use',
         'a_washer_dryer_use', 'a_washing_machine_use']
    ka = ['ka_electric_hob_use', 'ka_electric_oven_use', 'ka_gas_hob_use',
          'ka_gas_oven_use']
    for i in a:
        df_out[i + '_cont'] = fun_translate_usage_to_continuous(np.array(df_in[i]))
        df_out[i + '_cont'] = pd.to_numeric(df_out[i + '_cont'])

    for i in ka:
        df_out[i + '_cont'] = fun_translate_usage_to_continuous(np.array(df_in[i]))
        df_out[i + '_cont'] = pd.to_numeric(df_out[i + '_cont'])

    return (df_out)


def fun_create_combined_usage_column(df_in, col1, col2, new_col):
    """ For combining the usage of two overalpping appliances like washing
    machine and washer dryer
    """
    df_in[new_col] = df_in[col1]
    ind = (df_in[new_col].isnull()) & (~df_in[col2].isnull())
    df_in.loc[ind, new_col] = df_in.loc[ind, col2]
    return (df_in)


def one_hot_encoding(df_in, list_of_vars):
    """ Expands categorical columns into new binary columns
    """
    one_hot = pd.get_dummies(df_in[list_of_vars], drop_first=True)
    df = df_in.join(one_hot)
    df = df.drop(columns=list_of_vars)
    return (df)


def col_to_be_numeric(df_in, col_list):
    for i in col_list:
        df_in[i] = pd.to_numeric(df_in[i])
    return (df_in)


def fix_some_nan_ages(df_in, col_list):
    """ For owned devices use median age if it's missing
    """
    for i in col_list:
        fillin = df_in[i].median()
        ind = (df_in[i].isna()) & (df_in[i[:-3] + 'owned'] == True)
        df_in[i][ind] = fillin
    return (df_in)


def number_of_bedrooms_to_numeric(df_in):
    nb = df_in.number_of_bedrooms.values
    nb2 = np.array([])
    for i in nb:
        if i == 'fiveplus_bedroom':
            nb2 = np.append(nb2, 5)
        elif i == 'four_bedroom':
            nb2 = np.append(nb2, 4)
        elif i == 'three_bedroom':
            nb2 = np.append(nb2, 3)
        elif i == 'two_bedroom':
            nb2 = np.append(nb2, 2)
        elif i == 'one_bedroom':
            nb2 = np.append(nb2, 1)
        else:
            nb2 = np.append(nb2, np.nan)
    df_in['number_of_bedrooms2'] = nb2
    df_in['number_of_bedrooms2'][df_in['number_of_bedrooms2'].isna()] = df_in['number_of_bedrooms2'].median()
    return (df_in)


def get_mamh_df():
    query1 = """ select * from temp_me_and_my_home_non_fixed"""
    query2 = """ select * from temp_me_and_my_home_fixed"""
    a = Build_and_run_query()
    a.build_query(query1)
    non_fixed = a.run_query()
    a.build_query(query2)
    fixed = a.run_query()
    df_t = clean_mamh(fixed, non_fixed, uid=None)
    return (df_t)


def example_usage_for_cleaning(df_t=None):
    if df_t is None:
        df_t = get_mamh_df()
    df_in = fun_sort_age_columns(df_t)
    df_in = fun_create_appliances_answer_flags(df_in)

    to_be_numeric = ['a_dishwasher_age', 'a_power_shower_age', 'a_tumble_dryer_age',
                     'a_washer_dryer_age', 'a_washing_machine_age',
                     'ka_electric_oven_age', 'ka_electric_hob_age',
                     'ka_gas_hob_age', 'ka_gas_oven_age',
                     'ka_fridge_freezer_age', 'ka_standalone_freezer_age']
    df_in = col_to_be_numeric(df_in, to_be_numeric)
    df_in.heating_setpoint = pd.to_numeric(df_in.heating_setpoint)
    df_in.heating_setpoint[df_in.heating_setpoint.isna()] = df_in.heating_setpoint.median()
    df_in = fix_some_nan_ages(df_in, to_be_numeric)
    df_in = number_of_bedrooms_to_numeric(df_in)

    # now work with data for which we know about appliance ownership data
    df_in = df_in[df_in.a_answered == True]
    df_in = fun_replace_ownership_nan_with_false(df_in)

    df_in = fun_create_combined_usage_column(
        df_in, 'a_washing_machine_use', 'a_washer_dryer_use',
        'washing_machine_washer_dryer_use_combo')
    df_in = fun_create_combined_usage_column(df_in, 'a_washing_machine_age',
                                             'a_washer_dryer_age',
                                             'washing_machine_washer_dryer_age_combo')

    to_be_one_hot = ['heating_age', 'dwelling_age', 'dwelling_type',
                     'heating_control', 'heating_extent',
                     'dhw_tank_present', 'main_heat_source']

    df = one_hot_encoding(df_in, to_be_one_hot)
    return (df)


def basic_logistic_regression(df, input_vars, var_to_predict):
    df = df.dropna(subset=[var_to_predict], inplace=False)

    if df[var_to_predict].dtypes != 'bool':
        df = df[df[var_to_predict] != '']
    # input_vars = fun_usage_input_vars()

    X = df.loc[:, input_vars]
    y = df.loc[:, df.columns == var_to_predict].values.ravel()

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.1, random_state=42)

    lr = lm.LogisticRegression(random_state=0, solver='lbfgs', max_iter=1000,
                               multi_class='multinomial').fit(X_train, y_train)
    y_pred = lr.predict(X_test)
    cm = confusion_matrix(y_test, y_pred)
    return (X_train, y_train, X_test, y_test, cm)


def use_only_popular_categories(X, y, y_cat, cutoff=0.1):
    """ Use categories that have at a certain share of the total.
    """
    # l = np.unique(y_cat)
    # ls = '['+', '.join(str(x) for x in l)+']'
    # print("Categories meaning:"+ls+"=  "+str(le.inverse_transform(l)))
    # print(pd.Series(y).value_counts())

    y_hist = pd.Series(y).value_counts().values
    y_hist_test = (y_hist / sum(y_hist)) < cutoff
    y_to_be_removed = pd.Series(y).value_counts().index[y_hist_test]

    ind = np.full((len(X)), False, dtype=bool)
    for i in range(len(y_to_be_removed.values)):
        ind = ind + (y == y_to_be_removed.values[i])

    y_cat = y_cat[~ind]
    X = X[~ind]
    return (X, y, y_cat)


def plot_confusion_matrix(cm, classes,
                          normalize=False,
                          title='Confusion matrix',
                          cmap=plt.cm.Blues):
    """
    This function prints and plots the confusion matrix.
    Normalization can be applied by setting `normalize=True`.

    Example taken from scikit-learn website
    """
    if normalize:
        cm = cm.astype('float') / cm.sum(axis=1)[:, np.newaxis]
        print("Normalized confusion matrix")
    else:
        print('Confusion matrix, without normalization')

    print(cm)

    plt.imshow(cm, interpolation='nearest', cmap=cmap)
    plt.title(title)
    plt.colorbar()
    tick_marks = np.arange(len(classes))
    plt.xticks(tick_marks, classes, rotation=45)
    plt.yticks(tick_marks, classes)

    fmt = '.2f' if normalize else 'd'
    thresh = cm.max() / 2.
    for i, j in itertools.product(range(cm.shape[0]), range(cm.shape[1])):
        plt.text(j, i, format(cm[i, j], fmt),
                 horizontalalignment="center",
                 color="white" if cm[i, j] > thresh else "black")

    plt.ylabel('True label')
    plt.xlabel('Predicted label')
    plt.tight_layout()


def main():
    """ Simple usage example
    """
    var_to_predict = 'washing_machine_washer_dryer_use_combo'
    df = example_usage_for_cleaning()
    cm = basic_logistic_regression(df, var_to_predict)


if __name__ == "__main__":
    main()