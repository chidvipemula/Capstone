'''
This parser will do:
1. fetech data from bigquery and drop columns whose missing rate is more than a value ( in here we set 100%)
3. merge oppt_table
4. merge rev_table
5. link merged_oppt_table and merged_rev_talbe as the final table merged_table
'''

import numpy as np
import pandas as pd
import pdb
from big_query import BigQueryClass
import os
import seaborn as sns
import matplotlib.pyplot as plt

from google.cloud import bigquery




def dropColumns(df, nullpercent=1):
    threshhold = df.shape[0] * nullpercent
    # rs =  [c for c in df.columns if sum(df[c].isnull()) >= threshhold]
    rs = []
    for i in df.columns:
        try:
            num = sum(df[i].isnull())
            if num >= threshhold:
                rs.append(i)
        except:
            print("in exception: ")
    return rs


def dropNull_analysis():

    sql  = 'SELECT * FROM `infusionsoft-looker-poc.asu_msba_customer_ltv.CONFIDENTIAL_ltv_oppty_table` limit 100'
    df_oppt = client.get_results(sql)
    rs1 = dropColumns(df_oppt, 1)
    df_oppt_clean = df_oppt.drop(rs1, axis=1)
    print(str(len(rs1))+' columns are dropped')
    print(rs1)
    print(df_oppt_clean.shape)



    sql = 'SELECT * FROM `infusionsoft-looker-poc.asu_msba_customer_ltv.CONFIDENTIAL_ltv_revenue_table` limit 100'
    df_rev = client.get_results(sql)
    rs2 = dropColumns(df_rev, 1)
    df_rev_clean = df_rev.drop(rs2, axis=1)
    print(str(len(rs2)) + ' columns are dropped')
    print(rs2)
    print(df_rev_clean.shape)

    return df_oppt_clean, df_rev_clean


def dropAva_columns(df):
    rs = []
    for i in df.columns:
        try:
            if i.startswith('ava_'):
                rs.append(i)
        except:
            print("in exception: ")
    df_clean = df.drop(rs, axis=1)
    return df_clean

def merge_oppt_strategy(df):
    # how to merge oppt table
    #df is rows with same account id
    '''
    select most frequence values in each column and early time as start-related column value,
    latest time in stop-related column. No most frequent one, randomly select one
    '''

    startTime = {'created_date','escalation_date_c','new_stage_date_time_c','system_modstamp','target_implementation_date_c','owner_created_at',
                 'prospect_date','sales_accepted_c','demo_date','long_term_nurture_on','bubble_up_date_c'}
    stopTime ={'close_date','escalation_date_c','last_activity_date','last_activity_date_c','last_modified_date','paying_customer_date','closed_won_on',
               'closed_lost_on','partner_last_referred_date_c'}

    for column in df.columns:

        if column in startTime:
            try:
                minVal = df[column].min()
            except:
                minVal = df[column].dropna().min()

            df[column].values[:] = minVal

        elif column in stopTime:
            try:
                maxVal = df[column].max()
            except:
                maxVal =df[column].dropna().max()
                # pdb.set_trace()
            df[column].values[:] = maxVal

        else:

            modeValue = df[column].dropna().mode()
            if modeValue.shape[0] ==0: # mode doesn't consider NaN's , so if all rows are NaN, then return nothing
                modeValue = 'NaN'
            elif len(modeValue)>1:
                try:
                    modeValue = modeValue.max()
                except:
                    modeValue = modeValue.astype('float64').max()

            df[column].values[:] = modeValue
    return df.iloc[0,:]

def merge_oppt(df): #find all rows with the same account_id

    merged_df = pd.DataFrame()
    #get all unique account ids
    ids = set(df['account_id'])
    total= len(ids)
    counter = 1
    #find each id's app_name
    for id in ids:
        if counter%1000 ==0:
            print('working on '+str(counter)+'/'+str(total))
        counter +=1

        df_id =  df.loc[df['account_id'] == id]
        try:
            df_id_merged = merge_oppt_strategy(df_id) # returned is a series of one user account
            df1 = df_id_merged.to_frame().T # convert series to df
            merged_df = merged_df.append(df1, ignore_index=True)
        except:
            print('There are erros in merging '+str(id))

    print('..............Finished Merging Oppt Table.................')

    return merged_df
def opptTable(df_oppt):
    # first drop all ava_ columns in oppt
    df_oppt = dropAva_columns(df_oppt)
    df_merged_oppt = merge_oppt(df_oppt)
    return df_merged_oppt


def merge_revenue_strategy(df):
    # how to merge revenue table
    # df is rows with same app name
    '''
    mrr columns get avg, month_out select max, others mode, sales_effective_date is earliest
    '''

    for column in df.columns:

        if '_mrr' in column:
            try:
                avgVal = df[column].mean()
            except:
                avgVal = df[column].dropna().min()

            df[column].values[:] = avgVal

        elif 'months_out' == column:
            try:
                maxVal = df[column].max()
            except:
                maxVal = df[column].dropna().max()
                # pdb.set_trace()
            df[column].values[:] = maxVal
        elif '_date' in column:
            try:
                minVal = df[column].min()
            except:
                minVal = df[column].dropna().min()

            df[column].values[:] = minVal

        else:

            modeValue = df[column].dropna().mode()
            if modeValue.shape[0] == 0:  # mode doesn't consider NaN's , so if all rows are NaN, then return nothing
                modeValue = 'NaN'
            elif len(modeValue) > 1:
                try:
                    modeValue = modeValue.max()
                except:
                    modeValue = modeValue.astype('float64').max()

            df[column].values[:] = modeValue

    rs = df.iloc[0, :]
    rs['total_mrr'] = df['month_mrr'].sum() #create the target
    return rs

def merge_revenue(df): # find all rows with the same app_name
    merged_df = pd.DataFrame()

    # get all unique account ids
    apps = set(df['app_name'])
    total = len(apps)
    counter = 1
    # find each id's app_name
    for app in apps:
        # print(counter)
        if counter % 1000 == 0:
            print('working on ' + str(counter) + '/' + str(total))
        counter += 1
        # app = '001j000000YC1twAAD'
        df_app = df.loc[df['app_name'] == app]
        try:
            df_app_merged = merge_revenue_strategy(df_app)  # returned is a series
            df1 = df_app_merged.to_frame().T  # convert series to df

            merged_df = merged_df.append(df1, ignore_index=True)
            # pdb.set_trace()
        except:
            print('There are erros in merging ' + str(id))


    print('..............Finished Merging Revenue Table.................')

    return merged_df
def revenueTable(df_rev):
    df_merged_rev = merge_revenue(df_rev)
    return df_merged_rev

def merge_tables_strategy(df_oppt, df_rev, id, apps):
    total = 0.0

    for app in apps:
        try:

            row = df_rev.loc[df_rev['app_name'] == app]
            # print (row)
            if len(row)>0:
                '''
                row['total_mrr'] is series, so get value by using values
                row['total_mrr'].values =  array([2691.])
                '''
                total += row['total_mrr'].values[0] #
        except:
            print('11111111')

    rs = df_oppt.loc[df_oppt['account_id']==id]
    rs['total_value'] = total # add a new column in the oppt merged table as target, and the value is total app_name  expense
    return rs
def merge_tables(df, df_oppt, df_rev): # which are df_oppt,df_merged_oppt, df_merged_rev
    '''

    used to merge two tables. format will be customer A information in oppt and target is his total value

    Total value = sum of A's all apps value (A may have several apps)

    each row in df_rev is one app's information, and there's new column called total_mrr, which means the
    total value of this app.

    each row in df_oppt is one customer's information. in here app_name is the most frequence one.
    Hence, to have all app of customer A, we need to use original oppt_table information, which is df, to get all apps
    of this customer A
    '''
    merged_df = pd.DataFrame()

    # get all unique account ids
    ids = sorted(set(df_oppt['account_id'].unique()))

    total = len(ids)
    counter = 0


    for i in range(0,total):
        if counter % 1000 == 0:
            print('working on ' + str(counter) + '/' + str(total))
        counter += 1
        id = ids[i]
        apps = set(df.loc[df['account_id'] == id]['app_name'].unique()) #find all app_names of a customer

        try:
            df_table_merged = merge_tables_strategy(df_oppt, df_rev, id, apps)

            merged_df = merged_df.append( df_table_merged, ignore_index=True)

        except:
            print('There are erros in merging ' + str(id))

    return merged_df
def merge_oppt_revenue(df_oppt,df_merged_oppt, df_merged_rev): # merge oppt and revenue tables

    df_merged_oppt_rev = merge_tables(df_oppt,df_merged_oppt, df_merged_rev)
    return df_merged_oppt_rev

def run():
    df_oppt, df_rev = dropNull_analysis()
    df_merged_oppt = opptTable(df_oppt)
    df_merged_rev = revenueTable(df_rev)
    df = merge_oppt_revenue(df_oppt, df_merged_oppt, df_merged_rev ) # here df is the final merged table, df_merged_oppt_rev

if __name__ == '__main__':
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.environ["GOOGLE_APPLICATION_CREDENTIALS_msba"]
    print(os.system("echo $GOOGLE_APPLICATION_CREDENTIALS"))
    client = BigQueryClass()
    run()

