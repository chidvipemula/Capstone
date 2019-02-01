import numpy as np
import pandas as pd
import pdb
from big_query import BigQueryClass
import os
import seaborn as sns
import matplotlib.pyplot as plt

from google.cloud import bigquery





def drop_columns(df, nullpercent=1):
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
    sql = 'SELECT * FROM `infusionsoft-looker-poc.asu_msba_customer_ltv.CONFIDENTIAL_ltv_oppty_table` LIMIT 100'
    #sql  = 'SELECT * FROM `infusionsoft-looker-poc.asu_msba_customer_ltv.CONFIDENTIAL_ltv_revenue_table` LIMIT 100'
    # pdb.set_trace()
    df = client.get_results(sql)
    rs = drop_columns(df, 1)
    df_clean = df.drop(rs, axis=1)
    print(str(len(rs))+' columns are dropped')
    print(rs)
    print(df_clean.shape)
    return df_clean

def visualize_corr(df):
    plt.figure(figsize=(16, 18))
    sns.heatmap(df.round(2),
                # cmap="Blues",
                # cmap="BuPu",
                # cmap="Greens",
                cmap = "YlGnBu",
                xticklabels=df.columns,
                yticklabels=df.columns,vmin=0, vmax=1,annot=True,linewidths=.5)
    # plt.savefig('../Data/Corr_Rev.png')
    #plt.savefig('../Data/Corr_Oppt.png')
    plt.show()

def extract_highCorr(df,val):
    rs = df[df.abs()<=val]=0
    # pdb.set_trace()
    return rs


def corr_analysis(df):

    df_corr = df.corr()
    #rs  = extract_highCorr(df_corr,0.5) #this is used for analyzing oppt table correlation. only show |coor|>0.5
    visualize_corr(df_corr)
    # df_corr.to_csv('../Data/Revenue_corr.csv')

def data_description(df):
    df.info()
    pdb.set_trace()
    df.describe().to_csv('../Data/Revenue_DropNull_DataDescription.csv',index=True)




def run():

    df_clean = dropNull_analysis()
    corr_analysis(df_clean)
    data_description(df_clean)

if __name__ == '__main__':
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.environ["GOOGLE_APPLICATION_CREDENTIALS_msba"]
    print(os.system("echo $GOOGLE_APPLICATION_CREDENTIALS"))
    client = BigQueryClass()
    run()
