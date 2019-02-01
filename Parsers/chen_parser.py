import numpy as np
import pandas as pd
import pdb
from big_query import BigQueryClass
import os
from google.cloud import bigquery



def missing_table(df):
    mis_val = df.isnull().sum() # how many entries in each column are NaN ( missing)
    mis_percent = 100*df.isnull().sum()/len(df) #percentage of the missing values in each column
    mis_table = pd.concat([mis_val, mis_percent], axis = 1) # make a table to contain missing value and missing percentage

    '''
    mis_table_allFeatures return all columns' number of missing and their missing percentages. The results are NOT sorted
    '''
    mis_table_allFeatures = mis_table.rename(columns={0:"Number of  Missing Values", 1: "Missing Percentage of Total Entries"})


    # mis_table_missingFeatures = mis_table_allFeatures[mis_table_allFeatures.iloc[:,1]!=0].sort_values("Missing Percentage of Total Entries", ascending=False).round(2)
    mis_table_allFeaturess = mis_table_allFeatures.sort_values("Missing Percentage of Total Entries", ascending=False).round(2)
    print("Your selected dataframe has " + str(df.shape[1]) + " columns.\n" +"There are " + str(mis_table_allFeatures[mis_table_allFeatures.iloc[:,1]!=0].shape[0]) +
          " columns that have missing values.")

    print (mis_table_allFeatures)
    # mis_table_missingFeatures.to_csv('../Data/MissingTable_'+filename) # output the missing table to a csv file


if __name__ == '__main__':
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.environ["GOOGLE_APPLICATION_CREDENTIALS_msba"]
    client = BigQueryClass()
    sql  = 'SELECT * FROM `infusionsoft-looker-poc.asu_msba_customer_ltv.CONFIDENTIAL_ltv_oppty_table` LIMIT 100'
    df = client.get_results(sql)
    missing_table(df)
    # pdb.set_trace()
