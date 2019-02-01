'''
Basic BigQuery Operations Defined here
'''

from google.cloud import bigquery
import pdb
import pandas as pd


class BigQueryClass:

    def __init__(self):
        self.client = bigquery.Client()

    def get_results(self,sql):
        # query_job = self.client.query(sql)  # API request
        # rows = query_job.result()  # Waits for query to finish
        rows = pd.read_gbq(sql,project_id = 'infusionsoft-looker-poc', dialect = 'standard')
        return rows

