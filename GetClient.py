from google.cloud import bigquery
from google.oauth2 import service_account
filepath = "C:\\Users\\Chidvi\\Dropbox\Personal\\MSBA\Capstone\\asu-msba-customer-ltv-credentials.json"
def returnClient():
    credentials = service_account.Credentials.from_service_account_file(filepath)
    project_id = 'infusionsoft-looker-poc'
    client = bigquery.Client(credentials=credentials, project=project_id)
    return client
def returnDataSet():
    oppsQuote ='`'+'infusionsoft-looker-poc.asu_msba_customer_ltv.CONFIDENTIAL_ltv_oppty_table'+'`'
    opps ='infusionsoft-looker-poc.asu_msba_customer_ltv.CONFIDENTIAL_ltv_oppty_table'
    #rev ='`infusionsoft-looker-poc.asu_msba_customer_ltv.CONFIDENTIAL_ltv_revenue_table`'
    return oppsQuote, opps

