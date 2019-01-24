from google.cloud import bigquery
from google.oauth2 import service_account
credentials = service_account.Credentials.from_service_account_file(
    'C:/Users/Chidvi/Dropbox/Personal/MSBA/Capstone/Github/infusionsoft-looker-poc-1b568931f891.json')
project_id = 'infusionsoft-looker-poc'
client = bigquery.Client(credentials= credentials,project=project_id)
query = ("""SELECT * FROM infusionsoft-looker-poc.asu_msba_customer_ltv.CONFIDENTIAL_ltv_oppty_table Limit 10""")
query_job = client.query(query)  # API request - starts the query
