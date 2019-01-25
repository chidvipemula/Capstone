import GetClient
client = GetClient.returnClient()
query = ("""SELECT count(*) FROM `infusionsoft-looker-poc.asu_msba_customer_ltv.CONFIDENTIAL_ltv_oppty_table` LIMIT 10""")
query_job = client.query(query)  # API request - starts the query
Obj_Total_Records = query_job.result()
print()
print("======= Printing total records ==========")
print(Obj_Total_Records)
print()
print("======= Printed total records ==========")
# table = client.get_table('infusionsoft-looker-poc.asu_msba_customer_ltv.CONFIDENTIAL_ltv_oppty_table')
# ColumnNamesList = list(c.name for c in table.schema)

