import GetClient
import  pandas as pd
client = GetClient.returnClient()
opps_Quote, opps = GetClient.returnDataSet()
print(opps_Quote,'/n',opps)
query_string = 'SELECT count(*) ' + 'FROM ' + opps_Quote
print(query_string)
query = (query_string)
query_job = client.query(query)  # API request - starts the query
Obj_Total_Records = query_job.result()
for Row in Obj_Total_Records:
    print()
    print("======= Printing total records ==========")
    Total_Records= Row[0]
    print(Total_Records)
    print()
    print("======= Printed total records ==========")
table = client.get_table(opps)
ColumnNamesList = list(c.name for c in table.schema)
null_analysis_df = pd.DataFrame(columns=['Column_Name', 'Non_Null_Records', 'Percent_Null'])
for Column in ColumnNamesList:
    query_string = 'SELECT count('+ Column+') ' + 'FROM ' + opps_Quote
    query = (query_string)
    print(query_string)
    query_job = client.query(query)  # API request - starts the query
    obj_column_records_count = query_job.result()
    for row in obj_column_records_count:
        print()
        print("======= Printing total records ==========")
        total_column_records = row[0]
        print(total_column_records)
        if total_column_records == 0:
            print('======' + Column+' is empty =======')
        print()
        print("======= Printed total records ==========")
        percent_null = 1-(total_column_records/Total_Records)
        null_analysis_df = null_analysis_df.append({'Column_Name': Column, 'Non_Null_Records': total_column_records, 'Percent_Null': percent_null}, ignore_index=True)
null_analysis_df.to_csv('Null_Analysis.csv', index=False)
print('==============Created Null Analysis File=============')