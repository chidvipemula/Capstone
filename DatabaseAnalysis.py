import GetClient
import pandas as pd
import utils
client = GetClient.returnClient()
opps_Quote, opps = GetClient.returnDataSet()
Total_Records = utils.returnCount(utils.returnResult(client, 'SELECT count(*) ' + 'FROM ' + opps_Quote))
print(Total_Records)
table = client.get_table(opps)
ColumnNamesList = list(c.name for c in table.schema)
print(ColumnNamesList)
null_analysis_df = pd.DataFrame(columns=['Column_Name', 'Non_Null_Records', 'Percent_Null'])
print(null_analysis_df)
for Column in ColumnNamesList:
    print(Column)
    total_column_records = utils.returnCount(utils.returnResult(client, 'SELECT count(' + Column+') ' + 'FROM ' +
                                                                opps_Quote))
    percent_null = 1-(total_column_records/Total_Records)
    null_analysis_df = null_analysis_df.append({'Column_Name': Column, 'Non_Null_Records': total_column_records,
                                                'Percent_Null': percent_null}, ignore_index=True)
# null_analysis_df.to_csv('Null_Analysis.csv', index=False)
print('==============Created Null Analysis File=============')