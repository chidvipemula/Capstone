def returnCount(result):
    for row in result:
        total_column_records = row[0]
        return total_column_records
    return 0
def returnResult(client,query_string):
    query = (query_string)
    query_job = client.query(query)  # API request - starts the query
    return query_job.result()