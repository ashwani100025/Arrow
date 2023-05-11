import pymysql
import ast

replication_framework_user = 'datapit-rw'
replication_framework_user_pwd = 'F9WfD24UztfYxkNj2VIT'
replication_framework_db_name = 'replication_metadata'
replication_framework_table_name = 'pipelinetablelogs'
replication_framework_host = 'replication-metadata.c4qiefxr6q2c.ap-south-1.rds.amazonaws.com'
replication_framework_port = 3306



def make_rows(id):
    conn=pymysql.connect(host=replication_framework_host,port=replication_framework_port,user=replication_framework_user,password=replication_framework_user_pwd,database=replication_framework_db_name)
    cursor = conn.cursor()
    
    source_query = f'SELECT D.name as parameter_name, E.value as parameter_value, F.pipeline_id, F.id as pipelinetable_id, F.sourceconnector_id as source_db_id, F.type, B.name as source_db_type, A.db_name as source_db_name, F.sourcetable, F.source_primary_key, F.sourcesql, F.sourcequalitycol FROM connectors A JOIN pipelinetables F ON A.id = F.sourceconnector_id JOIN connectortypes B ON A.connectortype_id = B.id JOIN connectortypeparameters C ON B.id = C.connectortype_id JOIN parameters D ON C.parameter_id = D.id JOIN connectorparameters E ON C.id = E.connectortypeparameter_id and A.id = E.connector_id WHERE F.id = {id}'
    cursor.execute(source_query)
    source_results = cursor.fetchall()
    source_num_fields = len(cursor.description)
    source_field_names = [i[0] for i in cursor.description]

    destination_query = f'SELECT D.name as parameter_name, E.value as parameter_value, F.destinationconnector_id as destionation_db_id, B.name as destination_db_type, F.destinationtable, A.db_name as destination_database, F.file_format, F.destinationqualitycol, F.qualitytype, F.merge_condition, F.window_value, F.window_type, F.cdc_column, F.partition_strategy, F.create_table_ddl FROM connectors A JOIN pipelinetables F ON A.id = F.destinationconnector_id JOIN connectortypes B ON A.connectortype_id = B.id JOIN connectortypeparameters C ON B.id = C.connectortype_id JOIN parameters D ON C.parameter_id = D.id JOIN connectorparameters E ON C.id = E.connectortypeparameter_id and A.id = E.connector_id WHERE F.id = {id}'
    cursor.execute(destination_query)
    destination_results = cursor.fetchall()
    destination_num_fields = len(cursor.description)
    destination_field_names = [i[0] for i in cursor.description]

    cdc_query = f'SELECT case when count(*) > 0 then 1 else 0 end as is_ever_success,  max(cdc_column_max_value) as cdc_column_max_value from pipelinetablelogs where pipelinetable_id = {id} and completed = 1'
    cursor.execute(cdc_query)
    cdc_results = cursor.fetchone()
    cdc_num_fields = len(cursor.description)
    cdc_field_names = [i[0] for i in cursor.description]

    conn.close()

    temp_dict = {}



    #storing source config values to dict
    for i in range(2, source_num_fields):
        temp_dict[source_field_names[i]] = source_results[0][i]


    for i in range(len(source_results)):
        temp_dict[source_results[i][0]] = source_results[i][1]

    if temp_dict['username']:
        temp_dict['source_user_name'] = temp_dict['username']
        del temp_dict['username']

    if temp_dict['password']:
        temp_dict['source_password'] = temp_dict['password']
        del temp_dict['password']

    #storing source_db_url
    if temp_dict['source_db_type'] == 'mariadb':
        temp_dict['source_db_url'] = temp_dict['host']+':'+temp_dict['port']+'/'+temp_dict['source_db_name']+'?permitMysqlScheme'
        del temp_dict['host']
        del temp_dict['port']




    #storing destination config values to dict
    for i in range(2, destination_num_fields):
        temp_dict[destination_field_names[i]] = destination_results[0][i]


    for i in range(len(destination_results)):
        temp_dict[destination_results[i][0]] = destination_results[i][1]

    #storing destination_db_url
    if temp_dict['destination_db_type'] == 'file_system':
        temp_dict['destination_db_url'] = temp_dict['path']+'/'+temp_dict['destination_database']
        del temp_dict['path']

    #formatting partition_strategy
    temp_dict['partition_strategy'] = ast.literal_eval(temp_dict['partition_strategy'])




    #storing cdc values to temp_dict
    for i in range(cdc_num_fields):
        temp_dict[cdc_field_names[i]] = cdc_results[i]

    return temp_dict





def check_status(replication_id):
    conn=pymysql.connect(host=replication_framework_host,port=replication_framework_port,user=replication_framework_user,password=replication_framework_user_pwd,database=replication_framework_db_name)
    cursor = conn.cursor()
    query = f'SELECT status FROM {replication_framework_table_name} WHERE replication_id = "{replication_id}"'
    cursor.execute(query)
    res = cursor.fetchone()
    status = res[0]
    conn.close()

    return status

print(check_status('2023-05-09 13:05:39~2'))

