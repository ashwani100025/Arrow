import pymysql

def update_replication_batch_detail(type, data_dict):
	replication_framework_user = 'replication_user'
	replication_framework_user_pwd = 'user@123'
	replication_framework_db_name = 'replication_metadata'
	replication_framework_table_name = 'replication_batch_detail'
	replication_framework_host = 'localhost'
	replication_framework_port = 3306 
	#
	query = ''
	if (type == 'insert'):
		query = "insert into {database_name}.{table_name} (replication_id, mapping_id, created_at, status) values ('{replication_id}', '{mapping_id}', '{created_at}', '{status}' )".format(database_name = replication_framework_db_name, table_name = replication_framework_table_name, replication_id =data_dict['replication_id'], mapping_id = data_dict['mapping_id'], created_at = data_dict['created_at'], status = data_dict['status'])
	elif(type == 'update'):
		query = "update {database_name}.{table_name} {set_query}".format(database_name = replication_framework_db_name, table_name = replication_framework_table_name, set_query = data_dict["set_query"])
		print(query)
	con=pymysql.connect(host=replication_framework_host,port=replication_framework_port,user=replication_framework_user,password=replication_framework_user_pwd,database=replication_framework_db_name)
	#
	#print(query)
	con.query(query)
	con.commit()
	con.close()

def main():
	update_replication_batch_detail()

if __name__ == '__main__':
	main()
