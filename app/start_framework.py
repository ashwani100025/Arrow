#spark-submit --jars /usr/share/java/mysql-connector-j-8.0.32.jar  --driver-class-path /usr/share/java/mysql-connector-j-8.0.32.jar start_framework.py

from pyspark.sql.functions import *
import pyspark
from pyspark.sql import SparkSession 
#import pandas as pd
import sys
import json
import logging
import subprocess
import os
from datetime import datetime, timedelta

spark = SparkSession.builder.master("local").appName("Arrow").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

sys.path.append('/home/ashwani/Documents/Ashwani/Arrow')
import utils.utils as utils
import engine.spark_engine as engine




project_path = '/home/ashwani/Documents/Ashwani/Arrow/'
log_directory = project_path + 'log/'
todays_log_directory = log_directory + datetime.strftime(datetime.now(), '%Y%m%d') + '/'
log_file_path = todays_log_directory + datetime.strftime(datetime.now(), '%F_%T') + '.log'
os.makedirs(todays_log_directory, mode = 0o777, exist_ok = True)

logging.basicConfig(filename = log_file_path, format='%(asctime)s %(message)s', filemode='a')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def start_etl(start_time,row):
	objective = "starting ETL...('{mode}' mode)".format(mode = row["cdc_type"])
	print(objective)
# 	#sp.welcome(row)
# 	dict1 = row.to_dict()
# 	converted = json.dumps(dict1)
# 	call_file = "spark-submit --jars /usr/share/java/mysql-connector-java-8.0.28.jar  --driver-class-path /usr/share/java/mysql-connector-java-8.0.28.jar /home/ashwani/Documents/Ashwani/Arrow/engine/spark_engine.py '" + converted + "'"
# 	p1 = subprocess.Popen(call_file, shell=True)
	engine.start_engine(start_time,row)


def is_target_object_exists(row):
	print("within is_target_object_exists: checking target table is there or not")
	pass
def create_table_using_ddl(row):
	print("within create_table_using_ddl: creating table using given ddl")
	pass
	return True
def create_dynamic_table(row):
	print("within create_dynamic_table: creating dynamic table")
	pass
	return True
def sample_data_replication_check(mapping_id):
	print("within sample_data_replication_check: checking successfull status")
	pass
	return True

def get_workoad():
	data = [{
				'mapping_id'					:		1,
				'source_user_name'				:		'ashwani_kr',
				'source_password'				:		'ash21may',
				'source_db_id'					:		101,
				'source_db_type'				:		'mysql',
				'source_db_url'					:		'jdbc:mysql://localhost:3306',
				'source_db_name'				:		'training',
				'source_object_id'				:		1,
				'source_object_name'			:		'users',
				'source_transformation_query'	:		None,
				'target_user_name'				:		'ashwani-PC',
				'target_password'				:		'Pc@123',
				'target_db_id'					:		111,
				'target_db_type'				:		'file_system',
				'target_object_id'				:		2,
				'target_object_name'			:		't_users_info',
				'target_db_name'				:		't_training',
				'target_db_url'					:		'file:///home/ashwani/Documents/Ashwani/Arrow/dwh/users|csv',
				'is_ever_success'				:		1,
				'cdc_type'						:		'F',
				'cdc_column'					:		'updated_at',
				'partition_strategy'			:		{
															'pt_updated_at'	:	"to_date('updated_at')"
														},
				'cdc_column_max_value'			:		None,
				'create_table_ddl'				:		'query'
			},
			{
				'mapping_id'					:		2,
				'source_user_name'				:		'ashwani_kr',
				'source_password'				:		'ash21may',
				'source_db_id'					:		102,
				'source_db_type'				:		'mysql',
				'source_db_url'					:		'jdbc:mysql://localhost:3306',
				'source_db_name'				:		'training',
				'source_object_id'				:		2,
				'source_object_name'			:		'purchase_details',
				'source_transformation_query'	:		None,
				'target_user_name'				:		'ashwani-PC',
				'target_password'				:		'Pc@123',
				'target_db_id'					:		111,
				'target_db_type'				:		'file_system',
				'target_object_id'				:		3,
				'target_object_name'			:		't_purchase_info',
				'target_db_name'				:		't_training',
				'target_db_url'					:		'file:///home/ashwani/Documents/Ashwani/Arrow/dwh/purchase_details|csv',
				'is_ever_success'				:		1,
				'cdc_type'						:		'I',
				'cdc_column'					:		'updated_at',
				'partition_strategy'			:		None,
				'cdc_column_max_value'			:		'2023-03-10 10:56:10',
				'create_table_ddl'				:		'query'
			},
			{
				'mapping_id'					:		3,
				'source_user_name'				:		'ashwani-PC',
				'source_password'				:		'Pc@123',
				'source_db_id'					:		111,
				'source_db_type'				:		'file_system',
				'source_db_url'					:		'file:///home/ashwani/Documents/Ashwani/Arrow/dwh/users2.csv|csv',
				'source_db_name'				:		's_training',
				'source_object_id'				:		3,
				'source_object_name'			:		'users2',
				'source_transformation_query'	:		None,
				'target_user_name'				:		'ashwani_kr',
				'target_password'				:		'ash21may',
				'target_db_id'					:		102,
				'target_db_type'				:		'mysql',
				'target_object_id'				:		111,
				'target_object_name'			:		'users_load',
				'target_db_name'				:		'training',
				'target_db_url'					:		'jdbc:mysql://localhost:3306',
				'is_ever_success'				:		1,
				'cdc_type'						:		'F',
				'cdc_column'					:		'created_at',
				'partition_strategy'			:		None,
				'cdc_column_max_value'			:		'2023-03-10 10:56:10',
				'create_table_ddl'				:		'query'
			}]

	#workload_df = pd.DataFrame(data)
	return data

def old_object_etl(start_time, row):
	print("Enterd in old_object_etl")
	if row["cdc_type"] == 'F':
		logger.info("starting ETL with full mode")
		start_etl(start_time,row)
	elif row["cdc_type"] == 'I' or row["cdc_type"] == 'U':
		print("cdc_type is in ('I','U')")
		if row["cdc_column_max_value"] == None:
			logger.error("cdc_column_max_value SHOULD NOT BE NULL for 'I' and 'U' cdc_type")
		else:
			start_etl(start_time,row)
			
			
	else:
		logger.error("INVALID cdc_type / NOT IN ('F','I','U')")

'''
def new_object_etl(row):
	if is_target_object_exists(row) == True:
		print("Target table already created")
		start_etl(row)
	else:
		print("Target table NOT created")
		if pd.notnull(row["create_table_ddl"]) == True:
			table_created_flag = create_table_using_ddl(row)
			if table_created_flag == True:
				start_etl(row)
			else:
				logger.error("Failed to create table using given ddl / check for correct syntax given")
		else:
			table_created_flag = create_dynamic_table(row)
			if table_created_flag == True:
				start_etl(row)
			else:
				logger.error("Failed to create dynamic table")
	
	sample_data_replication_check_flag = sample_data_replication_check(row["mapping_id"])
	if (sample_data_replication_check_flag == True):
		row_changed = row
		row_changed["is_ever_success"] = 1
		old_object_etl(row_changed)
'''

def new_object_etl(start_time, row):
	if is_target_object_exists(row) == True:
		print("Target table already created")
		start_etl(start_time, row)
	else:
		print("Target table NOT created")
		if row["create_table_ddl"] == None:
			table_created_flag = create_dynamic_table(row)
			if table_created_flag == True:
				start_etl(start_time, row)
			else:
				logger.error("Failed to create dynamic table")
		
		else:
			table_created_flag = create_table_using_ddl(row)
			if table_created_flag == True:
				start_etl(start_time, row)
			else:
				logger.error("Failed to create table using given ddl / check for correct syntax given")
			
	
	sample_data_replication_check_flag = sample_data_replication_check(row["mapping_id"])
	if (sample_data_replication_check_flag == True):
		row_changed = row
		row_changed["is_ever_success"] = 1
		old_object_etl(row_changed)


def driver(row):
	start_time = datetime.now()
	created_time = datetime.strftime(start_time, '%F %T')
	replication_id = created_time + '~' + str(row["mapping_id"])
	batch_detail_data = {}
	batch_detail_data['replication_id'] = replication_id
	batch_detail_data['mapping_id'] = row["mapping_id"]
	batch_detail_data['created_at'] = created_time
	batch_detail_data['status'] = 'START'
	utils.update_replication_batch_detail('insert', batch_detail_data)
	row['replication_id'] = replication_id
	if row["is_ever_success"] == 1:
		print("OLD OBJECT FOUND")
		old_object_etl(start_time, row)
	elif row["is_ever_success"] == 0:
		print("NEW OBJECT FOUND")
		new_object_etl(start_time, row)
	else:
		logger.debug("is is_ever_success NOT IN (1 and 0)")



def main():
	logger.info('Entered in main')
	print("\n\ngetting workload_df...")
	workload_df = get_workoad()
	print("GOT workload_df")
	#for index,row in workload_df.iterrows():
	for index,row in enumerate(workload_df):
		print(f"\n\n################# calling for row {index} #################")
		logger.info(f"################# calling for row {index} #################")

		try:
			driver(row)
		except Exception as e:
			logger.error(str(e))
		logger.info(f"################# completed for row {index} #################")
		print(f"\n\n################# completed for row {index} #################")


    	

if __name__ == '__main__':
	main()
