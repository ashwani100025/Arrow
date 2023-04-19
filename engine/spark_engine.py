#spark-submit --jars /usr/share/java/mysql-connector-java-8.0.28.jar  --driver-class-path /usr/share/java/mysql-connector-java-8.0.28.jar spark_engine2.py
import sys
import json
from pyspark.sql.functions import *
import pyspark
from pyspark.sql import SparkSession 
#import pandas as pd
from datetime import datetime, timedelta

sys.path.append('/home/ashwani/Documents/Ashwani/Arrow')
import utils.utils as utils

spark = SparkSession.builder.master("local").appName("Arrow connector").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


sys.path.append('/home/ashwani/Documents/Ashwani/Arrow/connector')



def extract(row):
	print('\nextracting...')
	if row["source_db_type"] == 'mysql':
		import mysql_connector as source_connector
		query = source_connector.read_query_generator(row)
		print(query)

		df = source_connector.extract(row,query)
		df.show()
		return df

	elif row["source_db_type"] == 'file_system':
		import filesystem_connector as source_connector
		df = source_connector.extract(row)
		df.show()
		return df
	else:
		print("INVALID source_db_type!")



def transform(df,row=None):
	print("\ntransforming...")
	# if row["engine_transformation_query"] == None:
	# 	pass 
	# else:
	# 	pass 
	
	if(row["partition_strategy"] == None):
		print('no partition needed')
	else:
		print('partition needed')
		try:
			#base_str = 'df'
			for pt_col,value in row["partition_strategy"].items():
				if '(' in value:
					df = df.withColumn(f"{pt_col}", eval(f'{value}'))
				else:
					df = df.withColumn(f"{pt_col}", expr(f'{value}'))
			#df = df.withColumn("partition_dt", to_date(row["partition_column"]))
		except Exception as e:
			print(e)
		print("added partition_dt")
	
	
	return df

def load(df,row):
	print("\nloading...")
	if row["target_db_type"] == 'mysql':
		import mysql_connector as target_connector

	elif row["target_db_type"] == 'file_system':
		import filesystem_connector as target_connector
	else:
		print("INVALID target_db_type!")
	res = target_connector.load(df,row)

	if (res==True):
		return True
	else:
		return False


def start_engine(start_time,row):
	print("\n*** WELCOME TO SPARK ENGINE ***")
	#########EXTRACT
	utils.update_replication_batch_detail('update', {'set_query':"set status = 'EXTRACTION START' where replication_id = '{replication_id}'".format(replication_id = row["replication_id"])})
	df_extracted = extract(row)
	df_extracted.persist(pyspark.StorageLevel.MEMORY_ONLY).count()
	print("DATA EXTRACTED SUCCESSFULLY!")
	utils.update_replication_batch_detail('update', {'set_query':"set status = 'EXTRACTED' where replication_id = '{replication_id}'".format(replication_id = row["replication_id"])})
	print("extracting column names...")
	col_list = df_extracted.columns
	utils.update_replication_batch_detail('update', {'set_query':'set cols = "{cols}" where replication_id = "{replication_id}"'.format(replication_id = row["replication_id"], cols = col_list)})

	######### TRANSFORM
	df_transformed = transform(df_extracted,row)
	df_transformed.show()
	transformed_count = df_transformed.persist(pyspark.StorageLevel.MEMORY_ONLY).count()
	utils.update_replication_batch_detail('update', {'set_query':"set status = 'TRANSFORMED' where replication_id = '{replication_id}'".format(replication_id = row["replication_id"])})
	print("DATA TRANSFORMED!")

	######### LOAD
	is_success = load(df_transformed,row)
	if(is_success==True):
		end_time = datetime.now()
		utils.update_replication_batch_detail('update', {'set_query':"set status = 'LOADED' where replication_id = '{replication_id}'".format(replication_id = row["replication_id"])})
		print("DATA LOADED SUCCESSFULLY!")
		t_diff = end_time - start_time
		print((t_diff))
		utils.update_replication_batch_detail('update', {'set_query':"set duration = '{time_diff}' where replication_id = '{replication_id}'".format(replication_id = row["replication_id"], time_diff = t_diff)})
	cdc_column_max_value = df_transformed.agg(max(row["cdc_column"])).collect()[0][0]
	utils.update_replication_batch_detail('update', {'set_query':"set cdc_column_max_value = '{max_value}', replicated_record_count = '{transformed_count}'  where replication_id = '{replication_id}'".format(max_value = cdc_column_max_value, transformed_count = transformed_count, replication_id = row["replication_id"])})
	

