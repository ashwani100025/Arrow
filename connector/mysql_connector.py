#import pandas as pd
import logging
import os
import pymysql
from datetime import datetime, timedelta
from pyspark.sql.functions import *
import pyspark
from pyspark.sql import SparkSession 

spark = SparkSession.builder.master("local").appName("MySQL Connector").getOrCreate()

'''
def read_query_generator(row):
	print("in read_query_generator...")
	if pd.isnull(row["source_transformation_query"]) == True:
		base_query = 'SELECT * FROM {db_name}.{table_name}'.format(db_name = row["source_db_name"], table_name = row["source_object_name"]) + ' WHERE {condition}'
	else:
		base_query = row["source_transformation_query"]
		
	if row["is_ever_success"] == 1:
		if row["cdc_type"] == 'F':
			query = base_query.format(condition = " 1=1 ")
		else:
			condition = "{cdc_column} > '{cdc_column_max_value}' ".format(cdc_column = row["cdc_column"], cdc_column_max_value = row["cdc_column_max_value"])
			query = base_query.format(condition = condition)
	else:
		query = base_query.format(condition = " 1=1 ") +' LIMIT 1000'


	return query
'''

def read_query_generator(row):
	print("in read_query_generator...")
	if row["source_transformation_query"] == None:
		base_query = 'SELECT * FROM {db_name}.{table_name}'.format(db_name = row["source_db_name"], table_name = row["source_object_name"]) + ' WHERE {condition}'
	else:
		base_query = row["source_transformation_query"]
		
	if row["is_ever_success"] == 1:
		if row["cdc_type"] == 'F':
			query = base_query.format(condition = " 1=1 ")
		else:
			condition = "{cdc_column} > '{cdc_column_max_value}' ".format(cdc_column = row["cdc_column"], cdc_column_max_value = row["cdc_column_max_value"])
			query = base_query.format(condition = condition)
	else:
		query = base_query.format(condition = " 1=1 ") +' LIMIT 1000'


	return query

	

def extract(row,query):
	#extracting table to df
	df = spark.read \
		.format("jdbc") \
		.option("url", row["source_db_url"]) \
		.option("query", query) \
    	.option("user", row["source_user_name"]) \
    	.option("password", row["source_password"]) \
		.load()

	return df


def load(df,row):

	if(row["cdc_type"]=="F"):
		write_mode='overwrite'
	if(row["cdc_type"]=="I"):
		write_mode='append'
	if(row["cdc_type"]=="U"):
		print("Upsert in filesystem NOT supported")
		res = False

	if(row["cdc_type"]=="F" or row["cdc_type"]=="I"):
		try:
			if(row["partition_strategy"] == None):
				df.write \
				.format("jdbc") \
				.option("url", row["target_db_url"] +"/"+ row["target_db_name"]) \
				.option("user", row["target_user_name"]) \
			    .option("password", row["target_password"]) \
				.option("header", True) \
				.option("dbtable", row["target_object_name"]) \
				.mode(write_mode) \
				.save()
			else:
				pass

			res = True
		except Exception as e:
				print(str(e))
				res =  False

	return res




def object_existance_test(con,tb_name):
	cur = con.cursor()
	cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{0}'".format(tb_name.replace('\'', '\'\'')))
	if cur.fetchone()[0] == 1:
		cur.close()
		return True
	cur.close()
	return True


con=pymysql.connect(host="localhost",port=3306,user="ashwani_kr",password="ash21may",database="training")

tb = 'users'
res = object_existance_test(con,tb)
print(res)

con.close()
	
