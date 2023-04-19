from pyspark.sql.functions import *
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("filesystem conncector").getOrCreate()

def extract(row):
	df  = spark.read \
		 .format(row["source_db_url"].split('|')[1]) \
		 .option("header",True) \
		 .load(row["source_db_url"].split('|')[0])

	return df

def load(df, row):
	pt_list = []
	for pt_col in row["partition_strategy"]:
		pt_list.append(pt_col)
		
	save_path = row["target_db_url"].split('|')[0] + '/' + row["target_db_name"]+ '/' + row["target_object_name"]

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
					.format(row["target_db_url"].split('|')[1]) \
					.option("header",True) \
					.mode(write_mode) \
					.save(save_path)
			else:
				
				df.write \
					.format(row["target_db_url"].split('|')[1]) \
					.option("header",True) \
					.mode(write_mode) \
					.partitionBy(pt_list) \
					.save(save_path)
			res = True
		except Exception as e:
				print(str(e))
				res =  False

	return res


