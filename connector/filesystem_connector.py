from pyspark.sql.functions import *
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("filesystem conncector").getOrCreate()
spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")
def extract(row):
	df  = spark.read \
		 .format(row["source_db_url"].split('|')[1]) \
		 .option("header",True) \
		 .load(row["source_db_url"].split('|')[0])

	return df

def load(s_df, row):
	pt_list = []
	for pt_col in row["partition_strategy"]:
		pt_list.append(pt_col)
		
	save_path = row["target_db_url"].split('|')[0] + '/' + row["target_db_name"]+ '/' + row["target_object_name"]

	if(row["cdc_type"]=="F"):
		write_mode='overwrite'
	if(row["cdc_type"]=="I"):
		write_mode='append'
	if(row["cdc_type"]=="U"):
		write_mode='overwrite'

	if(row["cdc_type"]=="F" or row["cdc_type"]=="I"):
		try:
			if(row["partition_strategy"] == None):
				s_df.write \
					.format(row["target_db_url"].split('|')[1]) \
					.option("header",True) \
					.mode(write_mode) \
					.save(save_path)
			else:
				
				s_df.write \
					.format(row["target_db_url"].split('|')[1]) \
					.option("header",True) \
					.mode(write_mode) \
					.partitionBy(pt_list) \
					.save(save_path)
			res = True
		except Exception as e:
				print(str(e))
				res =  False
	else:
		print('cdc_type is in "U" upsert mode')
		print("\nCreating load_path_list...")

		partitions = s_df.select(pt_list).distinct()
		partitions.show()
		n = len(partitions.columns)
		lol_partition = partitions.collect()

		base_path = save_path + "/"
		path = ""
		load_path_list = []
		for i,ptn in enumerate(lol_partition):
			for j in range(n):
				if j == n-1:
					path += f"{pt_list[j]}={ptn[j]}/*"
				else:
					path += f"{pt_list[j]}={ptn[j]}/"
				
			load_path_list.append(base_path+path)
			path=""

		print(load_path_list)

		historical_partition_list = []

		for path in load_path_list:
			try:
				tmp_df = spark.read \
						.option("header", True) \
						.option("inferSchema", True) \
						.format("csv") \
						.load(path)
				
				historical_partition_list.append(path)
			
			except:
				print("Path does not exist ", path)
		
		print(historical_partition_list)



		try:
			t_df = spark.read \
				.option("header", True) \
				.option("inferSchema", True) \
				.format("csv") \
				.load(historical_partition_list)
		except Exception as e:
			print(e)
		
		print("\nExtracting previous stored data from file_system according to required partitions\n")
		t_df = t_df.withColumn("pt_created_at", to_date('created_at'))
		t_df.show()

		
		s_df.createOrReplaceTempView(f'tmp_{row["source_object_name"]}')
		t_df.createOrReplaceTempView(f'tmp_{row["target_object_name"]}')
		spark.sql(f'SELECT * FROM tmp_{row["source_object_name"]}').show()
		spark.sql(f'SELECT * FROM tmp_{row["target_object_name"]}').show()
		print("Generating upsert query...")
		try:
			query = f'(SELECT t.* FROM tmp_{row["target_object_name"]} t LEFT JOIN tmp_{row["source_object_name"]} s ON t.id = s.id WHERE s.id is NULL) UNION ALL (SELECT * FROM tmp_{row["source_object_name"]})'
			print(query)
			print("\nExecuting upsert query...\n")
			df1 = spark.sql(query)
			df1.show()
			
			print("Loading df into filesystem...")
			df1.write \
			.format(row["target_db_url"].split('|')[1]) \
			.option("header",True) \
			.option("partitionOverwriteMode", "dynamic") \
			.mode(write_mode) \
			.partitionBy(pt_list) \
			.save(save_path)
			res = True
		except Exception as e:
			print(e)
			res = False
		
		
	
	return res


