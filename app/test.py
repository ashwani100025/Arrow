#spark-submit --jars /usr/share/java/mysql-connector-j-8.0.32.jar  --driver-class-path /usr/share/java/mysql-connector-j-8.0.32.jar test.py

from pyspark.sql.functions import *
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("test").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

'''query = 'select count(*) from training.users'
print(query)
count_val = spark.read \
		.format("jdbc") \
		.option("url", 'jdbc:mysql://localhost:3306') \
		.option("query", query) \
    	.option("user", 'ashwani_kr') \
    	.option("password", 'ash21may') \
        .load() \
		.collect()[0][0]

print(type(count_val))'''

print('\n\n\n')

df = spark.read.csv("file:///home/ashwani/Documents/Ashwani/Arrow/dwh/users2.csv", header=True, inferSchema=True)
df.show()

pt_list = ['created_at']


partitions = df.select(pt_list).distinct()
partitions.show()
n = len(partitions.columns)
print(n)
lol_partition = partitions.collect()

base_path = "file:///home/ashwani/Documents/Ashwani/Arrow/dwh/users/t_training/t_users_info/"
path = ""
load_path_list = []
for i,ptn in enumerate(lol_partition):
	for j in range(n):
		
		path = f"{pt_list[j]} = {ptn[j]}/* "
		
		load_path_list.append(base_path+path)

print(load_path_list)

'''
'file:///home/ashwani/Documents/Ashwani/Arrow/dwh/users/t_training/t_users_info/created_at = 2021-06-22 13:45:00/*',
'file:///home/ashwani/Documents/Ashwani/Arrow/dwh/users/t_training/t_users_info/created_at = 2022-06-22 10:25:00/*',
'file:///home/ashwani/Documents/Ashwani/Arrow/dwh/users/t_training/t_users_info/created_at = 2021-06-21 09:24:00/*'
'''


'''
df = spark.read \
	.option("header", True) \
	.option("inferSchema", True) \
	.format("csv") \
	.load(["file:///home/ashwani/Documents/Ashwani/Arrow/dwh/users/t_training/t_users_info/pt_updated_at=2023-04-05/*",
		   'file:///home/ashwani/Documents/Ashwani/Arrow/dwh/users/t_training/t_users_info/pt_updated_at=2023-04-06/*',
		   'file:///home/ashwani/Documents/Ashwani/Arrow/dwh/users/t_training/t_users_info/pt_updated_at=2023-04-07/*',
		   'file:///home/ashwani/Documents/Ashwani/Arrow/dwh/users/t_training/t_users_info/pt_updated_at=2023-04-08/*',
		   'file:///home/ashwani/Documents/Ashwani/Arrow/dwh/users/t_training/t_users_info/pt_updated_at=2023-04-19/*'
		  ])


df.show()

'''

