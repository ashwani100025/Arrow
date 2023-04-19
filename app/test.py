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
df = spark.read.option("header",True).csv('file:///home/ashwani/Documents/Ashwani/users.csv')

pt_list = ['created_at', 'platform', 'city']

partitions = df.select(pt_list).distinct()
partitions.show()
n = len(partitions.columns)

max_val = df.select(max('created_at')).first()[0]
print(type(max_val))

lol_partition = partitions.collect()

'''
query = 'ALTER TABLE bronze.cp_user_status ADD IF NOT EXISTS '

for ptn in lol_partition:
	query += 'PARTITION ('
	for j in range(n):
		if j == (n-1):
			query += f"'{pt_list[j]}'='{ptn[j]}') "
			continue
		query += f"'{pt_list[j]}'='{ptn[j]}', "
	
'''


query = 'SELECT count(*) FROM bronze.cp_user_status WHERE ( '
		
for i,ptn in enumerate(lol_partition):
	if i > 0:
		query += 'OR ('
	else:
		query += '('
	for j in range(n):
		if j == (n-1):
			query += f"{pt_list[j]} = '{ptn[j]}') "
			continue
		query += f"{pt_list[j]} = '{ptn[j]}' AND "

query += ') AND updated_at > 1667766345'

print(query)


