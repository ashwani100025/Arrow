from pyspark.sql.functions import *
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("Arrow").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


print('\n\n\n')

df = spark.read.csv("file:///home/ashwani/Documents/Ashwani/Arrow/dwh/users2.csv", header=True)
df.show()

df.createOrReplaceTempView('table')

df2 = spark.sql("select count(*) as ct, sum(mobile_number) as sm from table").collect()
print(df2[0],df2[1])
#val,val2 = df2.first()[0],df2.first()[1]

#print(val,val2)

'''
pt_list = ['created_at', 'platform', 'city']


partitions = df.select(pt_list).distinct()
partitions.show()
n = len(partitions.columns)
print(n)
lol_partition = partitions.collect()


query = f"SELECT * FROM bronze.cp_redeem_info WHERE "

for ptn in lol_partition:
			query += '('
			for j in range(n):
				if j == (n-1):
					query += f"{pt_list[j]} = '{ptn[j]}') OR "
					continue
				query += f"{pt_list[j]} = '{ptn[j]}' AND "

print(query)

print(query[-4:-1])

query = query[0:-4]
print(query)'''