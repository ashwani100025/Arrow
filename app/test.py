#spark-submit --jars /usr/share/java/mysql-connector-j-8.0.32.jar  --driver-class-path /usr/share/java/mysql-connector-j-8.0.32.jar test.py

from pyspark.sql.functions import *
import pyspark
from pyspark.sql import SparkSession
import ast
import sys

spark = SparkSession.builder.master("local").appName("Arrow").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

my_list = ast.literal_eval(sys.argv[1])
print('\n')
print(my_list)
print('\n')
print(type(my_list))
print(sys.argv[0])


