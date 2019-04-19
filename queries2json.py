from os.path import expanduser, join
import json
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import monotonically_increasing_id

# warehouse_location points to the default location for managed databases and tables
warehouse_location = 'spark-warehouse'

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Hive integration example") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()


sqlDF1 = spark.sql("SELECT avg(salary) as avg_sal_female FROM arp.a3 where gender='Female'")
sqlDF2 = spark.sql("SELECT max(salary) as max_sal_men FROM arp.a3 where gender='Male'")
sqlDF3 = spark.sql("SELECT max(salary) as max_sal_female FROM arp.a3 where gender='Female'")
sqlDF4 = spark.sql("SELECT avg(salary) as avg_sal_men FROM arp.a3 where gender='Male'")
sqlDF5 = spark.sql("select max(salary) as max_sal_of_org,avg(salary) as average_sal_of_org,count(gender='Male') as no_of_males,count(gender='Female') as no_of_females from arp.a3")

sqlDF1.write.json("file:///home/mapr/Apurva/assignment/average_sal_female.json")
sqlDF2.write.json("file:///home/mapr/Apurva/assignment/max_sal_male.json")
sqlDF3.write.json("file:///home/mapr/Apurva/assignment/max_sal_female.json")
sqlDF4.write.json("file:///home/mapr/Apurva/assignment/average_sal_male.json")
sqlDF5.write.json("file:///home/mapr/Apurva/assignment/org_details.json")