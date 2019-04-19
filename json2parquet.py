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



pdf1 = spark.read.json("file:///home/mapr/Apurva/assignment/average_sal_female.json")
pdf1.write.parquet("file:///home/mapr/Apurva/assignment/average_sal_female.parquet")

pdf2 = spark.read.json("file:///home/mapr/Apurva/assignment/max_sal_male.json")
pdf2.write.parquet("file:///home/mapr/Apurva/assignment/max_sal_male.parquet")

pdf3 = spark.read.json("file:///home/mapr/Apurva/assignment/max_sal_female.json")
pdf3.write.parquet("file:///home/mapr/Apurva/assignment/max_sal_female.parquet")

pdf4 = spark.read.json("file:///home/mapr/Apurva/assignment/average_sal_male.json")
pdf4.write.parquet("file:///home/mapr/Apurva/assignment/average_sal_male.parquet")


pdf5 = spark.read.json("file:///home/mapr/Apurva/assignment/org_details.json")
pdf5.write.parquet("file:///home/mapr/Apurva/assignment/org_details.parquet")