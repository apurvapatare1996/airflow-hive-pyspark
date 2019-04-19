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
	
# df = spark.read.load("file:///home/mapr/Apurva/parquet/userdata1.parquet")
# df2 = spark.read.load("file:///home/mapr/Apurva/parquet/userdata2.parquet")
# df2.printSchema()
	
spark.sql("CREATE TABLE IF NOT EXISTS arp.a3 (id INT,first_name string,last_name string,email string,gender string,ip_address string,cc string,country string,birthdate string,salary double,title string) STORED AS parquet" )
spark.sql("LOAD DATA LOCAL INPATH 'file:///home/mapr/Apurva/assignment/parquet/userdata1.parquet' INTO TABLE arp.a3")
spark.sql("LOAD DATA LOCAL INPATH 'file:///home/mapr/Apurva/assignment/parquet/userdata2.parquet' INTO TABLE arp.a3")
spark.sql("LOAD DATA LOCAL INPATH 'file:///home/mapr/Apurva/assignment/parquet/userdata3.parquet' INTO TABLE arp.a3")
spark.sql("LOAD DATA LOCAL INPATH 'file:///home/mapr/Apurva/assignment/parquet/userdata4.parquet' INTO TABLE arp.a3")
spark.sql("LOAD DATA LOCAL INPATH 'file:///home/mapr/Apurva/assignment/parquet/userdata5.parquet' INTO TABLE arp.a3")

