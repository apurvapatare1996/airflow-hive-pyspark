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
	
#df = spark.read.load("file:///home/mapr/Apurva/parquet/userdata1.parquet")
#df2 = spark.read.load("file:///home/mapr/Apurva/parquet/userdata2.parquet")
#df2.printSchema()
	
spark.sql("CREATE TABLE IF NOT EXISTS arp.a3 (id INT,first_name string,last_name string,email string,gender string,ip_address string,cc string,country string,birthdate string,salary double,title string) STORED AS parquet" )
spark.sql("LOAD DATA LOCAL INPATH 'file:///home/mapr/Apurva/parquet/userdata1.parquet' INTO TABLE arp.a3")
spark.sql("LOAD DATA LOCAL INPATH 'file:///home/mapr/Apurva/parquet/userdata2.parquet' INTO TABLE arp.a3")
spark.sql("LOAD DATA LOCAL INPATH 'file:///home/mapr/Apurva/parquet/userdata3.parquet' INTO TABLE arp.a3")
spark.sql("LOAD DATA LOCAL INPATH 'file:///home/mapr/Apurva/parquet/userdata4.parquet' INTO TABLE arp.a3")
spark.sql("LOAD DATA LOCAL INPATH 'file:///home/mapr/Apurva/parquet/userdata5.parquet' INTO TABLE arp.a3")


sqlDF1 = spark.sql("SELECT avg(salary) as avg_sal_female FROM arp.a3 where gender='Female'")
sqlDF2 = spark.sql("SELECT max(salary) as max_sal_men FROM arp.a3 where gender='Male'")
sqlDF3 = spark.sql("SELECT max(salary) as max_sal_female FROM arp.a3 where gender='Female'")
sqlDF4 = spark.sql("SELECT avg(salary) as avg_sal_men FROM arp.a3 where gender='Male'")
sqlDF5 = spark.sql("select max(salary) as max_sal_of_org,avg(salary) as average_sal_of_org,count(gender='Male') as no_of_males,count(gender='Female') as no_of_females from arp.a3")

sqlDF1.write.json("file:///home/mapr/Apurva/average_sal_female.json")
sqlDF2.write.json("file:///home/mapr/Apurva/max_sal_male.json")
sqlDF3.write.json("file:///home/mapr/Apurva/max_sal_female.json")
sqlDF4.write.json("file:///home/mapr/Apurva/average_sal_male.json")
sqlDF5.write.json("file:///home/mapr/Apurva/org_details.json")


pdf1 = spark.read.json("file:///home/mapr/Apurva/average_sal_female.json")
pdf1.write.parquet("file:///home/mapr/Apurva/average_sal_female.parquet")

pdf2 = spark.read.json("file:///home/mapr/Apurva/max_sal_male.json")
pdf2.write.parquet("file:///home/mapr/Apurva/max_sal_male.parquet")

pdf3 = spark.read.json("file:///home/mapr/Apurva/max_sal_female.json")
pdf3.write.parquet("file:///home/mapr/Apurva/max_sal_female.parquet")

pdf4 = spark.read.json("file:///home/mapr/Apurva/average_sal_male.json")
pdf4.write.parquet("file:///home/mapr/Apurva/average_sal_male.parquet")


pdf5 = spark.read.json("file:///home/mapr/Apurva/org_details.json")
pdf5.write.parquet("file:///home/mapr/Apurva/org_details.parquet")




#sqlDF1.union(sqlDF2).show()


# sqlDF1 = sqlDF1.withColumn("id", monotonically_increasing_id())
# sqlDF2 = sqlDF2.withColumn("id", monotonically_increasing_id())
# sqlDF3 = sqlDF3.withColumn("id", monotonically_increasing_id())
# sqlDF4 = sqlDF4.withColumn("id", monotonically_increasing_id())
# sqlDF5 = sqlDF5.withColumn("id", monotonically_increasing_id())


# d1 = sqlDF5.join(sqlDF1,"id","outer").drop("id")
# # d1.show()
# d1 = d1.withColumn("id", monotonically_increasing_id())

# d2 = sqlDF2.join(sqlDF3,"id","outer").drop("id")
# # d2.show()
# d2 = d2.withColumn("id", monotonically_increasing_id())

# d3 = d1.join(d2,"id","outer").drop("id")
# # d3.show()
# d3 = d3.withColumn("id", monotonically_increasing_id())

# d4 = d3.join(sqlDF4,"id","outer").drop("id")
# d4.show()


# d4 = sqlDF4.join(sqlDF3,"id","outer").drop("id")
# #d4.show()

# d3 = d3.withColumn("id",monotonically_increasing_id())
# d4= d4.withColumn("id",monotonically_increasing_id())

# d5 = d4.join(d3,"id","outer").drop("id")
# #d5.show()



# d5 = d5.withColumn("id", monotonically_increasing_id())



# sqlDF5 = sqlDF5.withColumn("id", monotonically_increasing_id())


# d3.write.json("file:///home/mapr/Apurva/assignment2.json")

# df.select("first_name", "last_name","gender","email","country").write.save("file:///home/mapr/Apurva/employeeinfo.parquet")



