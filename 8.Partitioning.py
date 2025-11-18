from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("Partition-and-Bucketing").getOrCreate()

df=spark.read.format("csv")\
    .option("header","true")\
    .option("inferSchema","true")\
    .load("DF-Write-Data.csv")

df.show()

# Writing data with partitioning

df.write.format("csv")\
    .option("header","true")\
    .mode("overwrite")\
    .partitionBy("address")\
    .option("path","Partitioned-Data")\
    .save()

# now to read Partioned data

partioned_df=spark.read.format("csv")\
    .option("header","true")\
    .option("inferSchema","true")\
    .load("Partitioned-Data")

partioned_df.show(truncate=False)

# if we want to partition data using multiple columns
# we can pass multiple column names in the partitionBy method
# THE ORDER OF COLUMNS IN PARTITIONBY MATTERS
# So if we have mentioned partitionBy("address","age") 
# then the data will be first partitioned by address and then within each address partition it will be further partitioned by age