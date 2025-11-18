from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("Bucketing").getOrCreate()

df=spark.read.format("csv")\
    .option("header","true")\
    .option("inferSchema","true")\
    .load("DF-Write-Data.csv")

df.show()

# Writing data with bucketing

df.write.format("csv")\
    .option("header","true")\
    .mode("overwrite")\
    .bucketBy(3,"id")\
    .option("path","Bucketed-Data")\
    .saveAsTable("bucketed_table")# we need to save as table to use bucketing

# now to read Bucketed data
bucketed_df=spark.read.format("csv")\
    .option("header","true")\
    .option("inferSchema","true")\
    .load("spark-warehouse/Bucketed-Data")

bucketed_df.show(truncate=False)

# use repartition before writing bucketed data for better distribution of data across buckets
# because sometimes data may not be evenly distributed across buckets which may lead to data skewness