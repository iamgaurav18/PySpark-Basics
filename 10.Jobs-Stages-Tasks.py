from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark=SparkSession.builder.appName("Jobs-Stages-Tasks").getOrCreate()

df=spark.read.format("csv")\
    .option("header","true")\
    .option("inferSchema","true")\
    .load("DF-Write-Data.csv")

#print(df.rdd.getNumPartitions())   to get number of partitions
#df=df.repartition(2)  # to repartition the data into specified number of partitions (wide transformation)

df=df.filter(col("salary")>90000)\
    .select("id","name","age","salary")\
    .groupBy("age").count() # wide transformation

df.collect()  # action to trigger the job
input("Press Enter to exit...")
