from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("Reading-Parquet-File").getOrCreate()

df=spark.read.parquet("part-r-00000-1a9822ba-b8fb-4d8e-844a-ea30d0801b9e.gz.parquet")

df.show(truncate=False)