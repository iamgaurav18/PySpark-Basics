from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Writing-Data").getOrCreate()

df=spark.read.format("csv")\
    .option("header","true")\
    .option("inferSchema","true")\
    .load("DF-Write-Data.csv")

df.show()

df.write.format("csv")\
    .option("header","true")\
    .mode("overwrite")\
    .save("Output-CSV-Data") # this will only create one part- file in the specified folder

df2=spark.read.format("csv")\
    .option("inferSchema","true")\
    .option("header","true")\
    .load("Output-CSV-Data")

df2.show(truncate=False)