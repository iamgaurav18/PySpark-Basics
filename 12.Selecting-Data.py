from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import expr

spark=SparkSession.builder.appName("Selecting-Data").getOrCreate()

df=spark.read.format("csv")\
    .option("header","true")\
    .option("inferSchema","true")\
    .load("DF-Write-Data.csv")
df.show()

# String methods for selecting data
df2=df.select("id","name",)
df2.show()

# Selecting data using column object
df3=df.select(col("id"),col("name"),col("salary"))
df3.show()

# changing values while selecting data
df4=df.select(col("id"),(col("salary")*2))
df4.show()

#other ways to select data like list and object
df5=df.select("id",col("name"),df.salary,df["gender"])
df5.show()

# aliasing while selecting data
df6=df.select(col("id").alias("identifier"),col("name").alias("fullname"))
df6.show()

# selecting data using expressions
df7=df.select(expr("id as empid"),expr("salary *3 as tripple_salary"))
df7.show()

# SQL way of selecting data
sqldf=df.createOrReplaceTempView("EMPLOYEE")
result=spark.sql("""select name from EMPLOYEE""")
result.show()