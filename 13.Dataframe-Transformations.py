from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit

spark=SparkSession.builder.appName("Dataframe-Transformations").getOrCreate()

df=spark.read.format("csv")\
    .option("header","true")\
    .option("inferSchema","true")\
    .load("DF-Write-Data.csv")

# Dataframe Transformations

# 1. Aliasing columns
df1=df.select(col("id").alias("empid"), col("name").alias("empname"))
df1.show()

# SQL way of aliasing columns
df.createOrReplaceTempView("Employee")
df2=spark.sql("select id as empid, name as empname from Employee")
df2.show()

# 2. Filtering data

df3=df.filter((col("salary")>150000) & (col("age")>18))
df3.show()

# SQL way of filtering data
df4=spark.sql("""Select * from Employee where salary>150000 and age>18""")
df4.show()

# 3. Literal values
df5=df.select("*",lit("Sharma").alias("LastName"))
df5.show()

# SQL way of adding literal values
df6=spark.sql("""select *, 'Sharma' as LastName from Employee""")
df6.show()

# 4. Distinct values
df7=df.select("name").distinct()
df7.show()

# SQL way of distinct values
df8=spark.sql("""Select distinct name from Employee""")
df8.show()

# 5. Sorting data
df9=df.orderBy("salary")
df9.show()

# SQL way of sorting data
df10=spark.sql("""Select * from Employee order by salary""")
df10.show()

# 6. Adding Columns
df11=df.withColumn("DoublePay",col("salary")*2)
df11.show()

# SQL way of adding columns
df12=spark.sql("""Select *,salary*2 as DoublePay from Employee""")
df12.show()

# 7. Casting Columns
df13=df.withColumn("id",col("id").cast("string"))
df13.printSchema()

# SQL way of casting columns
df14=spark.sql("""Select cast(id as string) as id from Employee""")
df14.printSchema()

# 8. Dropping Columns
df15=df.drop("address").show()

# SQL way of dropping columns
df16=spark.sql("""Select id,name,age,gender,salary from Employee""")
df16.show()