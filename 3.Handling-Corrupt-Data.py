from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark=SparkSession.builder.appName("Handling-Corrupt-Data").getOrCreate()

df=spark.read.format("csv")\
    .option("header","true")\
    .option("inferSchema","true")\
    .option("mode","permissive")\
    .load("Corrupted-data.csv")   

df.show()

# Handling corrupt data using different modes

# using Failfast mode will give no result if the data has a corrupted record
# using dropmalformed mode will drop the corrupted record and give the rest of the data

# using permissive mode will put null value in place of corrupted record and give the rest of the data

# to view the corrupt record in permissive mode we can add a new column _corrupt_record to the dataframe

schema=StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("salary", IntegerType(), True),
    StructField("address", StringType(), True),
    StructField("nominee", StringType(), True),
    StructField("_corrupt_record", StringType(), True) # it will automatically add this column to view corrupt records
])

df2=spark.read.format("csv")\
    .option("header","true")\
    .option("mode","permissive")\
    .option("inferSchema","true")\
    .schema(schema)\
    .load("Corrupted-data.csv")

df2.show(truncate=False)

# to store corrupt records in a separate file we can use the option "badRecordsPath"

df3=spark.read.format("csv")\
    .option("header","true")\
    .option("inferSchema","true")\
    .option("badRecordsPath","/Users/gauravvishwakarma/Desktop/Code/PySpark-Basics/badRecords")\
    .schema(schema)\
    .load("Corrupted-data.csv")

df3.show(truncate=False)

# now the corrupt records will be stored in the specified path and the rest of the data will be shown

#to view the corrupt records stored in the specified path
corrupt_df=spark.read.format("json")\
    .load("/Users/gauravvishwakarma/Desktop/Code/PySpark-Basics/badRecords")        

corrupt_df.show(truncate=False)
