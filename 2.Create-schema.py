from pyspark.sql.types import StructType,StructField,StringType,IntegerType
from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("Create-Schema").getOrCreate()

DF1=spark.read.format("csv")\
    .option("header","true")\
    .option("inferschema","true")\
    .option("mode","Permissive")\
    .load("/Users/gauravvishwakarma/Downloads/2015-summary.csv")

DF1.show(5)

DF2=spark.read.format("csv")\
    .option("header","true")\
    .option("mode","Permissive")\
    .option("skipRows",1)\
    .schema(StructType([
        StructField("Origin-Country",StringType(),True),
        StructField("Destination-Country",StringType(),True),
        StructField("Count",IntegerType(),True)
    ]))\
    .load("/Users/gauravvishwakarma/Downloads/2015-summary.csv")

DF2.show(5)

# .option("skipRows",1) is used to skip the first row of data in csv file
# .schema() is used to define our own schema for dataframe instead of infering it from
# data in csv file

# we can also create schema using DDL( Data Definition Language) format as string
DF3=spark.read.format("csv")\
    .option("header","true")\
    .option("mode","Permissive")\
    .option("skipRows",1)\
    .schema("`Origin-Country` STRING, `Destination-Country` STRING, `Count` INT")\
    .load("/Users/gauravvishwakarma/Downloads/2015-summary.csv")

DF3.show(5)
