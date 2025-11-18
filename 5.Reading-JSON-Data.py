from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("Reading-JSON-Data").getOrCreate()

df1=spark.read.format("json")\
    .option("inferSchema","true")\
    .option("mode","permissive")\
    .load("line_delimited_json.json")

df1.show(truncate=False)

df2=spark.read.format("json")\
    .option("inferSchema","true")\
    .option("mode","permissive")\
    .load("corrupted-json.json")

df2.show(truncate=False)  # shows corrupted records in a new column _corrupt_record

df3=spark.read.format("json")\
    .option("inferSchema","true")\
    .option("mode","permissive")\
    .option("multiline","true")\
    .load("multiline-correct.json") # we have added multiline option to read multiline json files

df3.show(truncate=False)

df4=spark.read.format("json")\
    .option("inferSchema","true")\
    .option("mode","permissive")\
    .option("multiline","true")\
    .load("multiline-incorrect.json") # it will only process till it encounters the first corrupted record

df4.show(truncate=False)

df5=spark.read.format("json")\
    .option("inferSchema","true")\
    .option("mode","permissive")\
    .load("single_file_json with extra fields.json") # it will add a new column for that extra field and put null for other records    

df5.show(truncate=False)