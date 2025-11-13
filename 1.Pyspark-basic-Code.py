from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("Reading-CSV-File").getOrCreate() #Creating Spark Session

KidsDF=spark.read.format("csv")\
    .option("header","true")\
    .option("inferSchema","true")\
    .load("/Users/gauravvishwakarma/Downloads/Indian_Kids_Screen_Time.csv")

# header ture means it will read the headers as well
# inferSchema means it will assign the data type according to the data in csv 

KidsDF.show(10) # action to show the dataframe
# show() is an action that will show the df and takes number of rows as input
KidsDF.printSchema() # to print the schema of dataframe
# how to execute a SQL query on dataframe
KidsDF.createOrReplaceTempView("KidsScreenTime")

input("Press any key to continue...") # to pause the execution of code to see spark UI at localhost:4040