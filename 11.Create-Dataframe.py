from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("Create-Dataframe").getOrCreate()

mydata=[(1,2),(2,3),(3,4),(4,5),(5,6)]

schema=["id","value"]

df=spark.createDataFrame(data=mydata,schema=schema)
df.show()