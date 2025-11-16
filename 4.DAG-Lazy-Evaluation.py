from pyspark.sql.functions import col
from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("DAG-Lazy-Evaluation").getOrCreate()

df=spark.read.format("csv").option("header","true").option("inferschema","true").option("mode","PERMISSIVE").load("flight_data.csv")

flight_data_repartition=df.repartition(3)

us_flight_data=df.filter("DEST_COUNTRY_NAME='United States'")

us_ind_flight_data=us_flight_data.filter((col("ORIGIN_COUNTRY_NAME") == "India" ) | (col("ORIGIN_COUNTRY_NAME") == "Singapore"))

total_flight_ind_sing=us_ind_flight_data.groupBy("DEST_COUNTRY_NAME").sum("count")

total_flight_ind_sing.show()