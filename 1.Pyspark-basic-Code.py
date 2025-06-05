# importing Sparksession to create a spark session
from pyspark.sql import SparkSession
import time

# creating a spark session
sparks=SparkSession.builder \
    .appName("PySpark Basic Code") \
    .master("local") \
    .getOrCreate()

# master is the spark master URL, local means it will run on the local machine
# appName is the name of the application, it can be anything you want
# getOrCreate() will create a new spark session if it does not exist, otherwise it will return the existing one
# we can also select the master URL as "local[2]" to use 2 cores of the local machine
# or "local[*]" to use all available cores of the local machine

sc=sparks.sparkContext # creating a spark context since we will be dealing with RDDs 
# in case of dataframe we can directly use sparks
# sc is the spark context, it is used to create RDDs and perform operations on them

rdd=sc.parallelize([1,2,3,4,5,6])
# parallelize is used to create an RDD from a list, it will distribute the data across the cluster

result=rdd.map(lambda x: x*x).collect()
print("Number of partitions in RDD:", rdd.getNumPartitions()) # getting the number of partitions in the RDD

print(result)

#time.sleep(60) 
# used to pause the execution for 60 seconds, useful for debugging or waiting for the job to complete and see details on
# the spark UI at http://localhost:4040

print("stooping the spark session")
sparks.stop()  # stopping the spark session
