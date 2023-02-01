from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# create a Spark session
spark = SparkSession.builder.appName("EnrichData").getOrCreate()

# load the data into a dataframe
df = spark.read.csv("data.csv", header=True, inferSchema=True)

# convert timestamp to timestamp type
df = df.withColumn("timestamp", to_timestamp(df["timestamp"], "yyyy-MM-dd'T'HH:mm:ss'Z'"))

# create a window to group data by userid and sort by timestamp
w = Window.partitionBy("userid").orderBy("timestamp")

# calculate the time difference between consecutive rows for each userid
df = df.withColumn("time_diff", unix_timestamp(lead("timestamp").over(w)) - unix_timestamp(col("timestamp")))

# create a new column to keep track of the session id
df = df.withColumn("session_id", sum(when(col("time_diff") > 1800, 1).otherwise(0)).over(w))

# update the session id by adding the userid and row number
df = df.withColumn("session_id", concat(col("userid"), lit("_"), col("session_id")))

# write the enriched data to parquet format
df.write.parquet("enriched_data.parquet")



