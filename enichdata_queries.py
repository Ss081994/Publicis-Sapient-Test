# create a Spark session
spark = SparkSession.builder.appName("EnrichDataQueries").getOrCreate()

# read the enriched data from the parquet file
df = spark.read.parquet("enriched_data.parquet")

# register the dataframe as a temporary view
df.createOrReplaceTempView("enriched_data")

# query to get the number of sessions generated in a day
sessions_per_day = spark.sql("SELECT date(timestamp) as date, count(distinct session_id) as sessions_per_day FROM enriched_data group by date(timestamp)")

# query to get the total time spent by a user in a day
time_spent_per_day = spark.sql("SELECT userid, date(timestamp) as date, sum(time_diff) as time_spent_per_day FROM (SELECT userid, timestamp, lead(timestamp) over w - timestamp as time_diff FROM enriched_data window w as (partition by userid, date(timestamp) order by timestamp)) group by userid, date(timestamp)")

# query to get the total time spent by a user over a month
time_spent_per_month = spark.sql("SELECT userid, date_trunc('month', timestamp) as month, sum(time_diff) as time_spent_per_month FROM (SELECT userid, timestamp, lead(timestamp) over w - timestamp as time_diff FROM enriched_data window w as (partition by userid, date_trunc('month', timestamp) order by timestamp)) group by userid, date_trunc('month', timestamp)")




