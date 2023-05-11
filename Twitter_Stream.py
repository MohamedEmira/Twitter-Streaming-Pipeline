import pyspark 
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

spark = SparkSession.builder \
    .appName("Create Hive Table") \
    .config("spark.sql.catalogImplementation", "hive") \
    .enableHiveSupport() \
    .getOrCreate()
spark.conf.set("spark.sql.sources.default", "org.apache.spark.sql.execution.streaming.MemoryStreamSource")

tweet_df = spark \
    .readStream \
    .format("socket") \
    .option("host", "127.0.0.1") \
    .option("port", 7777) \
    .load()

tweet_df_string = tweet_df.selectExpr("CAST(value AS STRING)")

writeTweet = tweet_df_string.writeStream. \
    outputMode("append"). \
    format("memory"). \
    queryName("tweetquery"). \
    trigger(processingTime='2 seconds'). \
    start()
time.sleep(60)

data = spark.sql("select * from tweetquery")
schema = StructType([
    StructField("id", StringType()),
    StructField("text", StringType()),
    StructField("in_reply_to_user_id", LongType()),
    StructField("created_at", TimestampType()),
    StructField("public_metrics", StringType()),
    StructField("source", StringType()),
    StructField("username", StringType()),
    StructField("user_metrics", StringType())
])

split_df = data.select(split(col("value"), "\n").alias("split_value")) \
               .select(explode(col("split_value")).alias("value"))

parsed_df = split_df.select(from_json(col("value"), schema).alias("json")) \
                   .select("json.id", "json.text","json.in_reply_to_user_id","json.created_at","json.public_metrics","json.source","json.username","json.user_metrics")

metrics_schema = "struct<retweet_count:int, reply_count:int, like_count:int, quote_count:int>"
user_metrics_schema = "struct<followers_count:int, following_count:int, tweet_count:int, listed_count:int>"
parsed_df = parsed_df.withColumn("public_metrics", from_json(col("public_metrics"), metrics_schema))
parsed_df = parsed_df.withColumn("user_metrics", from_json(col("user_metrics"), user_metrics_schema))
df = parsed_df.select(
    "id", "text", "in_reply_to_user_id", "created_at", "source", "username",
    col("public_metrics.retweet_count").alias("retweet_count"),
    col("public_metrics.reply_count").alias("reply_count"),
    col("public_metrics.like_count").alias("like_count"),
    col("public_metrics.quote_count").alias("quote_count"),
    col("user_metrics.followers_count").alias("followers_count"),
    col("user_metrics.following_count").alias("following_count"),
    col("user_metrics.tweet_count").alias("tweet_count"),
    col("user_metrics.listed_count").alias("listed_count")
)
df = df.na.replace("", None)
df = df.withColumn("replied", when(col("in_reply_to_user_id").isNull(), False).otherwise(True)).drop("in_reply_to_user_id")
df = df.withColumn("source", when(col("source").isNull(), "UnKnown").otherwise(col("source")))

df = df.withColumn("year", year("created_at")) \
       .withColumn("month", month("created_at")) \
       .withColumn("day", dayofmonth("created_at")) \
       .withColumn("hour", hour("created_at"))
df.write.partitionBy("Year", "Month", "Day", "Hour").parquet("/twitter-landing-data", mode="overwrite")
df.write.partitionBy("Year", "Month", "Day", "Hour").parquet("/Backup", mode="append")
spark.sql("CREATE TABLE IF NOT EXISTS twitter_staging (id String, text STRING, created_at TIMESTAMP, source STRING, username STRING, retweet_count INT, reply_count INT, like_count INT, quote_count INT, followers_count INT, following_count INT, tweet_count INT, listed_count INT, replied BOOLEAN) \
           USING PARQUET \
           PARTITIONED BY (year INT, month INT, day INT, hour INT) \
           LOCATION '/twitter-landing-data'")
spark.sql("MSCK REPAIR TABLE twitter_staging")