import pyspark 
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException

spark = SparkSession.builder \
    .appName("Create Fact Table") \
    .config("spark.sql.catalogImplementation", "hive") \
    .enableHiveSupport() \
    .getOrCreate()
spark.conf.set("spark.sql.sources.default", "parquet")

df = spark.sql("SELECT date_format(created_at, 'yyyy-MM-dd') as date_key, SUM(retweet_count) as tweet_count, SUM(reply_count) as reply_count, SUM(like_count) as like_count, SUM(quote_count) as quote_count FROM tweet_raw GROUP BY date_key")

df2 = spark.sql("""
    SELECT  max(date_key)as date_key, 
            SUM(followers_count) AS followers_count, 
            SUM(following_count) AS following_count, 
            SUM(tweet_count) AS tweet_count, 
            SUM(listed_count) AS listed_count 
    FROM users_raw ur 
    JOIN (
        SELECT DISTINCT username, MAX(date_format(created_at, 'yyyy-MM-dd')) AS date_key 
        FROM tweet_raw
        GROUP BY username
    ) tr 
    ON ur.username = tr.username
    GROUP BY date_key
""")

try:
    tweet_fact_df = spark.table("tweet_fact")
    tweet_fact_df = df.join(tweet_fact_df, ["date_key"], "left_anti")
    user_fact_df = spark.table("users_fact")
    user_fact_df = df2.join(user_fact_df, ["date_key"], "left_anti")
except AnalysisException:
    # Handle case where tweet_fact table does not exist
    tweet_fact_df = None
    tweet_fact_df = df
    user_fact_df = None
    user_fact_df = df2

tweet_fact_df.write.mode("append").format("parquet").option("path", "/twitter-fact").saveAsTable("tweet_fact")
user_fact_df.write.mode("append").format("parquet").option("path", "/users-fact").saveAsTable("users_fact")