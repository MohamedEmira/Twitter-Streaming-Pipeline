SET hive.exec.dynamic.partition.mode=nonstrict;

CREATE TABLE IF NOT EXISTS tweet_raw (
    id BIGINT,
    username STRING,
    text STRING,
    created_at TIMESTAMP,
    source STRING,
    replied BOOLEAN,
    retweet_count INT,
    reply_count INT,
    like_count INT,
    quote_count INT
)
PARTITIONED BY (Year INT, Month INT, Day INT, Hour INT)
STORED AS PARQUET
LOCATION '/twitter-raw-data/tweet_text_raw';

CREATE TABLE IF NOT EXISTS users_raw (
    username STRING,
    followers_count INT,
    following_count INT,
    tweet_count INT,
    listed_count INT
)
PARTITIONED BY (Year INT, Month INT, Day INT, Hour INT)
STORED AS PARQUET
LOCATION '/twitter-raw-data/users_raw';

INSERT INTO TABLE tweet_raw PARTITION(Year, Month, Day, Hour)
SELECT ts.id,ts.username ,ts.text, ts.created_at, ts.source, ts.replied, ts.retweet_count, ts.reply_count, ts.like_count, ts.quote_count, ts.Year, ts.Month, ts.Day, ts.Hour
FROM twitter_staging ts
WHERE NOT EXISTS (
    SELECT ttr.id FROM tweet_raw ttr
    WHERE ttr.id = ts.id
);
INSERT INTO TABLE users_raw PARTITION(Year, Month, Day, Hour)
SELECT  ts.username, ts.followers_count, ts.following_count, ts.tweet_count, ts.listed_count,ts.Year,ts.Month,ts.Day,ts.Hour
FROM twitter_staging ts
WHERE NOT EXISTS (
    SELECT ur.username FROM users_raw ur
    WHERE ur.username = ts.username
);


