# Twitter-Streaming-Pipeline


## Overview
This repository contains scripts and tools for setting up a Twitter data pipeline to retrieve tweets, perform transformations using Apache Spark, and store the data in HDFS. Additionally, a data warehouse is created using Hive with dimension and fact tables for further analysis and insights.

## Twitter Listener
The first step in the pipeline is a script that connects to the Twitter API and retrieves recent tweets based on a given search query. In this project, the topic of interest is "Cleopatra." The script makes a GET request to retrieve the relevant data and converts it to a JSON object. The data is then sent to the socket for further processing.

## Twitter_Stream
The Twitter_Stream script, written in PySpark, reads data from the socket stream. The fields of interest are defined, and a schema is created using StructType. The retrieved data includes tweet information such as ID, text, in_reply_to_user_id, created_at, public_metrics, and source. User information, such as username and user_metrics, is also retrieved. The JSON data is converted to a DataFrame using the defined schema.

The script performs data cleaning and transformation. Public and user metrics are separated into their respective fields. The in_reply_to column is converted to a boolean value (true or false) to indicate if the tweet is a reply or a new tweet. The source field is set to "unknown" if no source information is available. The timestamp of the tweet is used to extract hour, day, month, and year information. Finally, the DataFrame is stored in a Parquet file, and a Hive table is created on top of it as the landing table.

## Hive Dimension Tables
The next step involves creating dimension tables in Hive and inserting the data into them. Two dimension tables are created: tweet_raw, which contains all the data about tweets, and user_raw, which contains user-related data. Care is taken to avoid duplicates while inserting data.

## Fact Tables
The fact tables are created using a Python script. The fact grain is set to a day for meaningful analysis. Duplicates are checked and avoided during the append operation. The users_fact table is used to calculate coverage, representing the number of users reached by a topic. Only users who have tweeted today are considered, and multiple tweets from the same user are counted only once.

## Pipeline
The pipeline script is a collection of all the individual scripts, allowing for easy execution. It can be run as a bash script or a Python script.

**Notes:**
- The script was originally designed to run every 10 minutes, but due to limited data availability, it was changed to run daily. Adjustments can be made to run it more frequently, such as every 10 minutes, and the fact grain can be changed to an hour.
- The original plan included sentiment analysis, dividing the data into positive and negative categories, and creating separate fact tables for each. Additional fact tables were planned for influencers (users with a certain number of followers) and famous tweets with high engagement. Unfortunately, due to time constraints, these features were not implemented.

Feel free to explore the repository and use the provided scripts to set up your Twitter data pipeline and perform analysis using Apache Spark and Hive.

