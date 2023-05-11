import os
import subprocess

#open the listner in sperate process because it's blocking code
twitter_listener_process = subprocess.Popen(['python3', 'twitter_listener.py'], stdout=subprocess.PIPE)

# run the twitter stream 
twitter_stream = ['/opt/spark-3.1.2-bin-hadoop3.2/bin/spark-submit', 'stream.py']
subprocess.run(twitter_stream, check=True)

#create raw tables and insert data
raw_tables = ['hive', '-f', 'hive.hql']
subprocess.run(raw_tables , check=True)

#create fact tables and insert data
Fact_spark = ['/opt/spark-3.1.2-bin-hadoop3.2/bin/spark-submit', 'spark.py']
subprocess.run(Fact_spark, check=True)