# Running Spark Streaming Word Count on EMR

# 1. Clean up old directories if they exist

hadoop fs -rm -r /user/hadoop/streaming/input
hadoop fs -rm -r /user/hadoop/streaming/output
hadoop fs -rm -r /user/hadoop/checkpoint
```

# 2. Create fresh directories

hadoop fs -mkdir -p /user/hadoop/streaming/input
hadoop fs -mkdir -p /user/hadoop/streaming/output
hadoop fs -mkdir -p /user/hadoop/checkpoint
```

# 3. Set permissions if needed

hadoop fs -chmod 777 /user/hadoop/streaming/input
hadoop fs -chmod 777 /user/hadoop/streaming/output
hadoop fs -chmod 777 /user/hadoop/checkpoint


# 4. Submit Spark job with correct paths

spark-submit \
--class streaming.StreamingWordCount \
--master yarn \
--deploy-mode client \
--executor-memory 1G \
--num-executors 2 \
wordcount.jar \
hdfs:///user/hadoop/streaming/input \
hdfs:///user/hadoop/streaming/output


# 5. In another terminal window, copy input files

hadoop fs -put text1.txt /user/hadoop/streaming/input/
hadoop fs -put text2.txt /user/hadoop/streaming/input/
hadoop fs -put text3.txt /user/hadoop/streaming/input/


# 6. List output directories

hadoop fs -ls /user/hadoop/streaming/output


# 7. View results

hadoop fs -cat /user/hadoop/streaming/output/taskA-001/part*
hadoop fs -cat /user/hadoop/streaming/output/taskB-001/part*
hadoop fs -cat /user/hadoop/streaming/output/taskC-001/part*

hadoop fs -cat /user/hadoop/streaming/output/taskA-002/part*
hadoop fs -cat /user/hadoop/streaming/output/taskB-002/part*
hadoop fs -cat /user/hadoop/streaming/output/taskC-002/part*


# 8. Copy output to local file system

hadoop fs -get /user/hadoop/streaming/output/taskA-001/part* .
hadoop fs -get /user/hadoop/streaming/output/taskB-001/part* .
hadoop fs -get /user/hadoop/streaming/output/taskC-001/part* .

hadoop fs -get /user/hadoop/streaming/output/taskA-002/part* .
hadoop fs -get /user/hadoop/streaming/output/taskB-002/part* .
hadoop fs -get /user/hadoop/streaming/output/taskC-002/part* .

# 9. Quit the Spark job
Press Ctrl+C in the terminal where spark-submit is running
