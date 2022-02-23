# -*- coding: utf-8 -*-

import ast

import tweepy

from argparse import ArgumentParser
from datetime import datetime

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

# Twitter API credentials
CONSUMER_KEY = ""
CONSUMER_SECRET = ""
ACCESS_TOKEN = ""
ACCESS_TOKEN_SECRET = ""

SPARK_APP_NAME = "TweetCountKafkaTwitter"
SPARK_CHECKPOINT_TMP_DIR = "tmp_spark_streaming"
# SPARK_BATCH_INTERVAL = 10
SPARK_LOG_LEVEL = "OFF"

KAFKA_BOOTSTRAP_SERVERS = "broker:29092"
KAFKA_TOPIC = "tweets-kafka"

def update_total_count(current_count, count_state):
    """
    Update the previous value by a new ones.

    Each key is updated by applying the given function
    on the previous state of the key (count_state) and the new values
    for the key (current_count).
    """
    if count_state is None:
        count_state = 0
    return sum(current_count, count_state)


def create_streaming_context(SPARK_BATCH_INTERVAL):
    """Create Spark streaming context."""

    # Create Spark Context
    sc = SparkContext(appName=SPARK_APP_NAME)
    # Set log level
    sc.setLogLevel(SPARK_LOG_LEVEL)
    # Create Streaming Context
    ssc = StreamingContext(sc, SPARK_BATCH_INTERVAL)
    # Sets the context to periodically checkpoint the DStream operations for master
    # fault-tolerance. The graph will be checkpointed every batch interval.
    # It is used to update results of stateful transformations as well
    ssc.checkpoint(SPARK_CHECKPOINT_TMP_DIR)
    return ssc


def create_stream(ssc):
    """
    Create subscriber (consumer) to the Kafka topic and
    extract only messages (works on RDD that is mini-batch).
    """
    return (
        KafkaUtils.createDirectStream(
            ssc, topics=[KAFKA_TOPIC],
            kafkaParams={"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
            .map(lambda x: x[1])
    )

def filter_tweet_task1(tweet):
    new_dict = ast.literal_eval(tweet)
    return new_dict["user"]["id"] in [314174343, 285532415, 147964447, 34200559, 338960856, 200036850, 72525490, 20510157, 99918629]

def filter_tweet_task2(tweet):
    new_dict = ast.literal_eval(tweet)
    if "retweeted_status" in new_dict:
        if new_dict["retweeted_status"]["user"]["id"] in [314174343, 285532415, 147964447, 34200559, 338960856, 200036850, 72525490, 20510157, 99918629]:
            return True
    return False

def filter_tweet_task3(tweet):
    new_dict = ast.literal_eval(tweet)
    if "in_reply_to_user_id" in new_dict:
        if new_dict["in_reply_to_user_id"] in [314174343, 285532415, 147964447, 34200559, 338960856, 200036850, 72525490, 20510157, 99918629]:
            return True
    return False

def parse_tweet_task1(tweet, window):
    new_dict = ast.literal_eval(tweet)
    return (new_dict["user"]["screen_name"] + " by " + window, 1)

def parse_tweet_task2(tweet):
    new_dict = ast.literal_eval(tweet)
    return (new_dict["retweeted_status"]["id"], 1)

def parse_tweet_task2_res(tweet):
    new_dict = ast.literal_eval(tweet)
    return (new_dict["retweeted_status"]["user"]["screen_name"] + " " + str(new_dict["retweeted_status"]["id"]) + " " + new_dict["retweeted_status"]["text"], 1)

def parse_tweet_task3(tweet):
    new_dict = ast.literal_eval(tweet)
    return (new_dict["in_reply_to_status_id_str"], 1)

def parse_tweet_task3_res(tweet):
    new_dict = ast.literal_eval(tweet)
    return (new_dict["in_reply_to_screen_name"] + " " + str(new_dict["in_reply_to_status_id_str"]) + " " + get_tweet_text_by_id(new_dict["in_reply_to_status_id_str"]), 1)

def get_tweet_text_by_id(id_of_tweet):
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    api = tweepy.API(auth)
    
    return (api.get_status(id_of_tweet)).text

def main():

    parser = ArgumentParser()
    parser.add_argument("--task1", action='store_true')
    parser.add_argument("--task2", action='store_true')
    parser.add_argument("--task2_res", action='store_true')
    parser.add_argument("--task3", action='store_true')
    parser.add_argument("--task3_res", action='store_true')
    args = parser.parse_args()    
                
    # DZ1 Task1
    if args.task1:
        
        # Init Spark streaming context
        SPARK_BATCH_INTERVAL = 10
        ssc = create_streaming_context(SPARK_BATCH_INTERVAL)

        # Get tweet stream
        tweets_task1_1min = create_stream(ssc)
        tweets_task1_10min = create_stream(ssc)

        # Count tweets for each RDD (mini-batch)
        total_counts_sorted_task1_1min = (
            tweets_task1_1min
                .filter(lambda tweet: filter_tweet_task1(tweet))
                .map(lambda tweet: parse_tweet_task1(tweet, "1 min"))
                .reduceByKeyAndWindow(lambda x1, x2: x1 + x2, lambda x1, x2: x1 - x2, 60, 30)
                # Sort by counts
                .transform(lambda x_rdd: x_rdd.sortBy(lambda x: -x[1]))
        )

        total_counts_sorted_task1_10min = (
            tweets_task1_10min
                .filter(lambda tweet: filter_tweet_task1(tweet))
                .map(lambda tweet: parse_tweet_task1(tweet, "10 min"))
                .reduceByKeyAndWindow(lambda x1, x2: x1 + x2, lambda x1, x2: x1 - x2,  60*10, 30)
                # Sort by counts
                .transform(lambda x_rdd: x_rdd.sortBy(lambda x: -x[1]))
        )

        # Print result
        total_counts_sorted_task1_1min.pprint(20)
        total_counts_sorted_task1_10min.pprint(20)
        
        # Start Spark Streaming
        ssc.start()

        # Waiting for termination
        ssc.awaitTermination()

        # Start Spark Streaming
        ssc.start()

        # Waiting for termination
        ssc.awaitTermination()
    
    # DZ2 Task2
    if args.task2:
                
        SPARK_BATCH_INTERVAL = 60
        ssc = create_streaming_context(SPARK_BATCH_INTERVAL)

        tweets_task2 = create_stream(ssc)

        total_counts_sorted_task2 = (
            tweets_task2
                .filter(lambda tweet: filter_tweet_task2(tweet))
                .map(lambda tweet: parse_tweet_task2(tweet))
                .reduceByKey(lambda x1, x2: x1 + x2)
                .updateStateByKey(update_total_count)
                # Sort by counts
                .transform(lambda x_rdd: x_rdd.sortBy(lambda x: -x[1]))
        )
        
        # Print result
        total_counts_sorted_task2.pprint(20)
    
        tweets_sorted_task2_res = (
            tweets_task2
                .filter(lambda tweet: filter_tweet_task2(tweet))
                .map(lambda tweet: parse_tweet_task2_res(tweet))
                .reduceByKey(lambda x1, x2: x1 + x2)
                .updateStateByKey(update_total_count)
                # Sort by counts
                .transform(lambda x_rdd: x_rdd.sortBy(lambda x: -x[1]))
        )
        
        # Save result
        tweets_sorted_task2_res.saveAsTextFiles("DZ2-task2")
        
        # Start Spark Streaming
        ssc.start()

        # Waiting for termination
        ssc.awaitTermination()

        # Start Spark Streaming
        ssc.start()

        # Waiting for termination
        ssc.awaitTermination(60*30)
    
    # DZ2 Task2 - results
    if args.task2_res:
        
        # Create Spark Context
        sc = SparkContext(appName=SPARK_APP_NAME)
        
        # https://stackoverflow.com/questions/35750614/pyspark-get-list-of-files-directories-on-hdfs-path
        # https://stackoverflow.com/questions/50526076/find-latest-file-pyspark
        URI           = sc._gateway.jvm.java.net.URI
        Path          = sc._gateway.jvm.org.apache.hadoop.fs.Path
        FileSystem    = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
        Configuration = sc._gateway.jvm.org.apache.hadoop.conf.Configuration


        fs = FileSystem.get(URI("hdfs://master:9000"), Configuration())

        files = fs.listStatus(Path("/user/bigdata/"))

#         for file in files:
#             print(file.getPath())
            
        # You can also filter for directory here
        files_status = [(file.getPath().toString(), file.getModificationTime()) for file in files]

        files_status.sort(key = lambda tup: tup[1], reverse=True)

        for file in files_status:
            if "DZ2-task2" in file[0]:
                most_recently_updated = file[0]
                break
        
        loadRdds = sc.textFile(most_recently_updated)
        
        # https://stackoverflow.com/questions/25295277/view-rdd-contents-in-python-spark
        for rdd in loadRdds.collect()[0:5]:
            print(rdd)
            
    # DZ2 Task3
    if args.task3:
                
        SPARK_BATCH_INTERVAL = 60
        ssc = create_streaming_context(SPARK_BATCH_INTERVAL)

        tweets_task3 = create_stream(ssc)

        total_counts_sorted_task3 = (
            tweets_task3
                .filter(lambda tweet: filter_tweet_task3(tweet))
                .map(lambda tweet: parse_tweet_task3(tweet))
                .reduceByKey(lambda x1, x2: x1 + x2)
                .updateStateByKey(update_total_count)
                # Sort by counts
                .transform(lambda x_rdd: x_rdd.sortBy(lambda x: -x[1]))
        )
        
        # Print result
        total_counts_sorted_task3.pprint(20)
    
        tweets_sorted_task3_res = (
            tweets_task3
                .filter(lambda tweet: filter_tweet_task3(tweet))
                .map(lambda tweet: parse_tweet_task3_res(tweet))
                .reduceByKey(lambda x1, x2: x1 + x2)
                .updateStateByKey(update_total_count)
                # Sort by counts
                .transform(lambda x_rdd: x_rdd.sortBy(lambda x: -x[1]))
        )
        # Save result
        tweets_sorted_task3_res.saveAsTextFiles("DZ2-task3")
        
        # Start Spark Streaming
        ssc.start()

        # Waiting for termination
        ssc.awaitTermination()

        # Start Spark Streaming
        ssc.start()

        # Waiting for termination
        ssc.awaitTermination(60*30)

    # DZ2 Task3 - results
    if args.task3_res:
        
        # Create Spark Context
        sc = SparkContext(appName=SPARK_APP_NAME)
        
        # https://stackoverflow.com/questions/35750614/pyspark-get-list-of-files-directories-on-hdfs-path
        # https://stackoverflow.com/questions/50526076/find-latest-file-pyspark
        URI           = sc._gateway.jvm.java.net.URI
        Path          = sc._gateway.jvm.org.apache.hadoop.fs.Path
        FileSystem    = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
        Configuration = sc._gateway.jvm.org.apache.hadoop.conf.Configuration


        fs = FileSystem.get(URI("hdfs://master:9000"), Configuration())

        files = fs.listStatus(Path("/user/bigdata/"))

#         for file in files:
#             print(file.getPath())
            
        # You can also filter for directory here
        files_status = [(file.getPath().toString(), file.getModificationTime()) for file in files]

        files_status.sort(key = lambda tup: tup[1], reverse=True)

        for file in files_status:
            if "DZ2-task3" in file[0]:
                most_recently_updated = file[0]
                break
        
        loadRdds = sc.textFile(most_recently_updated)
        
        # https://stackoverflow.com/questions/25295277/view-rdd-contents-in-python-spark
        for rdd in loadRdds.collect()[0:5]:
            print(rdd)
            
if __name__ == "__main__":
    main()