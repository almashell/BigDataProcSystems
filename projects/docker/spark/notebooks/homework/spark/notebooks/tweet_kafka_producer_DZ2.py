# -*- coding: utf-8 -*-

import sys
import time

# import signal
# from contextlib import contextmanager

from argparse import ArgumentParser

import tweepy
import json
from kafka import KafkaProducer
import kafka.errors

# Twitter API credentials
CONSUMER_KEY = ""
CONSUMER_SECRET = ""
ACCESS_TOKEN = ""
ACCESS_TOKEN_SECRET = ""

# Kafka topic name
TOPIC_NAME = "tweets-kafka"

# Kafka server
KAFKA_HOST = "broker"
KAFKA_PORT = "29092"

# class TimeoutException(Exception): pass

class KafkaCommunicator:
    def __init__(self, producer, topic):
        self.producer = producer
        self.topic = topic

    def send(self, message):
        self.producer.send(self.topic, str(message).encode('utf-8'))

    def close(self):
        self.producer.close()


class StreamListener(tweepy.Stream):
    """Listener to tweet stream from twitter."""
    def __init__(self,
                 consumer_key,
                 consumer_secret,
                 access_token,
                 access_token_secret,
                 communicator):
        super().__init__(consumer_key, consumer_secret, access_token, access_token_secret)
        self.communicator = communicator

    def on_data(self, raw_data):
        """Receiving a new data."""
        data = json.loads(raw_data)
#         print("Tweet from", data, "\n")
        if "user" in data:
            if "retweeted_status" in data:
                if data["retweeted_status"]["user"]["id"] in [314174343, 285532415, 147964447, 34200559, 338960856, 200036850, 72525490, 20510157, 99918629]:
                    print("\tRetweet of", data["user"]["screen_name"], "from", data["retweeted_status"]["user"]["screen_name"], "\n")
                else:
                    print("Retweet of", data["user"]["screen_name"], "from", data["retweeted_status"]["user"]["screen_name"], "\n")
            if "in_reply_to_status_id_str" in data and data["in_reply_to_status_id_str"] is not None:
                if data["in_reply_to_user_id"] in [314174343, 285532415, 147964447, 34200559, 338960856, 200036850, 72525490, 20510157, 99918629]:
                    print("\tReply from", data["user"]["screen_name"], "on", data["in_reply_to_screen_name"], "\n")
                else:
                    print("Reply from", data["user"]["screen_name"], "on", data["in_reply_to_screen_name"], "\n")             
            else:
                if data["user"]["id"] in [314174343, 285532415, 147964447, 34200559, 338960856, 200036850, 72525490, 20510157, 99918629]:
                    print("\tTweet from", data["user"]["screen_name"], "\n")
                else:
                    print("Tweet from", data["user"]["screen_name"], "\n")

            self.communicator.send(data)
        
#         print(srceen_name, user_id, "\n")    
#         self.communicator.send(data)
        
    def on_error(self, status):
        print(status)
        return True
    
def create_communicator():
    """Create Kafka producer."""
    producer = KafkaProducer(bootstrap_servers=KAFKA_HOST + ":" + KAFKA_PORT)
    return KafkaCommunicator(producer, TOPIC_NAME)


def create_stream(communicator):
    """Set stream for twitter api with custom listener."""
    return StreamListener(
        consumer_key=CONSUMER_KEY,
        consumer_secret=CONSUMER_SECRET,
        access_token=ACCESS_TOKEN,
        access_token_secret=ACCESS_TOKEN_SECRET,
        communicator=communicator)


def run_processing(stream):
    # Region that approximately corresponds to Moscow
    # region = [34.80, 49.87, 149.41, 74.13]
    # Start filtering messages
    stream.filter(locations=[34.80, 49.87, 149.41, 74.13], follow=["314174343", "285532415", "147964447", "34200559", "338960856", "200036850", "72525490", "20510157", "99918629"])


def main():
    
#     parser = ArgumentParser()
#     parser.add_argument("--test", action='store_true')
#     args = parser.parse_args()
    
#     if args.test:
#         communicator = create_communicator()
        
#         #T=0s
#         time.sleep(0)
#         test_message = {}
#         test_message["user"] = {"screen_name" : "T=0s"}
#         communicator.send(test_message)
        
#         #T=10s
#         time.sleep(10)
#         test_message = {}
#         test_message["user"] = {"screen_name" : "T=10s"}
#         communicator.send(test_message)        
        
#         #T=20s
#         time.sleep(10)
#         test_message = {}
#         test_message["user"] = {"screen_name" : "T=20s"}
#         communicator.send(test_message)
        
#         #T=30s
#         time.sleep(10)
#         test_message = {}
#         test_message["user"] = {"screen_name" : "T=30s"}
#         communicator.send(test_message)
        
#         #T=40s
#         time.sleep(10)
#         test_message = {}
#         test_message["user"] = {"screen_name" : "T=40s"}
#         communicator.send(test_message)
        
#         #T=50s
#         time.sleep(10)
#         test_message = {}
#         test_message["user"] = {"screen_name" : "T=50s"}
#         communicator.send(test_message)
        
                
#         #T=60s
#         time.sleep(10)
#         test_message = {}
#         test_message["user"] = {"screen_name" : "T=60s"}
#         communicator.send(test_message)
        
#         communicator.close()
        
#         sys.exit("Test DZ1.1 done")
        
    communicator = None
    tweet_stream = None
    try:
        communicator = create_communicator()
        tweet_stream = create_stream(communicator)
            
        run_processing(tweet_stream)
    except KeyboardInterrupt:
        pass
    except kafka.errors.NoBrokersAvailable:
        print("Kafka broker not found.")
    finally:
        if communicator:
            communicator.close()
        if tweet_stream:
            tweet_stream.disconnect()


if __name__ == "__main__":
    main()