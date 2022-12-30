from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import sys
import boto3
import os
from datetime import datetime
import calendar
import random
import time
import simplejson

stream_name = "tomz_test"

kinesis_client = boto3.client('kinesis', region_name='us-east-1')

# twitter API keys
consumerKey = os.environ["TWEET_CONSUMER_KEY"]
consumerSecret = os.environ["TWEET_CONSUMER_SECRET"]
accessToken = os.environ["TWITTER_ACCESS_TOKEN"]
accessSecret = os.environ["TWITTER_ACCESS_SECRET"]

class Listener(StreamListener):
    def on_data(self, data):
        print(data.rstrip())
        
        # pumping tweets into kinesis
        put_response = kinesis_client.put_record(
            StreamName=stream_name,
            Data=data,
            PartitionKey="fake"
        )
        
        time.sleep(0.05)
        return True
    
    def on_error (self, status):
        print status
auth = OAuthHandler(consumerKey, consumerSecret)
auth.set_access_token(accessToken, accessSecret)

track = sys.argv[1].split(',')
twitterStream = Stream(auth, listener())
twitterStream.filter(track=track, language=["en"])
